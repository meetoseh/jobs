"""SMS Receipt Reconciliation Job"""
import json
import time
from typing import Any, Dict, List, Literal, Optional
from error_middleware import handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from lib.sms.events import MessageResourceEvent
from lib.sms.sms_info import (
    MessageResource,
    PendingSMS,
    SMSFailureInfo,
    SMSSuccess,
    SMSSuccessResult,
    encode_data_for_failure_job,
    encode_data_for_success_job,
)
import lib.sms.event_stats as event_stats
from redis_helpers.lmove_using_purgatory import lmove_using_purgatory_safe
from redis_helpers.remove_pending_sms import remove_pending_sms_safe
from redis_helpers.run_with_prep import run_with_prep as run_redis_commands_with_prep
import dataclasses

from redis_helpers.update_pending_sms import update_pending_sms_safe

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""After this time the job stops to avoid blocking other jobs"""

SUCCESS_STATUSES = frozenset(("delivered", "sent", "read"))
FAILURE_STATUSES = frozenset(("failed", "undelivered", "canceled", "lost"))


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Reconciles events in the receipt event queue based on the corresponding
    message resource state in the receipt pending set.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"sms:receipt_reconciliation_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:sms:receipt_reconciliation_job",
            b"started_at",
            str(started_at).encode("ascii"),
        )

        stats = RunStats()
        stop_reason: Optional[
            Literal["list_exhausted", "time_exhausted", "signal"]
        ] = None

        while True:
            if gd.received_term_signal:
                logging.info(f"SMS Receipt Reconciliation Job: Received Term Signal")
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.info(f"SMS Receipt Reconciliation Job: Time Exhausted")
                stop_reason = "time_exhausted"
                break

            event_raw = await lmove_using_purgatory_safe(
                itgs, b"sms:event", b"sms:event:purgatory"
            )
            if event_raw is None:
                logging.info(f"SMS Receipt Reconciliation Job: List Exhausted")
                stop_reason = "list_exhausted"
                break

            try:
                event = MessageResourceEvent.parse_raw(
                    event_raw, content_type="application/json"
                )
            except:
                logging.warning(
                    f"SMS Receipt Reconciliation job skipping invalid event: {event_raw}",
                    exc_info=True,
                )
                await redis.lpop(b"sms:event:purgatory")
                continue

            pending_info_raw = await redis.hgetall(
                f"sms:pending:{event.sid}".encode("utf-8")
            )
            if pending_info_raw is None or len(pending_info_raw) == 0:
                pending_info = None
            else:
                try:
                    pending_info = PendingSMS.from_redis_mapping(pending_info_raw)
                except:
                    logging.warning(
                        f"SMS Receipt Reconciliation job skipping invalid pending info: {pending_info_raw}",
                        exc_info=True,
                    )
                    await redis.lpop(b"sms:event:purgatory")
                    continue

            attempted_at = time.time()
            try:
                result = await reconcile_event(
                    itgs, event, pending_info, now=attempted_at
                )
            except EventModifiedDuringReconciliation:
                logging.warning(
                    f"SMS Receipt Reconciliation job optimistic locking strategy on event failed: {event=}, {pending_info=}",
                    exc_info=True,
                )
                # keep it in purgatory so we try it again next loop
                continue
            except Exception as exc:
                await handle_error(
                    exc,
                    extra_info=f"SMS Receipt Reconciliation job failed to reconcile event: {event=}, {pending_info=} - skipping",
                )
                # Generally safe to skip here since we can always poll if it's truly an internal
                # error and not some internal service giving a transient error. If redis is hiccuping
                # this will fail anyway and it'll bubble up to be retried the next time the job is
                # run which is also fine
                await redis.lpop(b"sms:event:purgatory")
                continue

            await increment_events(itgs, events=result.events, now=attempted_at)
            stats.add_stats(result.stats)
            await redis.lpop(b"sms:event:purgatory")

        finished_at = time.time()
        logging.info(
            f"SMS Receipt Reconciliation Job: Finished: {stats=}, {stop_reason=}"
        )
        await redis.hset(
            b"stats:sms:receipt_reconciliation_job",
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"stop_reason": stop_reason.encode("utf-8"),
                b"attempted": stats.attempted,
                b"pending": stats.pending,
                b"succeeded": stats.succeeded,
                b"failed": stats.failed,
                b"found": stats.found,
                b"updated": stats.updated,
                b"duplicate": stats.duplicate,
                b"out_of_order": stats.out_of_order,
                b"removed": stats.removed,
            },
        )


@dataclasses.dataclass
class RunStats:
    attempted: int = 0
    pending: int = 0
    succeeded: int = 0
    failed: int = 0
    found: int = 0
    updated: int = 0
    duplicate: int = 0
    out_of_order: int = 0
    removed: int = 0

    def add_stats(self, other: "RunStats") -> None:
        self.attempted += other.attempted
        self.pending += other.pending
        self.succeeded += other.succeeded
        self.failed += other.failed
        self.found += other.found
        self.updated += other.updated
        self.duplicate += other.duplicate
        self.out_of_order += other.out_of_order
        self.removed += other.removed


@dataclasses.dataclass
class EventIncrement:
    name: event_stats.SMSEventStatsEvent
    extra: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class EventResult:
    stats: RunStats
    events: List[EventIncrement]


class EventModifiedDuringReconciliation(Exception):
    def __init__(self) -> None:
        super()


async def reconcile_event(
    itgs: Itgs,
    event: MessageResourceEvent,
    pending_info: Optional[PendingSMS],
    now: float,
) -> EventResult:
    """Attempts to reconcile the given event. If we need to update or remove the
    pending info, we do so using an optimistic locking strategy. If the pending info
    is changed between the pending info being fetched and this performing the update
    or removal, this raises an EventModifiedDuringReconciliation exception.
    """
    stats = RunStats()
    events: List[EventIncrement] = []

    stats.attempted += 1
    events.append(EventIncrement(name="attempted", extra={"status": event.status}))
    if event.received_via == "webhook":
        events.append(
            EventIncrement(name="received_via_webhook", extra={"status": event.status})
        )
    elif event.received_via == "poll":
        events.append(
            EventIncrement(name="received_via_polling", extra={"status": event.status})
        )

    event_status_category: Literal["success", "failure", "pending"] = None
    if event.status in SUCCESS_STATUSES:
        stats.succeeded += 1
        event_status_category = "success"
        events.append(EventIncrement(name="succeeded", extra={"status": event.status}))
    elif event.status in FAILURE_STATUSES:
        stats.failed += 1
        event_status_category = "failure"
        events.append(EventIncrement(name="failed", extra={"status": event.status}))
    else:
        stats.pending += 1
        event_status_category = "pending"
        events.append(EventIncrement(name="pending", extra={"status": event.status}))

    if pending_info is None:
        events.append(EventIncrement(name="unknown", extra={"status": event.status}))
        return EventResult(stats=stats, events=events)

    stats.found += 1
    events.append(EventIncrement(name="found", extra=None))

    if is_out_of_order(event, pending_info):
        stats.out_of_order += 1
        events.append(
            EventIncrement(
                name="out_of_order",
                extra={
                    "stored_status": pending_info.message_resource.status,
                    "event_status": event.status,
                },
            )
        )
        return EventResult(stats=stats, events=events)

    if event_status_category == "pending":
        if event.status == pending_info.message_resource.status:
            stats.duplicate += 1
            events.append(
                EventIncrement(
                    name="duplicate",
                    extra={"status": event.status},
                )
            )
            return EventResult(stats=stats, events=events)

        success = await update_pending_sms_safe(
            itgs,
            b"sms:pending",
            event.sid.encode("utf-8"),
            pending_info.num_changes,
            delta={
                b"num_changes": str(pending_info.num_changes + 1).encode("ascii"),
                b"message_resource_last_updated_at": str(now).encode("ascii"),
                b"message_resource_status": event.status.encode("utf-8"),
                b"message_resource_date_updated": b""
                if event.date_updated is None
                else str(event.date_updated).encode("ascii"),
            },
        )
        if not success:
            raise EventModifiedDuringReconciliation()

        stats.updated += 1
        events.append(
            EventIncrement(
                name="updated",
                extra={
                    "old_status": pending_info.message_resource.status,
                    "new_status": event.status,
                },
            )
        )
        return EventResult(stats=stats, events=events)

    if event_status_category == "success":
        job_to_enqueue = json.dumps(
            {
                "name": pending_info.success_job.name,
                "kwargs": {
                    **pending_info.success_job.kwargs,
                    "data_raw": encode_data_for_success_job(
                        sms=SMSSuccess(
                            aud="success",
                            uid=pending_info.uid,
                            initially_queued_at=pending_info.send_initially_queued_at,
                            phone_number=pending_info.phone_number,
                            body=pending_info.body,
                        ),
                        result=SMSSuccessResult(
                            message_resource_created_at=pending_info.message_resource_created_at,
                            message_resource_succeeded_at=now,
                            message_resource=MessageResource(
                                sid=event.sid,
                                status=event.status,
                                error_code=event.error_code,
                                date_updated=event.date_updated,
                            ),
                        ),
                    ),
                },
            }
        )
    else:
        job_to_enqueue = json.dumps(
            {
                "name": pending_info.failure_job.name,
                "kwargs": {
                    **pending_info.failure_job.kwargs,
                    "data_raw": encode_data_for_failure_job(
                        sms=PendingSMS(
                            aud="pending",
                            uid=pending_info.uid,
                            send_initially_queued_at=pending_info.send_initially_queued_at,
                            message_resource_created_at=pending_info.message_resource_created_at,
                            message_resource_last_updated_at=now,
                            message_resource=MessageResource(
                                sid=event.sid,
                                status=event.status,
                                error_code=event.error_code,
                                date_updated=event.date_updated,
                            ),
                            failure_job_last_called_at=now,
                            num_failures=pending_info.num_failures + 1,
                            num_changes=pending_info.num_changes + 1,
                            phone_number=pending_info.phone_number,
                            body=pending_info.body,
                            failure_job=pending_info.failure_job,
                            success_job=pending_info.success_job,
                        ),
                        failure_info=SMSFailureInfo(
                            action="pending",
                            identifier="ApplicationErrorOther",
                            subidentifier=event.error_code
                            if event.error_code is not None
                            else "failure_status_without_error_code",
                            retryable=False,
                        ),
                    ),
                },
            }
        )

    success = await remove_pending_sms_safe(
        itgs,
        src=b"sms:pending",
        job_queue=b"jobs:hot",
        sid=event.sid.encode("utf-8"),
        expected_num_changes=pending_info.num_changes,
        job=job_to_enqueue.encode("utf-8"),
    )
    if not success:
        raise EventModifiedDuringReconciliation()

    stats.removed += 1
    events.append(
        EventIncrement(
            name="removed",
            extra={
                "old_status": pending_info.message_resource.status,
                "new_status": event.status,
            },
        )
    )
    return EventResult(stats=stats, events=events)


events_by_sequence_order = {
    "queued": 0,
    "scheduled": 0,
    "accepted": 1,
    "sending": 2,
    "sent": 3,
    "delivered": 9,
    "canceled": 9,
    "failed": 9,
    "undelivered": 9,
}


def is_out_of_order(event: MessageResourceEvent, sms: PendingSMS):
    if event.date_updated is not None and sms.message_resource.date_updated is not None:
        if event.date_updated < sms.message_resource.date_updated:
            return True
        if event.date_updated > sms.message_resource.date_updated:
            return False

    old_sequence_order = events_by_sequence_order.get(sms.message_resource.status)
    new_sequence_order = events_by_sequence_order.get(event.status)

    if old_sequence_order is not None and new_sequence_order is not None:
        if new_sequence_order < old_sequence_order:
            return True
        if new_sequence_order > old_sequence_order:
            return False

    if event.information_received_at < sms.message_resource_last_updated_at:
        return True
    return False


async def increment_events(
    itgs: Itgs, *, events: List[EventIncrement], now: float
) -> None:
    if not events:
        return

    redis = await itgs.redis()

    async def prep(force: bool):
        await event_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            for event in events:
                await event_stats.attempt_increment_event(
                    pipe, event=event.name, extra=event.extra, now=now
                )
            await pipe.execute()

    await run_redis_commands_with_prep(prep, func)


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sms.receipt_reconciliation")

    asyncio.run(main())
