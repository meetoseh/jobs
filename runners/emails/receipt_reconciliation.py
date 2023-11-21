"""Reconciles email events with the receipt pending set"""
from typing import Optional, cast
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import time
from lib.basic_redis_lock import basic_redis_lock
import dataclasses
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
import redis_helpers.run_with_prep
from lib.emails.email_info import (
    EmailFailureInfo,
    EmailPending,
    EmailSuccessInfo,
    encode_data_for_failure_job,
    encode_data_for_success_job,
)
import unix_dates
import pytz
from lib.emails.events import (
    EmailBounceNotification,
    EmailComplaintNotification,
    EmailEvent,
)
import asyncio

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
tz = pytz.timezone("America/Los_Angeles")


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls items from the event queue, checks if the corresponding message id is
    in the receipt pending set, and acts accordingly.

    See: https://oseh.io/admin/email_dashboard

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"email:reconciliation_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(  # type: ignore
            b"stats:email_events:reconciliation_job",  # type: ignore
            b"started_at",  # type: ignore
            str(started_at).encode("ascii"),  # type: ignore
        )

        num_in_purgatory = await redis.llen(b"email:reconciliation_purgatory")  # type: ignore
        if num_in_purgatory > 0:
            slack = await itgs.slack()
            await slack.send_web_error_message(
                f"**Email Reconciliation Job** - recovering {num_in_purgatory=} emails from purgatory",
                "Email Reconc. Job - recovering from purgatory",
            )

            next_to_send_raw = await redis.lindex(b"email:reconciliation_purgatory", 0)  # type: ignore
            if next_to_send_raw is None:
                raise Exception(
                    f"Email Reconciliation Job - {num_in_purgatory=} but LINDEX 0 is None"
                )
        else:
            next_to_send_raw = await redis.lmove(
                b"email:event", b"email:reconciliation_purgatory", "LEFT", "RIGHT"  # type: ignore
            )
            num_in_purgatory = 1

        if next_to_send_raw is None:
            logging.info("Email Reconciliation Job - no events to reconcile")
            finished_at = time.time()
            await redis.hset(  # type: ignore
                b"stats:email_events:reconciliation_job",  # type: ignore
                mapping={
                    b"finished_at": finished_at,
                    b"running_time": finished_at - started_at,
                    b"stop_reason": b"list_exhausted",
                    b"attempted": 0,
                    b"succeeded_and_found": 0,
                    b"succeeded_but_abandoned": 0,
                    b"bounced_and_found": 0,
                    b"bounced_but_abandoned": 0,
                    b"complaint_and_found": 0,
                    b"complaint_and_abandoned": 0,
                },
            )
            return

        logging.debug("Email Reconciliation Job - have work to do")
        run_stats = RunStats()
        stop_reason: Optional[str] = None

        async def advance_next_to_send_raw():
            nonlocal next_to_send_raw, num_in_purgatory
            assert num_in_purgatory > 0

            async with redis.pipeline() as pipe:
                pipe.multi()
                await pipe.lpop(b"email:reconciliation_purgatory")  # type: ignore
                num_in_purgatory -= 1

                if num_in_purgatory == 0:
                    await pipe.lmove(
                        b"email:event",  # type: ignore
                        b"email:reconciliation_purgatory",  # type: ignore
                        "LEFT",
                        "RIGHT",
                    )
                else:
                    await pipe.lindex(b"email:reconciliation_purgatory", 0)  # type: ignore
                result = await pipe.execute()

            next_to_send_raw = result[1]
            if num_in_purgatory == 0 and next_to_send_raw is not None:
                num_in_purgatory = 1

        while True:
            if next_to_send_raw is None:
                logging.debug(
                    "Email Reconciliation Job - no more events to process, stopping"
                )
                stop_reason = "list_exhausted"
                break

            if gd.received_term_signal:
                logging.debug("Email Reconciliation Job - signal received, stopping")
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.debug(
                    "Email Reconciliation Job - max job time exceeded, stopping"
                )
                stop_reason = "time_exhausted"
                break

            event = EmailEvent.model_validate_json(next_to_send_raw)

            # If it's very close to when the event was received, we might
            # have raced the storing of the message in the receipt pending
            # set. hence, in that case, we'll retry for a bit.
            while True:
                message_raw = await redis.hgetall(  # type: ignore
                    f"email:receipt_pending:{event.message_id}".encode("utf-8")  # type: ignore
                )
                if isinstance(message_raw, dict) and len(message_raw) == 0:
                    message_raw = None
                if message_raw is not None:
                    break
                if time.time() - event.received_at >= 5:
                    break
                if gd.received_term_signal:
                    break
                await asyncio.sleep(0.1)

            if gd.received_term_signal:
                logging.debug(
                    "Email Reconciliation Job - signal received mid-message, but where it is still safe, stopping"
                )
                stop_reason = "signal"
                break

            message = (
                None
                if message_raw is None
                else EmailPending.from_redis_mapping(message_raw)
            )

            if event.notification.type == "Delivery" and message is not None:
                await handle_delivery_and_found(itgs, event, message)
                run_stats.attempted += 1
                run_stats.succeeded_and_found += 1
            elif event.notification.type == "Delivery" and message is None:
                await handle_delivery_but_abandoned(itgs, event)
                run_stats.attempted += 1
                run_stats.succeeded_but_abandoned += 1
            elif event.notification.type == "Bounce" and message is not None:
                await handle_bounce_and_found(itgs, event, message)
                run_stats.attempted += 1
                run_stats.bounced_and_found += 1
            elif event.notification.type == "Bounce" and message is None:
                await handle_bounce_but_abandoned(itgs, event)
                run_stats.attempted += 1
                run_stats.bounced_but_abandoned += 1
            elif event.notification.type == "Complaint" and message is not None:
                await handle_complaint_and_found(itgs, event, message)
                run_stats.attempted += 1
                run_stats.complaint_and_found += 1
            elif event.notification.type == "Complaint" and message is None:
                await handle_complaint_and_abandoned(itgs, event)
                run_stats.attempted += 1
                run_stats.complaint_and_abandoned += 1
            else:
                raise NotImplementedError(f"unknown notification type: {event=}")

            await advance_next_to_send_raw()

        finished_at = time.time()
        logging.info(
            f"Email Reconciliation Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Attempted: {run_stats.attempted}\n"
            f"- Succeeded And Found: {run_stats.succeeded_and_found}\n"
            f"- Succeeded But Abandoned: {run_stats.succeeded_but_abandoned}\n"
            f"- Bounced And Found: {run_stats.bounced_and_found}\n"
            f"- Bounced But Abandoned: {run_stats.bounced_but_abandoned}\n"
            f"- Complaint And Found: {run_stats.complaint_and_found}\n"
            f"- Complaint But Abandoned: {run_stats.complaint_and_abandoned}\n"
        )
        await redis.hset(  # type: ignore
            b"stats:email_events:reconciliation_job",  # type: ignore
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"stop_reason": stop_reason.encode("utf-8"),
                b"attempted": run_stats.attempted,
                b"succeeded_and_found": run_stats.succeeded_and_found,
                b"succeeded_but_abandoned": run_stats.succeeded_but_abandoned,
                b"bounced_and_found": run_stats.bounced_and_found,
                b"bounced_but_abandoned": run_stats.bounced_but_abandoned,
                b"complaint_and_found": run_stats.complaint_and_found,
                b"complaint_and_abandoned": run_stats.complaint_and_abandoned,
            },
        )


async def handle_delivery_and_found(
    itgs: Itgs, event: EmailEvent, message: EmailPending
):
    logging.debug(
        f"email {event.message_id} was delivered to {message.email} and was found"
    )
    today = unix_dates.unix_timestamp_to_unix_date(
        message.send_initially_queued_at, tz=tz
    )
    key = f"stats:email_events:daily:{today}".encode("ascii")
    attempted_extra = f"stats:email_events:daily:{today}:extra:attempted".encode(
        "ascii"
    )
    succeeded_extra = f"stats:email_events:daily:{today}:extra:succeeded".encode(
        "ascii"
    )

    data_raw = encode_data_for_success_job(
        message, EmailSuccessInfo(delivery_received_at=time.time())
    )

    redis = await itgs.redis()
    jobs = await itgs.jobs()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_events:daily:earliest", today)
            await pipe.hincrby(key, b"attempted", 1)  # type: ignore
            await pipe.hincrby(attempted_extra, b"found", 1)  # type: ignore
            await pipe.hincrby(key, b"succeeded", 1)  # type: ignore
            await pipe.hincrby(succeeded_extra, b"found", 1)  # type: ignore
            await pipe.delete(
                f"email:receipt_pending:{event.message_id}".encode("utf-8")
            )
            await pipe.zrem(b"email:receipt_pending", event.message_id)
            await jobs.enqueue_in_pipe(
                pipe,
                message.success_job.name,
                **message.success_job.kwargs,
                data_raw=data_raw,
            )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def handle_delivery_but_abandoned(itgs: Itgs, event: EmailEvent):
    logging.warning(
        f"email {event.message_id} was delivered, but the callback was already lost"
    )
    today = unix_dates.unix_timestamp_to_unix_date(time.time(), tz=tz)
    key = f"stats:email_events:daily:{today}".encode("ascii")
    attempted_extra = f"stats:email_events:daily:{today}:extra:attempted".encode(
        "ascii"
    )
    succeeded_extra = f"stats:email_events:daily:{today}:extra:succeeded".encode(
        "ascii"
    )

    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_events:daily:earliest", today)
            await pipe.hincrby(key, b"attempted", 1)  # type: ignore
            await pipe.hincrby(attempted_extra, b"abandoned", 1)  # type: ignore
            await pipe.hincrby(key, b"succeeded", 1)  # type: ignore
            await pipe.hincrby(succeeded_extra, b"abandoned", 1)  # type: ignore
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def handle_bounce_and_found(itgs: Itgs, event: EmailEvent, message: EmailPending):
    logging.debug(f"email {event.message_id} bounced to {message.email} and was found")
    today = unix_dates.unix_timestamp_to_unix_date(
        message.send_initially_queued_at, tz=tz
    )
    key = f"stats:email_events:daily:{today}".encode("ascii")
    attempted_extra = f"stats:email_events:daily:{today}:extra:attempted".encode(
        "ascii"
    )
    bounced_extra = f"stats:email_events:daily:{today}:extra:bounced".encode("ascii")

    notification = cast(EmailBounceNotification, event.notification)

    data_raw = encode_data_for_failure_job(
        message,
        EmailFailureInfo(
            step="receipt",
            error_identifier="Bounce",
            retryable=False,
            extra=f"{notification.reason.primary}/{notification.reason.secondary}",
        ),
    )

    redis = await itgs.redis()
    jobs = await itgs.jobs()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_events:daily:earliest", today)
            await pipe.hincrby(key, b"attempted", 1)  # type: ignore
            await pipe.hincrby(attempted_extra, b"found", 1)  # type: ignore
            await pipe.hincrby(key, b"bounced", 1)  # type: ignore
            await pipe.hincrby(  # type: ignore
                bounced_extra,  # type: ignore
                f"found:{notification.reason.primary}:{notification.reason.secondary}".encode(  # type: ignore
                    "utf-8"
                ),
                1,
            )
            await pipe.delete(
                f"email:receipt_pending:{event.message_id}".encode("utf-8")
            )
            await pipe.zrem(b"email:receipt_pending", event.message_id)
            await jobs.enqueue_in_pipe(
                pipe,
                message.failure_job.name,
                **message.failure_job.kwargs,
                data_raw=data_raw,
            )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def handle_bounce_but_abandoned(itgs: Itgs, event: EmailEvent):
    logging.debug(
        f"email {event.message_id} bounced, but the callback was already lost"
    )
    today = unix_dates.unix_timestamp_to_unix_date(time.time(), tz=tz)
    key = f"stats:email_events:daily:{today}".encode("ascii")
    attempted_extra = f"stats:email_events:daily:{today}:extra:attempted".encode(
        "ascii"
    )
    bounced_extra = f"stats:email_events:daily:{today}:extra:bounced".encode("ascii")

    notification = cast(EmailBounceNotification, event.notification)

    redis = await itgs.redis()
    jobs = await itgs.jobs()

    event_obj = event.model_dump()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_events:daily:earliest", today)
            await pipe.hincrby(key, b"attempted", 1)  # type: ignore
            await pipe.hincrby(attempted_extra, b"found", 1)  # type: ignore
            await pipe.hincrby(key, b"bounced", 1)  # type: ignore
            await pipe.hincrby(  # type: ignore
                bounced_extra,  # type: ignore
                f"abandoned:{notification.reason.primary}:{notification.reason.secondary}".encode(  # type: ignore
                    "utf-8"
                ),
                1,
            )
            await jobs.enqueue_in_pipe(
                pipe, "runners.emails.abandoned_email_callback", event_obj=event_obj
            )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def handle_complaint_and_found(
    itgs: Itgs, event: EmailEvent, message: EmailPending
):
    logging.debug(
        f"email {event.message_id} was complained about by {message.email} and was found"
    )
    today = unix_dates.unix_timestamp_to_unix_date(
        message.send_initially_queued_at, tz=tz
    )
    key = f"stats:email_events:daily:{today}".encode("ascii")
    attempted_extra = f"stats:email_events:daily:{today}:extra:attempted".encode(
        "ascii"
    )
    complaint_extra = f"stats:email_events:daily:{today}:extra:complaint".encode(
        "ascii"
    )

    notification = cast(EmailComplaintNotification, event.notification)

    data_raw = encode_data_for_failure_job(
        message,
        EmailFailureInfo(
            step="receipt",
            error_identifier="Complaint",
            retryable=False,
            extra=notification.feedback_type,
        ),
    )

    redis = await itgs.redis()
    jobs = await itgs.jobs()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_events:daily:earliest", today)
            await pipe.hincrby(key, b"attempted", 1)  # type: ignore
            await pipe.hincrby(attempted_extra, b"found", 1)  # type: ignore
            await pipe.hincrby(key, b"complaint", 1)  # type: ignore
            await pipe.hincrby(  # type: ignore
                complaint_extra,  # type: ignore
                f"found:{notification.feedback_type}".encode("utf-8"),  # type: ignore
                1,
            )
            await pipe.delete(
                f"email:receipt_pending:{event.message_id}".encode("utf-8")
            )
            await pipe.zrem(b"email:receipt_pending", event.message_id)
            await jobs.enqueue_in_pipe(
                pipe,
                message.failure_job.name,
                **message.failure_job.kwargs,
                data_raw=data_raw,
            )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def handle_complaint_and_abandoned(itgs: Itgs, event: EmailEvent):
    logging.debug(
        f"email {event.message_id} was complained about, but the callback was already lost"
    )
    today = unix_dates.unix_timestamp_to_unix_date(time.time(), tz=tz)
    key = f"stats:email_events:daily:{today}".encode("ascii")
    attempted_extra = f"stats:email_events:daily:{today}:extra:attempted".encode(
        "ascii"
    )
    complaint_extra = f"stats:email_events:daily:{today}:extra:complaint".encode(
        "ascii"
    )

    notification = cast(EmailComplaintNotification, event.notification)

    event_obj = event.model_dump()

    redis = await itgs.redis()
    jobs = await itgs.jobs()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_events:daily:earliest", today)
            await pipe.hincrby(key, b"attempted", 1)  # type: ignore
            await pipe.hincrby(attempted_extra, b"abandoned", 1)  # type: ignore
            await pipe.hincrby(key, b"complaint", 1)  # type: ignore
            await pipe.hincrby(  # type: ignore
                complaint_extra,  # type: ignore
                f"abandoned:{notification.feedback_type}".encode("utf-8"),  # type: ignore
                1,
            )
            await jobs.enqueue_in_pipe(
                pipe, "runners.emails.abandoned_email_callback", event_obj=event_obj
            )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


@dataclasses.dataclass
class RunStats:
    attempted: int = 0
    succeeded_and_found: int = 0
    succeeded_but_abandoned: int = 0
    bounced_and_found: int = 0
    bounced_but_abandoned: int = 0
    complaint_and_found: int = 0
    complaint_and_abandoned: int = 0


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.emails.receipt_reconciliation")

    asyncio.run(main())
