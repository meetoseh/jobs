"""SMS Receipt Stale Detection Job"""
import time
from typing import Literal, Optional
from error_middleware import handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from lib.sms.sms_info import PendingSMS, SMSFailureInfo, encode_data_for_failure_job
from redis_helpers.get_stale_receipts import get_stale_receipts_safe

category = JobCategory.LOW_RESOURCE_COST
RETRY_POLL_TIME_SECONDS = 900
"""How far into the future we update the score of items that we fail to"""

REDIS_BATCH_SIZE = 10
"""How many items are updated and returned atomically in redis at a time;
must be small enough to avoid stalling other redis clients
"""

MAX_JOB_TIME_SECONDS = 50
"""After this time the job stops to avoid blocking other jobs"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Checks for items whose score in the Receipt Pending Set, interpreted
    as POSIX time, is older than the current time. Such items have their failure
    callback invoked and their score is bumped to 15 minutes in the future
    (RETRY_POLL_TIME_SECONDS)

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"sms:receipt_stale_detection_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:sms:receipt_stale_job",
            b"started_at",
            str(started_at).encode("ascii"),
        )

        callbacks_queued: int = 0
        stop_reason: Optional[
            Literal["list_exhausted", "time_exhausted", "signal"]
        ] = None
        jobs = await itgs.jobs()

        while True:
            if gd.received_term_signal:
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                stop_reason = "time_exhausted"
                break

            cycled = await get_stale_receipts_safe(
                itgs,
                b"sms:pending",
                started_at,
                time.time() + RETRY_POLL_TIME_SECONDS,
                REDIS_BATCH_SIZE,
            )
            if not cycled:
                stop_reason = "list_exhausted"
                break

            callbacks_queued += len(cycled)
            for receipt_raw in cycled:
                try:
                    pending = PendingSMS.from_redis_mapping(receipt_raw)
                except Exception as e:
                    await handle_error(
                        e, extra_info=f"bad receipt in pending set: {receipt_raw=}"
                    )
                    continue

                logging.debug(f"Queueing failure callback on {pending}")
                await jobs.enqueue(
                    pending.failure_job.name,
                    **pending.failure_job.kwargs,
                    data_raw=encode_data_for_failure_job(
                        sms=pending,
                        failure_info=SMSFailureInfo(
                            action="pending",
                            identifier="StuckPending",
                            subidentifier=None,
                            retryable=True,
                            extra=None,
                        ),
                    ),
                )

        finished_at = time.time()
        logging.info(
            f"Receipt Stale Detection Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Callbacks Queued: {callbacks_queued}"
        )
        await redis.hset(
            b"stats:sms:receipt_stale_job",
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"stop_reason": stop_reason.encode("utf-8"),
                b"callbacks_queued": callbacks_queued,
            },
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sms.receipt_stale_detection")

    asyncio.run(main())
