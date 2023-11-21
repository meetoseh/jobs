"""Sign in with Oseh Send Delayed Email Verification Job"""
from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import time

from redis_helpers.move_cold_to_hot import move_cold_to_hot_safe

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""If this job detects its been running for more than this duration in seconds,
it stops to allow other jobs to run
"""

SUBQUEUE_BACKPRESSURE_THRESHOLD = 1_000
"""The length of the Email To Send queue that triggers backpressure"""

REDIS_MOVE_BATCH_SIZE = 100
"""How many items we try to move from the delayed queue to the email to send
queue at a time
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates overdue jobs from the Delayed Email Verification Queue to the
    Email To Send queue

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    redis = await itgs.redis()
    await redis.hset(  # type: ignore
        b"stats:sign_in_with_oseh:send_delayed_job",  # type: ignore
        b"started_at",  # type: ignore
        str(started_at).encode("ascii"),  # type: ignore
    )

    stop_reason: Optional[str] = None
    num_moved: int = 0
    while True:
        if gd.received_term_signal:
            stop_reason = "signal"
            break

        if time.time() - started_at >= MAX_JOB_TIME_SECONDS:
            stop_reason = "time_exhausted"
            break

        move_result = await move_cold_to_hot_safe(
            itgs,
            b"sign_in_with_oseh:delayed_emails",
            b"email:to_send",
            int(started_at),
            REDIS_MOVE_BATCH_SIZE,
            backpressure=SUBQUEUE_BACKPRESSURE_THRESHOLD,
        )

        num_moved += move_result.num_moved

        if move_result.stop_reason == "backpressure":
            stop_reason = "backpressure"
            break

        if move_result.stop_reason in ("src_empty", "max_score"):
            stop_reason = "list_exhausted"
            break

        assert move_result.stop_reason == "max_count", move_result

    finished_at = time.time()
    logging.debug(
        "SIWO Send Delayed Email Verifications Job Finished:\n"
        f"- Started At: {started_at:.3f}\n"
        f"- Finished At: {finished_at:.3f}\n"
        f"- Running Time: {finished_at - started_at:.3f}\n"
        f"- Stop Reason: {stop_reason}\n"
        f"- Moved: {num_moved}\n"
    )
    await redis.hset(
        b"stats:sign_in_with_oseh:send_delayed_job",  # type: ignore
        mapping={
            b"finished_at": str(finished_at).encode("ascii"),
            b"running_time": str(finished_at - started_at).encode("ascii"),
            b"attempted": str(num_moved).encode("ascii"),
            b"moved": str(num_moved).encode("ascii"),
            b"stop_reason": stop_reason.encode("ascii"),
        },
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.siwo.send_delayed_email_verifications")

    asyncio.run(main())
