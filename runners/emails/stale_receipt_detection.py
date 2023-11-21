"""Removes stale receipts from the receipt pending set"""
from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import pytz
from lib.emails.email_info import (
    EmailFailureInfo,
    EmailPending,
    encode_data_for_failure_job,
)
from redis_helpers.ext_zpopmin import ext_zpopmin_safe
from lib.basic_redis_lock import basic_redis_lock
import dataclasses
import time

category = JobCategory.LOW_RESOURCE_COST
tz = pytz.timezone("America/Los_Angeles")


MAX_STALENESS = 60 * 60 * 24
"""Receipts older than this value in seconds are considered stale and will
be removed from the receipt pending set, triggering their failure job.
"""

MAX_JOB_TIME_SECONDS = 50
"""The maximum amount of time this job is allowed to run for. If exceeded,
stops with the stop reason `time_exhausted`
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Detects and removes old messages from the Receipt Pending Set, triggering
    their failure job.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"email:stale_receipt_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(  # type: ignore
            b"stats:email_events:stale_receipt_job",  # type: ignore
            b"started_at",  # type: ignore
            str(started_at).encode("ascii"),  # type: ignore
        )

        jobs = await itgs.jobs()
        run_stats = RunStats()
        stop_reason: Optional[str] = None

        max_score = started_at - MAX_STALENESS

        while True:
            if gd.received_term_signal:
                logging.debug("Email Stale Receipt Job - signal received, stopping")
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.debug(
                    "Email Stale Receipt Job - max job time exceeded, stopping"
                )
                stop_reason = "time_exhausted"
                break

            next_raw_mapping = await ext_zpopmin_safe(
                itgs, b"email:receipt_pending", b"email:receipt_pending:", max_score
            )

            if next_raw_mapping is None:
                logging.debug("Email Stale Receipt Job - no more items, stopping")
                stop_reason = "list_exhausted"
                break

            email = EmailPending.from_redis_mapping(next_raw_mapping)
            logging.debug(f"Abandoning stale email in receipt pending set: {email}")
            await jobs.enqueue(
                email.failure_job.name,
                **email.failure_job.kwargs,
                data_raw=encode_data_for_failure_job(
                    email=email,
                    info=EmailFailureInfo(
                        step="receipt",
                        error_identifier="ReceiptTimeout",
                        retryable=False,
                        extra=None,
                    ),
                ),
            )
            run_stats.abandoned += 1

        finished_at = time.time()
        logging.info(
            f"Email Stale Receipt Detection Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Abandoned: {run_stats.abandoned}\n"
        )
        await redis.hset(  # type: ignore
            b"stats:email_events:stale_receipt_job",  # type: ignore
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"stop_reason": stop_reason.encode("utf-8"),
                b"abandoned": run_stats.abandoned,
            },
        )


@dataclasses.dataclass
class RunStats:
    abandoned: int = 0


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.emails.stale_receipt_detection")

    asyncio.run(main())
