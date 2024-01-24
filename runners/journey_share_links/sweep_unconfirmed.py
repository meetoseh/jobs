"""Sweeps unconfirmed journey share link views and either removes or persists stale ones"""
import time
from typing import Literal, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from dataclasses import dataclass

from lib.basic_redis_lock import basic_redis_lock
from redis_helpers.journey_share_links_sweep_unconfirmed import (
    journey_share_links_sweep_unconfirmed_safe,
)

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""The maximum amount of time this job will run before yielding to other jobs"""

REDIS_BATCH_SIZE = 100
"""How many items are processed within a redis script call, i.e., before yielding
to other commands
"""


@dataclass
class _RunStats:
    found: int = 0
    removed: int = 0
    queued: int = 0


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Looks through unconfirmed journey share link views for stale items. For
    those which were for an invalid code, i.e., which were stored for
    ratelimiting purposes, they are removed from the view pseudo-set and
    unconfirmed views sorted set. For those which were for a valid code,
    they are added to the view to log queue to be persisted.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"journey_share_links:sweep_unconfirmed_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:journey_share_links:sweep_unconfirmed_job",  # type: ignore
            mapping={
                b"started_at": str(started_at).encode("ascii"),
            },
        )

        stop_reason: Optional[
            Literal["list_exhausted", "time_exhausted", "signal", "backpressure"]
        ] = None
        stats = _RunStats()

        to_log_length = await redis.llen(b"journey_share_links:views_to_log")  # type: ignore
        if to_log_length > 1000:
            await handle_warning(
                f"{__name__}:backpressure",
                f"journey_share_links:views_to_log has {to_log_length} items, "
                f"backpressure is preventing sweeping unconfirmed views",
            )
            stop_reason = "backpressure"

        while stop_reason is None:
            loop_at = time.time()

            if gd.received_term_signal:
                logging.info("Received term signal, stopping early")
                stop_reason = "signal"
                continue

            if loop_at - started_at > MAX_JOB_TIME_SECONDS:
                logging.info("Time exhausted, stopping early")
                stop_reason = "time_exhausted"
                continue

            progress = await journey_share_links_sweep_unconfirmed_safe(
                itgs, loop_at, REDIS_BATCH_SIZE
            )
            logging.debug(f"Made progress: {progress=}")

            stats.found += progress.found
            stats.removed += progress.removed
            stats.queued += progress.queued

            if not progress.have_more_work:
                logging.info("No more stale views to process")
                stop_reason = "list_exhausted"

        finished_at = time.time()
        await redis.hset(
            b"stats:journey_share_links:sweep_unconfirmed_job",  # type: ignore
            mapping={
                b"finished_at": str(finished_at).encode("ascii"),
                b"running_time": str(finished_at - started_at).encode("ascii"),
                b"found": str(stats.found).encode("ascii"),
                b"removed": str(stats.removed).encode("ascii"),
                b"queued": str(stats.queued).encode("ascii"),
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.journey_share_links.sweep_unconfirmed")

    asyncio.run(main())
