"""Touch Stale Detection Job"""
import time
from typing import Dict, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.touch_pending_cleanup_stale import (
    touch_pending_cleanup_stale,
    ensure_touch_pending_cleanup_stale_script_exists,
)
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
from functools import partial
import pytz
import unix_dates

category = JobCategory.LOW_RESOURCE_COST

STALE_AGE_SECONDS = 60 * 60 * 36
"""The number of seconds before we consider a touch stale. This should be longer
than any of the subsystem timeouts to avoid false alerts, but less than 48 hours
so that we can backdate the stats appropriately
"""

MAX_JOB_TIME_SECONDS = 50
"""The amount of time after which we stop due to time_exhausted to allow other
jobs to run
"""

CLEANUP_BATCH_SIZE = 12
"""The number of items we cleanup within a single redis transaction. Should be
small enough not to block the redis server for too long, but large enough to
avoid excessive overhead
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Detects entries which have remained in the `touch:pending` sorted set
    for excessively long. Any such entries are cleaned up (triggering the failure
    callback, if there is one), and an alert is sent since this guarrantees one
    of the subsystems failed, and its timeout technique also failed.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    started_at_unix_date = unix_dates.unix_timestamp_to_unix_date(
        started_at, tz=pytz.timezone("America/Los_Angeles")
    )
    redis = await itgs.redis()
    await redis.hset(
        b"stats:touch_stale:detection_job",
        mapping={
            b"started_at": str(started_at).encode("ascii"),
        },
    )

    earliest_key = b"stats:touch_stale:daily:earliest"
    stale = 0
    stop_reason: Optional[str] = None

    async def prep_cleanup(force: bool):
        await ensure_touch_pending_cleanup_stale_script_exists(redis, force=force)

    async def do_cleanup():
        return await touch_pending_cleanup_stale(
            redis,
            b"touch:pending",
            b"jobs:hot",
            started_at - STALE_AGE_SECONDS,
            CLEANUP_BATCH_SIZE,
            time.time(),
        )

    async def prep_stats(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def do_stats(num_stale: int):
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(redis, earliest_key, started_at_unix_date)
            await pipe.hincrby(
                f"stats:touch_stale:daily:{started_at_unix_date}".encode("utf-8"),
                b"stale",
                num_stale,
            )
            await pipe.execute()

    while True:
        if gd.received_term_signal:
            logging.warning(
                "Touch Stale Detection received term signal, stopping early"
            )
            stop_reason = "signal"
            break

        if time.time() - started_at > MAX_JOB_TIME_SECONDS:
            logging.warning("Touch Stale Detection reached time limit, stopping early")
            stop_reason = "time_exhausted"
            break

        num_stale = await run_with_prep(prep_cleanup, do_cleanup)
        if num_stale == 0:
            stop_reason = "list_exhausted"
            break

        stale += num_stale
        await run_with_prep(prep_stats, partial(do_stats, num_stale))

        if num_stale < CLEANUP_BATCH_SIZE:
            stop_reason = "list_exhausted"
            break

    assert stop_reason is not None
    finished_at = time.time()
    logging.info(
        f"Touch Stale Detection Job Finished:\n"
        f"- Started At: {started_at:.3f}\n"
        f"- Finished At: {finished_at:.3f}\n"
        f"- Running Time: {finished_at - started_at:.3f}\n"
        f"- Stop Reason: {stop_reason}\n"
        f"- Stale: {stale}"
    )
    await redis.hset(
        b"stats:touch_stale:detection_job",
        mapping={
            b"finished_at": finished_at,
            b"running_time": finished_at - started_at,
            b"stale": stale,
            b"stop_reason": stop_reason.encode("utf-8"),
        },
    )

    if stale > 0:
        await handle_warning(
            identifier=f"{__name__}:found_stale",
            text=f"Found {stale} stale touch entries within touch:pending. This "
            "guarrantees there is a bug in either the touch system itself or one of "
            "its subsystems, as this timeout is redundant.",
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.stale_detection")

    asyncio.run(main())
