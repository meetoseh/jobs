"""Sweeps `stripe:queued_syncs`, performing overdue syncs one at a time"""

import time
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.shared.sync_user_stripe_revenue_cat import sync_user_stripe_revenue_cat
from redis_helpers.cond_zpopmin import cond_zpopmin_safe

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 10
"""The maximum duration in seconds this job can run before stopping itself to allow
other jobs to run
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Performs any over-due stripe syncs one at a time. This does not use a purgatory,
    but it also does not remove entries until the sync succeeds. To prevent repeatedly
    looping on the same customer, if an error occurs, the sync is requeued at a much
    later time (1 day) to allow time for human intervention.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    redis = await itgs.redis()
    while True:
        loop_at = time.time()
        if loop_at - started_at >= MAX_JOB_TIME_SECONDS:
            logging.warning(
                f"Stopping {__name__} to allow other jobs to run (time exhausted)"
            )
            return

        if gd.received_term_signal:
            logging.info(f"Stopping {__name__} early due to signal")
            return

        item = await cond_zpopmin_safe(itgs, b"stripe:queued_syncs", int(started_at))
        if item is None:
            logging.debug(f"No more stripe syncs pending")
            return

        try:
            await sync_user_stripe_revenue_cat(itgs, user_sub=item.decode("utf-8"))
        except Exception:
            await redis.zadd(
                b"stripe:queued_syncs", mapping={item: loop_at + 86400}, nx=True
            )
            raise


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sweep_stripe_syncs")

    asyncio.run(main())
