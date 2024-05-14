"""Cleans up user_client_screens for users who haven't been back in a while"""

import time
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST

STALE_TIME_SECONDS = 60 * 60 * 24 * 30


async def execute(itgs: Itgs, gd: GracefulDeath):
    """For each user which has at least one row in user_client_screens, but no rows in
    user_client_screens more recent than the given stale time in seconds, delete all of their
    user_client_screens rows.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor()

    now = time.time()
    cutoff = now - STALE_TIME_SECONDS

    # PERF:
    #
    # With just the correlated subquery
    # --SCAN user_client_screens USING COVERING INDEX user_client_screens_user_id_outer_counter_inner_counter_idx
    # --CORRELATED SCALAR SUBQUERY 1
    #   |--SEARCH ucs USING INDEX user_client_screens_user_id_outer_counter_inner_counter_idx (user_id=?)
    #
    # Adding a redundant added_at <= ? to the subquery
    # --SEARCH user_client_screens USING INDEX user_client_screens_added_at_idx (added_at<?)
    # --CORRELATED SCALAR SUBQUERY 1
    #   |--SEARCH ucs USING INDEX user_client_screens_user_id_outer_counter_inner_counter_idx (user_id=?)

    await cursor.execute(
        """
DELETE FROM user_client_screens
WHERE
    added_at <= ?
    AND NOT EXISTS (
        SELECT 1 FROM user_client_screens AS ucs
        WHERE ucs.user_id = user_client_screens.user_id
          AND ucs.added_at > ?
    )
        """,
        (cutoff, cutoff),
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.cleanup_old_user_client_screens")

    asyncio.run(main())
