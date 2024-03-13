"""Prunes old user <-> home screen images relationships"""

from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import time

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Deletes old user_home_screen_images rows to keep the table from growing too large.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor()
    await cursor.execute(
        "DELETE FROM user_home_screen_images WHERE created_at < ?",
        [time.time() - 60 * 60 * 24 * 30],
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.prune_old_user_home_screen_images")

    asyncio.run(main())
