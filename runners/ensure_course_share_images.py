"""Ensures that all courses have the correct share image"""

from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath

from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Ensures all courses have the appropriate course share image. This utilizes
    the fact that `process_course_share_image` only performs required work.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")
    jobs = await itgs.jobs()
    redis = await itgs.redis()

    last_uid: Optional[str] = None
    db_limit = 100
    redis_limit = 10

    while True:
        response = await cursor.execute(
            "SELECT uid FROM courses WHERE (? IS NULL OR uid > ?) AND hero_image_file_id IS NOT NULL ORDER BY uid ASC LIMIT ?",
            (last_uid, last_uid, db_limit),
        )

        if not response.results:
            break

        for start_idx in range(0, len(response.results), redis_limit):
            uids = [
                row[0] for row in response.results[start_idx : start_idx + redis_limit]
            ]
            async with redis.pipeline(transaction=False) as pipe:
                for uid in uids:
                    await jobs.enqueue_in_pipe(
                        pipe, "runners.process_course_share_image", course_uid=uid
                    )
                await pipe.execute()

        last_uid = response.results[-1][0]


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.ensure_course_share_images")

    asyncio.run(main())
