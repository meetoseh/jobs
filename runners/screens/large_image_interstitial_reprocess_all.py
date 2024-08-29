"""Queues jobs to reprocess every large image interstitial image"""

from typing import List, Optional, cast
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Queues reprocessing of all large image interstitial images

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    jobs = await itgs.jobs()

    last_uid = cast(Optional[str], None)
    batch_size: int = 100
    redis_batch_size: int = 10

    redis_batch: List[str] = []

    async def _send_redis_batch():
        if not redis_batch:
            return

        logging.info(f"Queueing batch of {len(redis_batch)} jobs")
        await itgs.ensure_redis_liveliness()
        redis = await itgs.redis()
        jobs = await itgs.jobs()
        async with redis.pipeline(transaction=False) as pipe:
            for image_file_uid in redis_batch:
                await jobs.enqueue_in_pipe(
                    pipe,
                    "runners.screens.large_image_interstitial_reprocess_image",
                    image_file_uid=image_file_uid,
                )
            await pipe.execute()

        redis_batch.clear()

    while True:
        response = await cursor.execute(
            """
SELECT
    client_flow_images.uid,
    image_files.uid
FROM client_flow_images, image_files
WHERE
    client_flow_images.list_slug = ?
    AND client_flow_images.image_file_id = image_files.id
    AND (? IS NULL OR client_flow_images.uid > ?)
ORDER BY client_flow_images.uid ASC
LIMIT ?
            """,
            ("large_image_interstitial", last_uid, last_uid, batch_size),
        )

        rows = response.results or []

        for row in rows:
            if gd.received_term_signal:
                logging.info("Term signal received, stopping early")
                return

            row_client_flow_image_uid = cast(str, row[0])
            row_image_file_uid = cast(str, row[1])

            redis_batch.append(row_image_file_uid)
            if len(redis_batch) >= redis_batch_size:
                await _send_redis_batch()

            last_uid = row_client_flow_image_uid

        if len(rows) < batch_size:
            await _send_redis_batch()
            return


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.screens.large_image_interstitial_reprocess_all")

    asyncio.run(main())
