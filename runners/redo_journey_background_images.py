"""Queues jobs to redo all the journey background images whose original file
is still available. This is useful whenever a new export is added. This file
supports being run directly, which will simply queue this job.
"""
import asyncio
import json
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import time
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Queues a job for each journey background image which still has an original
    file available to be reprocessed.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue("runners.redo_journey_background_images")

    conn = await itgs.conn()
    cursor = conn.cursor("none")
    jobs = await itgs.jobs()

    biggest_id = 0
    batch_size = 25
    while True:
        if gd.received_term_signal:
            return await bounce()

        response = await cursor.execute(
            """
            SELECT id, uid FROM journey_background_images
            WHERE
                EXISTS (
                    SELECT 1 FROM image_files
                    WHERE image_files.id = journey_background_images.image_file_id
                      AND image_files.original_s3_file_id IS NOT NULL
                )
                AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (biggest_id, batch_size),
        )
        if not response.results:
            logging.info("Successfully queued all journey background images")
            return

        biggest_id = response.results[-1][0]

        now = time.time()
        serd_jobs = [
            json.dumps(
                {
                    "name": "runners.redo_journey_background_image",
                    "kwargs": {"journey_background_image_uid": uid},
                    "queued_at": now,
                }
            )
            for _, uid in response.results
        ]
        await jobs.conn.rpush(jobs.queue_key, *serd_jobs)


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.redo_journey_background_images")

    asyncio.run(main())
