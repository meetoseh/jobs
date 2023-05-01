from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Queues a job to generate a journey transcript on each journey that is missing one
    and isn't deleted.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    jobs = await itgs.jobs()

    last_content_file_id: Optional[int] = None
    batch_size: int = 50
    while True:
        response = await cursor.execute(
            """
            SELECT
                content_files.id,
                journeys.uid
            FROM content_files, journeys
            WHERE
                journeys.audio_content_file_id = content_files.id
                AND journeys.deleted_at IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM content_file_transcripts
                    WHERE
                        content_file_transcripts.content_file_id = content_files.id
                )
                AND (
                    ? IS NULL OR content_files.id > ?
                )
            ORDER BY content_files.id ASC
            LIMIT ?
            """,
            (last_content_file_id, last_content_file_id, batch_size),
        )
        if not response.results:
            break

        for _, journey_uid in response.results:
            if gd.received_term_signal:
                logging.info("received term signal; stopping early")
                return
            await jobs.enqueue(
                "runners.generate_journey_transcript", journey_uid=journey_uid
            )

        last_content_file_id = response.results[-1][0]


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.generate_missing_transcripts")

    asyncio.run(main())
