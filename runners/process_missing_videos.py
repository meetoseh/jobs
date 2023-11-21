from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Determines what journeys, if any, are missing a sample video file export or a regular
    video file export and queues the corresponding jobs to create them.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    jobs = await itgs.jobs()
    batch_size = 50
    last_uid: Optional[str] = None

    while True:
        response = await cursor.execute(
            """
            SELECT uid, sample_content_file_id, video_content_file_id FROM journeys 
            WHERE 
                video_content_file_id IS NULL 
                OR sample_content_file_id IS NULL 
                AND (? IS NULL OR uid > ?)
            LIMIT ?
            """,
            (
                last_uid,
                last_uid,
                batch_size,
            ),
        )

        if not response.results:
            break

        for row in response.results:
            uid: str = row[0]
            sample_content_file_id: Optional[int] = row[1]
            video_content_file_id: Optional[int] = row[2]

            last_uid = uid

            if sample_content_file_id is None:
                logging.debug(
                    f"Queueing process_journey_video_sample for journey {uid=}"
                )
                await jobs.enqueue(
                    "runners.process_journey_video_sample", journey_uid=uid
                )
            if video_content_file_id is None:
                logging.debug(f"Queueing process_journey_video for journey {uid=}")
                await jobs.enqueue("runners.process_journey_video", journey_uid=uid)

        if len(response.results) < batch_size:
            break


if __name__ == "__main__":
    import asyncio
    import yaml
    import logging.config

    async def main():
        gd = GracefulDeath()
        with open("logging.yaml") as f:
            logging_config = yaml.safe_load(f)

        logging.config.dictConfig(logging_config)
        async with Itgs() as itgs:
            await execute(itgs, gd)

    asyncio.run(main())
