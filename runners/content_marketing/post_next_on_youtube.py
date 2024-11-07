"""Queues a job to post the oldest journey not yet on youtube to youtube"""

from typing import cast
from itgs import Itgs
from graceful_death import GracefulDeath
import asyncio
import logging

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Discovers the oldest journey not yet on youtube. If one is found, queues a job to
    post it to youtube.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    # note: we assumed at most 1 depth of variation_of_journey_id, and we only post the
    # longer one. if there are multiple variations with the same length, we break ties
    # by preferring the lower lexicographical uid
    response = await cursor.execute(
        """
SELECT
    journeys.uid
FROM journeys, content_files
WHERE
    journeys.deleted_at IS NULL
    AND journeys.special_category IS NULL
    AND journeys.video_content_file_id = content_files.id
    AND NOT EXISTS (
        SELECT 1 FROM journeys AS alt_journeys, content_files AS alt_content_files
        WHERE
            (
                alt_journeys.id = journeys.variation_of_journey_id
                OR alt_journeys.variation_of_journey_id = journeys.id
            )
            AND alt_journeys.deleted_at IS NULL
            AND alt_journeys.special_category IS NULL
            AND alt_journeys.video_content_file_id = alt_content_files.id
            AND (
                alt_content_files.duration_seconds > content_files.duration_seconds
                OR (
                    alt_content_files.duration_seconds = content_files.duration_seconds
                    AND alt_journeys.uid < journeys.uid
                )
            )    
    )
    AND NOT EXISTS (
        SELECT 1 FROM journey_youtube_videos
        WHERE
            journey_youtube_videos.journey_id = journeys.id
            AND journey_youtube_videos.finished_at IS NOT NULL
    )
ORDER BY journeys.created_at ASC
LIMIT 1
        """
    )

    if not response.results:
        logging.info("All journeys have already been posted to YouTube")
        return

    journey_uid = cast(str, response.results[0][0])
    logging.info(f"Queueing job to post {journey_uid=} on YouTube")

    jobs = await itgs.jobs()
    await jobs.enqueue(
        "runners.content_marketing.post_on_youtube", journey_uid=journey_uid
    )


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.content_marketing.post_next_on_youtube")

    asyncio.run(main())
