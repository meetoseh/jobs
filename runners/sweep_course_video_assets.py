"""Sweeps over course videos and checks for missing subresources, like thumbnails
or transcripts, and creates jobs to generate them if they are missing.
"""

import logging
from typing import Optional, cast
from itgs import Itgs
from graceful_death import GracefulDeath
import time

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Sweeps over course videos and checks for missing subresources, like thumbnails
    or transcripts, and creates jobs to generate them if they are missing.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    started_at = time.time()
    max_uploaded_at = started_at - 3600

    last_uid: Optional[str] = None
    while True:
        response = await cursor.execute(
            """
WITH missing_subresources(course_video_uid, content_file_uid, has_thumbnails, has_transcript, last_uploaded_at) AS (
SELECT
    course_videos.uid,
    content_files.uid,
    EXISTS (
        SELECT 1 FROM course_video_thumbnail_images
        WHERE
            json_extract(course_video_thumbnail_images.source, '$.type') = 'frame'
            AND json_extract(course_video_thumbnail_images.source, '$.video_sha512') = content_files.original_sha512
    ),
    EXISTS (
        SELECT 1 FROM content_file_transcripts
        WHERE
            content_file_transcripts.content_file_id = course_videos.content_file_id
    ),
    course_videos.last_uploaded_at
FROM course_videos, content_files
WHERE
    content_files.id = course_videos.content_file_id
)
SELECT
    missing_subresources.course_video_uid,
    missing_subresources.content_file_uid,
    missing_subresources.has_thumbnails,
    missing_subresources.has_transcript
FROM missing_subresources
WHERE
    (NOT missing_subresources.has_thumbnails OR NOT missing_subresources.has_transcript)
    AND missing_subresources.last_uploaded_at < ?
    AND (? IS NULL OR missing_subresources.course_video_uid > ?)
ORDER BY missing_subresources.course_video_uid
LIMIT 10
            """,
            (max_uploaded_at, last_uid, last_uid),
        )

        if not response.results:
            break

        jobs = await itgs.jobs()
        async with jobs.conn.pipeline() as pipe:
            for row in response.results:
                course_video_uid = cast(str, row[0])
                content_file_uid = cast(str, row[1])
                has_thumbnails = bool(row[2])
                has_transcript = bool(row[3])

                last_uid = course_video_uid

                if not has_thumbnails:
                    logging.info(
                        f"Queueing thumbnail job for course video {course_video_uid=}, {content_file_uid=}"
                    )
                    await jobs.enqueue_in_pipe(
                        pipe,
                        "runners.generate_course_video_thumbnails",
                        content_file_uid=content_file_uid,
                    )

                if not has_transcript:
                    logging.info(
                        f"Queueing transcript job for course video {course_video_uid=}, {content_file_uid=}"
                    )
                    await jobs.enqueue_in_pipe(
                        pipe,
                        "runners.generate_course_transcript",
                        content_file_uid=content_file_uid,
                    )

            await pipe.execute()


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sweep_course_video_assets")

    asyncio.run(main())
