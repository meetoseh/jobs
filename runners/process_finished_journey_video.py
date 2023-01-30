import asyncio
import random
from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
from temp_files import temp_file
from jobs import JobCategory
from content import hash_content_sync
from error_middleware import handle_warning
import socket
import time
import videos
import json

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, journey_uid: str, s3_file_key: str, sha512: str
):
    """Finalizes processing for a journey video where we've already produced the "raw"
    video and uploaded it to s3. This involves performing our standard video processing
    (which produces a variety of exports), uploading those to s3, and storing them in our
    database. When this completes successfully, we swap the journey's video, deleting
    the old one and all of its constituent exports.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_uid (str): the uid of the journey the video is for
        s3_file_key (str): the s3 key where the video file is stored
    """

    async def bounce():
        slack = await itgs.slack()
        await slack.send_ops_message(f"Bouncing {__name__} for {journey_uid=}")
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_finished_journey_video",
            journey_uid=journey_uid,
            s3_file_key=s3_file_key,
            sha512=sha512,
        )

    with temp_file() as source_filepath:
        files = await itgs.files()
        with open(source_filepath, "wb") as f:
            await files.download(
                f, bucket=files.default_bucket, key=s3_file_key, sync=True
            )

        downloaded_sha512 = hash_content_sync(source_filepath)
        if downloaded_sha512 != sha512:
            await handle_warning(
                f"{__name__}:sha512_mismatch",
                f"not finalizing processing on {journey_uid=} - file was corrupted ({sha512=}, {downloaded_sha512=})",
            )
            return

        slack = await itgs.slack()
        await slack.send_ops_message(
            f"Finalizing video for {journey_uid=} on {socket.gethostname()}"
        )

        started_at = time.time()
        try:
            result = await videos.process_video(
                source_filepath,
                itgs=itgs,
                gd=gd,
                max_file_size=1024 * 1024 * 1024 * 5,
                name_hint="journey_video",
                exports=videos.INSTAGRAM_VERTICAL,
            )
        except videos.ProcessVideoAbortedException:
            return await bounce()

        conn = await itgs.conn()
        cursor = conn.cursor("weak")
        for _ in range(10):
            response = await cursor.execute(
                """
                SELECT
                    content_files.uid
                FROM journeys
                LEFT OUTER JOIN content_files ON content_files.id = journeys.video_content_file_id
                WHERE journeys.uid=?
                """,
                (journey_uid,),
            )

            if not response.results:
                await handle_warning(
                    f"{__name__}:journey_deleted",
                    f"Journey {journey_uid=} was deleted while processing its video",
                )
                jobs = await itgs.jobs()
                await jobs.enqueue("runners.delete_content_file", uid=result.uid)
                return

            old_video_content_file_uid: Optional[str] = response.results[0][0]

            response = await cursor.execute(
                """
                UPDATE journeys
                SET video_content_file_id=content_files.id
                FROM content_files
                WHERE
                    journeys.uid = ?
                    AND content_files.uid = ?
                    AND (
                        (? IS NULL AND journeys.video_content_file_id IS NULL) 
                        OR EXISTS (
                            SELECT 1 FROM content_files AS cf
                            WHERE cf.id = journeys.video_content_file_id
                              AND cf.uid = ?
                        )
                    )
                """,
                (
                    journey_uid,
                    result.uid,
                    old_video_content_file_uid,
                    old_video_content_file_uid,
                ),
            )
            if response.rows_affected is not None and response.rows_affected > 0:
                break

            await asyncio.sleep(1 + random.random())
        else:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.delete_content_file", uid=result.uid)

            await handle_warning(
                f"{__name__}:failed_to_swap_video",
                f"Failed to swap video for {journey_uid=} after 10 attempts",
            )
            return

        if (
            old_video_content_file_uid is not None
            and old_video_content_file_uid != result.uid
        ):
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.delete_content_file", uid=old_video_content_file_uid
            )

        redis = await itgs.redis()
        purgatory_key = json.dumps(
            {"key": s3_file_key, "bucket": files.default_bucket}, sort_keys=True
        )
        await files.delete(bucket=files.default_bucket, key=s3_file_key)
        await redis.zrem("files:purgatory", purgatory_key)

        await cursor.execute(
            "DELETE FROM s3_files WHERE key=?",
            (s3_file_key,),
        )

        await slack.send_ops_message(
            f"Finalized video for {journey_uid=} on {socket.gethostname()} in {time.time() - started_at:.2f}s"
        )
