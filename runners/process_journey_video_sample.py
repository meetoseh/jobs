from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
from error_middleware import handle_warning
from jobs import JobCategory
from temp_files import temp_file, temp_dir
from content import hash_content_sync, upload_s3_file_and_put_in_purgatory, S3File
import shareables.journey_audio.main
import secrets
import socket
import time
import os

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, journey_uid: str):
    """Produces a new video sample for the journey with the given uid, uploads
    it to S3, then queues process_finished_journey_video_sample to re-encode and
    store in the database.

    The result is a 15s vertical video clip containing the journey title,
    instructor name, the first 15s of the journey audio, the journey background
    image, and a visualization of the journey audio.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_uid (str): The UID of the journey to process
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_journey_video_sample", journey_uid=journey_uid
        )

    conn = await itgs.conn()
    cursor = conn.cursor("strong")

    response = await cursor.execute(
        """
        SELECT
            journeys.title,
            instructors.name,
            audio_contents.uid,
            audio_contents.original_sha512,
            audio_s3_files.key,
            background_images.uid,
            background_images.original_sha512,
            background_s3_files.key
        FROM journeys
        JOIN instructors ON instructors.id = journeys.instructor_id
        JOIN content_files AS audio_contents ON audio_contents.id = journeys.audio_content_file_id
        LEFT OUTER JOIN s3_files AS audio_s3_files ON audio_s3_files.id = audio_contents.original_s3_file_id
        JOIN image_files AS background_images ON background_images.id = journeys.background_image_file_id
        LEFT OUTER JOIN s3_files AS background_s3_files ON background_s3_files.id = background_images.original_s3_file_id
        WHERE
            journeys.uid = ?
        """,
        (journey_uid,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}:dne", f"no journey with {journey_uid=} exists"
        )
        return

    journey_title: str = response.results[0][0]
    instructor_name: str = response.results[0][1]
    audio_uid: str = response.results[0][2]
    # may null sha512 later
    audio_sha512: Optional[str] = response.results[0][3]
    audio_key: Optional[str] = response.results[0][4]
    background_uid: str = response.results[0][5]
    # may null sha512 later
    background_sha512: Optional[str] = response.results[0][6]
    background_key: Optional[str] = response.results[0][7]

    if audio_key is None:
        # the original has been lost, we will find the highest quality mp4 export
        response = await cursor.execute(
            """
            SELECT
                content_file_exports.format,
                content_file_exports.bandwidth,
                content_file_exports.codecs,
                content_file_exports.quality_parameters,
                content_file_export_parts.uid,
                s3_files.key
            FROM content_file_exports
            JOIN content_file_export_parts ON content_file_export_parts.content_file_export_id = content_file_exports.id
            JOIN s3_files ON s3_files.id = content_file_export_parts.s3_file_id
            WHERE
                EXISTS (
                    SELECT 1 FROM content_files
                    WHERE 
                        content_files.uid = ?
                        AND content_files.id = content_file_exports.content_file_id
                )
                AND content_file_exports.format = 'mp4'
            ORDER BY content_file_exports.bandwidth DESC
            LIMIT 1
            """,
            (audio_uid,),
        )
        if not response.results:
            await handle_warning(
                f"{__name__}:no_audio",
                f"for {journey_uid=}, the original audio is lost and no mp4 export exists",
            )
            return

        audio_format: str = response.results[0][0]
        audio_bandwidth: int = response.results[0][1]
        audio_codecs: str = response.results[0][2]
        audio_quality_parameters: str = response.results[0][3]
        audio_export_uid: str = response.results[0][4]
        audio_key: str = response.results[0][5]

        await handle_warning(
            f"{__name__}:no_original_audio",
            (
                f"for {journey_uid=}, the original audio is lost, "
                f"so we are using the highest quality mp4 export instead: "
                f"{audio_format=} {audio_bandwidth=} {audio_codecs=} "
                f"{audio_quality_parameters=} {audio_export_uid=} {audio_key=}"
            ),
        )
        del audio_format
        del audio_bandwidth
        del audio_codecs
        del audio_quality_parameters
        del audio_export_uid
        audio_sha512 = None

    if background_key is None:
        # the original has been lost, we will find the highest quality webp export at the
        # correct size
        response = await cursor.execute(
            """
            SELECT
                image_file_exports.format,
                image_file_exports.quality_settings,
                s3_files.key
            FROM image_file_exports
            JOIN s3_files ON s3_files.id = image_file_exports.s3_file_id
            WHERE
                EXISTS (
                    SELECT 1 FROM image_files
                    WHERE
                        image_files.uid = ?
                        AND image_files.id = image_file_exports.image_file_id
                )
                AND image_file_exports.format = 'webp'
                AND image_file_exports.width = 1080
                AND image_file_exports.height = 1920
            ORDER BY s3_files.file_size DESC
            LIMIT 1
            """,
            (background_uid,),
        )

        if not response.results:
            await handle_warning(
                f"{__name__}:no_image",
                f"for {journey_uid=}, the original background is lost and no 1080x1920 webp export exists",
            )
            return

        background_format: str = response.results[0][0]
        background_quality_settings: str = response.results[0][1]
        background_key: str = response.results[0][2]

        await handle_warning(
            f"{__name__}:no_original_background",
            (
                f"for {journey_uid=}, the original background is lost, "
                f"so we are using the highest quality 1080x1920 webp export instead: "
                f"{background_format=} {background_quality_settings=} {background_key=}"
            ),
        )

        del background_format
        del background_quality_settings
        background_sha512 = None

    with temp_file() as audio_source_path, temp_file() as background_source_path:
        files = await itgs.files()

        with open(audio_source_path, "wb") as f:
            await files.download(
                f, bucket=files.default_bucket, key=audio_key, sync=True
            )

        if audio_sha512 is not None:
            downloaded_sha512 = hash_content_sync(audio_source_path)
            if downloaded_sha512 != audio_sha512:
                await handle_warning(
                    f"{__name__}:sha512_mismatch",
                    f"for {journey_uid=}, {audio_key=} has a sha512 mismatch: {downloaded_sha512=} {audio_sha512=} - NOT PROCESSING",
                )
                return

        with open(background_source_path, "wb") as f:
            await files.download(
                f, bucket=files.default_bucket, key=background_key, sync=True
            )

        if background_sha512 is not None:
            downloaded_sha512 = hash_content_sync(background_source_path)
            if downloaded_sha512 != background_sha512:
                await handle_warning(
                    f"{__name__}:sha512_mismatch",
                    f"for {journey_uid=}, {background_key=} has a sha512 mismatch: {downloaded_sha512=} {background_sha512=} - NOT PROCESSING",
                )
                return

        if gd.received_term_signal:
            await bounce()
            return

        slack = await itgs.slack()
        await slack.send_ops_message(
            f"Producing journey video sample for {journey_uid=}, {journey_title=}, {instructor_name=} on {socket.gethostname()}"
        )
        started_at = time.time()
        with temp_dir() as destination_dir:
            result = shareables.journey_audio.main.run_pipeline(
                audio_source_path,
                background_source_path,
                journey_title,
                instructor_name,
                duration=15,
                dest_folder=destination_dir,
            )

            finished_at = time.time()
            await slack.send_ops_message(
                f"Finished producing journey video sample for {journey_uid=}, {journey_title=}, {instructor_name=} on {socket.gethostname()} in {finished_at - started_at:.2f}s; queueing job to finalize"
            )

            sha512 = hash_content_sync(result.output_path)

            file_size = os.path.getsize(result)
            s3_file = S3File(
                uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
                bucket=files.default_bucket,
                key=f"s3_files/videos/journey_video_samples/{secrets.token_urlsafe(16)}.mp4",
                file_size=file_size,
                content_type="video/mp4",
                created_at=finished_at,
            )

            await upload_s3_file_and_put_in_purgatory(
                s3_file, result.output_path, itgs=itgs, protect_for=60 * 60 * 24 * 7
            )
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.process_finished_journey_video_sample",
                journey_uid=journey_uid,
                s3_file_key=s3_file.key,
                sha512=sha512,
            )
