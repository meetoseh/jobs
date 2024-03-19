"""Extracts thumbnail images for an onboarding video."""

import json
import os
import secrets
import shutil
import time
from typing import Optional, cast

import aiofiles
from content import hash_filelike
from images import ProcessImageAbortedException, process_image
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
from lib.progressutils.async_progress_tracking_bytes_io import (
    AsyncProgressTrackingReadableBytesIO,
    AsyncProgressTrackingWritableBytesIO,
)
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    CustomFailureReasonException,
    success_or_failure_reporter,
)
from runners.generate_course_video_thumbnails import (
    TARGETS,
    THUMBNAIL_FRAMES,
    extract_frame,
)
from temp_files import temp_dir, temp_file

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    content_file_uid: str,
    job_progress_uid: Optional[str] = None,
):
    """Extracts thumbnail images for an onboarding video.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        content_file_uid (str): the uid of the content file to extract thumbnails from.
            must have a single-file video original file, and if that's unavailable,
            must have an mp4 video export.
        job_progress_uid (str or None, optional): the job progress uid to report to.
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.generate_onboarding_video_thumbnails",
            content_file_uid=content_file_uid,
            job_progress_uid=job_progress_uid,
        )

    async with success_or_failure_reporter(
        itgs, job_progress_uid=job_progress_uid, start_message="locating source file"
    ) as prog:
        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        response = await cursor.executeunified3(
            (
                (
                    "SELECT content_files.original_sha512 FROM content_files WHERE uid=?",
                    (content_file_uid,),
                ),
                (
                    """
SELECT
    onboarding_video_thumbnails.uid,
    image_files.uid,
    onboarding_video_thumbnails.source
FROM content_files, onboarding_video_thumbnails, image_files
WHERE
    content_files.uid=?
    AND json_extract(onboarding_video_thumbnails.source, '$.type') = 'frame'
    AND json_extract(onboarding_video_thumbnails.source, '$.video_sha512') = content_files.original_sha512
    AND onboarding_video_thumbnails.image_file_id = image_files.id
                    """,
                    (content_file_uid,),
                ),
                (
                    """
SELECT
    s3_files.key,
    s3_files.file_size
FROM content_files, s3_files
WHERE
    content_files.uid = ?
    AND s3_files.id = content_files.original_s3_file_id
                    """,
                    (content_file_uid,),
                ),
                (
                    """
SELECT
    content_file_exports.format_parameters,
    content_file_exports.quality_parameters,
    content_file_exports.bandwidth,
    s3_files.key,
    s3_files.file_size
FROM content_files, content_file_exports, content_file_export_parts, s3_files
WHERE
    content_files.uid = ?
    AND content_file_exports.content_file_id = content_files.id
    AND content_file_exports.format = 'mp4'
    AND content_file_export_parts.content_file_export_id = content_file_exports.id
    AND s3_files.id = content_file_export_parts.s3_file_id
    AND NOT EXISTS (
        SELECT 1 FROM content_file_exports AS cfe
        WHERE
            cfe.content_file_id = content_files.id
            AND cfe.format = 'mp4'
            AND (
                cfe.bandwidth > content_file_exports.bandwidth
                OR (
                    cfe.bandwidth = content_file_exports.bandwidth
                    AND cfe.uid < content_file_exports.uid
                )
            )
    )
                    """,
                    (content_file_uid,),
                ),
            )
        )

        exists_response = response.items[0]
        existing_thumbnails_response = response.items[1]
        original_file_response = response.items[2]
        mp4_export_response = response.items[3]

        if not exists_response.results:
            raise CustomFailureReasonException(f"{content_file_uid=} not found")

        if existing_thumbnails_response.results:
            raise CustomFailureReasonException(
                f"{content_file_uid=} already has generated thumbnails: {existing_thumbnails_response.results}"
            )

        original_sha512 = cast(str, exists_response.results[0][0])
        source_sha512: Optional[str] = None
        if original_file_response.results:
            source_sha512 = original_sha512
            source_key = cast(str, original_file_response.results[0][0])
            source_file_size = cast(int, original_file_response.results[0][1])
            source_name = "original file"
        elif mp4_export_response.results:
            export_format_parameters = mp4_export_response.results[0][0]
            export_quality_parameters = mp4_export_response.results[0][1]
            export_bandwidth = mp4_export_response.results[0][2]

            source_key = cast(str, mp4_export_response.results[0][3])
            source_file_size = cast(int, mp4_export_response.results[0][4])
            source_name = f"mp4 export {export_bandwidth}bps, {export_format_parameters=}, {export_quality_parameters=}"
        else:
            raise CustomFailureReasonException(
                f"{content_file_uid=} has no source file or usable export"
            )

        files = await itgs.files()
        with temp_file() as source_filepath, temp_dir() as thumbnail_dir:
            async with aiofiles.open(
                source_filepath, "wb"
            ) as raw_file, AsyncProgressTrackingWritableBytesIO(
                itgs,
                job_progress_uid=job_progress_uid,
                expected_file_size=source_file_size,
                delegate=raw_file,
                message=f"downloading {source_name}",
            ) as tracked_file:
                await files.download(
                    tracked_file,
                    bucket=files.default_bucket,
                    key=source_key,
                    sync=False,
                )

            real_file_size = os.path.getsize(source_filepath)
            async with aiofiles.open(
                source_filepath, "rb"
            ) as raw_file, AsyncProgressTrackingReadableBytesIO(
                itgs,
                job_progress_uid=job_progress_uid,
                expected_file_size=real_file_size,
                delegate=raw_file,
                message=f"hashing {source_name}",
            ) as tracked_file:
                real_sha512 = await hash_filelike(tracked_file)

            if source_sha512 is not None and real_sha512 != source_sha512:
                raise CustomFailureReasonException(
                    f"{source_name=} sha512 mismatch: {real_sha512=} != {source_sha512=}"
                )

            ffmpeg = shutil.which("ffmpeg")
            assert ffmpeg is not None, "ffmpeg not found"

            for frame_idx, frame in enumerate(THUMBNAIL_FRAMES):
                if gd.received_term_signal:
                    await bounce()
                    raise BouncedException()

                await prog.push_progress(
                    f"extracting frame {frame + 1}",
                    indicator={
                        "type": "bar",
                        "at": frame_idx,
                        "of": len(THUMBNAIL_FRAMES),
                    },
                )
                extract_frame(
                    ffmpeg,
                    source_file=source_filepath,
                    dest_file=os.path.join(thumbnail_dir, f"{frame}.png"),
                    frame=frame,
                )

            for frame_idx, frame in enumerate(THUMBNAIL_FRAMES):
                if gd.received_term_signal:
                    await bounce()
                    raise BouncedException()

                try:
                    image = await process_image(
                        os.path.join(thumbnail_dir, f"{frame}.png"),
                        TARGETS,
                        itgs=itgs,
                        gd=gd,
                        max_width=16384,
                        max_height=16384,
                        max_area=8192 * 8192,
                        max_file_size=1024 * 1024 * 512,
                        name_hint=f"onboarding_video_thumbnail__frame_{frame + 1}",
                        job_progress_uid=job_progress_uid,
                    )
                except ProcessImageAbortedException:
                    await bounce()
                    raise BouncedException()

                await prog.push_progress(
                    f"storing thumbnail extracted from frame {frame + 1}",
                    indicator={"type": "spinner"},
                )

                source = json.dumps(
                    {
                        "type": "frame",
                        "frame_number": frame + 1,
                        "video_sha512": original_sha512,
                        "via_sha512": real_sha512,
                    },
                    sort_keys=True,
                )
                await cursor.execute(
                    """
INSERT INTO onboarding_video_thumbnails (
    uid, image_file_id, source, last_uploaded_at
)
SELECT
    ?,
    image_files.id,
    ?,
    ?
FROM image_files
WHERE
    image_files.uid = ?
    AND NOT EXISTS (
        SELECT 1 FROM onboarding_video_thumbnails AS ovt
        WHERE
            ovt.image_file_id = image_files.id
            AND ovt.source = ?
    )
                    """,
                    (
                        f"oseh_ovt_{secrets.token_urlsafe(16)}",
                        source,
                        time.time(),
                        image.uid,
                        source,
                    ),
                )


if __name__ == "__main__":
    import asyncio

    async def main():
        content_file_uid = input("content file uid: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.generate_onboarding_video_thumbnails",
                content_file_uid=content_file_uid,
            )

    asyncio.run(main())
