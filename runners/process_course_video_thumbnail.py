"""Success handler for course video thumbnail images"""

import json
import secrets
import time
from typing import Optional
from file_uploads import StitchFileAbortedException, stitch_file_upload
from images import ProcessImageAbortedException, process_image
from itgs import Itgs
from graceful_death import GracefulDeath

from jobs import JobCategory
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from runners.generate_course_video_thumbnails import TARGETS
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: Optional[str] = None,
):
    """Processes the s3 file upload with the given uid as a course video thumbnail image.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        job_progress_uid (str, None): The uid of the job progress to update
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_course_video_thumbnail",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
        )

    async with success_or_failure_reporter(
        itgs, job_progress_uid=job_progress_uid
    ) as prog:
        with temp_file() as stitched_path:
            try:
                await stitch_file_upload(
                    file_upload_uid,
                    stitched_path,
                    itgs=itgs,
                    gd=gd,
                    job_progress_uid=job_progress_uid,
                )
            except StitchFileAbortedException:
                await bounce()
                raise BouncedException()

            try:
                image = await process_image(
                    stitched_path,
                    TARGETS,
                    itgs=itgs,
                    gd=gd,
                    max_width=16384,
                    max_height=16384,
                    max_area=8192 * 8192,
                    max_file_size=1024 * 1024 * 512,
                    name_hint=f"course_video_thumbnail__user",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        conn = await itgs.conn()
        cursor = conn.cursor()

        await prog.push_progress("finishing up", indicator={"type": "spinner"})

        cvt_uid = f"oseh_cvt_{secrets.token_urlsafe(16)}"
        last_uploaded_at = time.time()
        await cursor.execute(
            """
            INSERT INTO course_video_thumbnail_images (
                uid,
                image_file_id,
                source,
                last_uploaded_at
            )
            SELECT
                ?,
                image_files.id,
                ?,
                ?
            FROM image_files
            WHERE
                image_files.uid = ?
            ON CONFLICT(image_file_id)
            DO UPDATE SET last_uploaded_at = ?
            """,
            (
                cvt_uid,
                json.dumps(
                    {"type": "user", "sub": uploaded_by_user_sub}, sort_keys=True
                ),
                last_uploaded_at,
                image.uid,
                last_uploaded_at,
            ),
        )

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )
