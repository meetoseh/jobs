"""Processes a raw video file intended to be used as a course intro video"""

import secrets
from typing import Optional
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from temp_files import temp_file
import time
from jobs import JobCategory
from videos import (
    DESKTOP_LANDSCAPE,
    INSTAGRAM_VERTICAL,
    ADMIN_MOBILE_PREVIEW_1X,
    MOBILE_PORTRAIT_1_5X,
    MOBILE_PORTRAIT_1X,
    MOBILE_PORTRAIT_2X,
    ProcessVideoAbortedException,
    process_video,
)

EXPORTS = [
    *INSTAGRAM_VERTICAL,
    *DESKTOP_LANDSCAPE,
    *ADMIN_MOBILE_PREVIEW_1X,
    *MOBILE_PORTRAIT_1X,
    *MOBILE_PORTRAIT_1_5X,
    *MOBILE_PORTRAIT_2X,
]

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: Optional[str] = None,
):
    """Processes the s3 file upload with the given uid as a course intro video.

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
            "runners.process_course_video",
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
                content = await process_video(
                    stitched_path,
                    itgs=itgs,
                    gd=gd,
                    max_file_size=4 * 1024 * 1024 * 1024,
                    name_hint="course_video",
                    exports=EXPORTS,
                    job_progress_uid=job_progress_uid,
                    min_width=1920,
                    min_height=1080,
                )
            except ProcessVideoAbortedException:
                await bounce()
                raise BouncedException()

        conn = await itgs.conn()
        cursor = conn.cursor()

        await prog.push_progress("finishing up", indicator={"type": "spinner"})

        cv_uid = f"oseh_cv_{secrets.token_urlsafe(16)}"
        last_uploaded_at = time.time()
        await cursor.execute(
            """
            INSERT INTO course_videos (
                uid,
                content_file_id,
                uploaded_by_user_id,
                last_uploaded_at
            )
            SELECT
                ?,
                content_files.id,
                users.id,
                ?
            FROM content_files
            LEFT OUTER JOIN users ON users.sub = ?
            WHERE
                content_files.uid = ?
            ON CONFLICT(content_file_id)
            DO UPDATE SET last_uploaded_at = ?
            """,
            (
                cv_uid,
                last_uploaded_at,
                uploaded_by_user_sub,
                content.uid,
                last_uploaded_at,
            ),
        )

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )
        await prog.push_progress(
            "queueing follow-up jobs",
            indicator={"type": "bar", "at": 1, "of": 3},
        )

        if job_progress_uid is not None:
            thumbnails_job_progress_uid = f"oseh_jp_{secrets.token_urlsafe(16)}"
            await jobs.enqueue_with_progress(
                "runners.generate_course_video_thumbnails",
                thumbnails_job_progress_uid,
                content_file_uid=content.uid,
                job_progress_uid=thumbnails_job_progress_uid,
            )
            await prog.push_progress(
                "queueing follow-up jobs",
                type="spawned",
                indicator={"type": "bar", "at": 2, "of": 3},
                spawned={
                    "name": "generate thumbnails",
                    "uid": thumbnails_job_progress_uid,
                },
            )
        else:
            await jobs.enqueue(
                "runners.generate_course_video_thumbnails",
                content_file_uid=content.uid,
            )

        if job_progress_uid is not None:
            transcript_job_progress_uid = f"oseh_jp_{secrets.token_urlsafe(16)}"
            await jobs.enqueue_with_progress(
                "runners.generate_course_transcript",
                transcript_job_progress_uid,
                content_file_uid=content.uid,
                job_progress_uid=transcript_job_progress_uid,
            )
            await prog.push_progress(
                "queueing follow-up jobs",
                type="spawned",
                indicator={"type": "bar", "at": 3, "of": 3},
                spawned={
                    "name": "generate transcript",
                    "uid": transcript_job_progress_uid,
                },
            )
        else:
            await jobs.enqueue(
                "runners.generate_course_transcript", content_file_uid=content.uid
            )
