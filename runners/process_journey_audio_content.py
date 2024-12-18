"""Processes a raw audio/video file intended to be used as a journey audio content"""

import secrets
from audio import ProcessAudioAbortedException, process_audio
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from lib.progressutils.success_or_failure_reporter import success_or_failure_reporter
from temp_files import temp_file
import time
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: str,
):
    """Processes the s3 file upload with the given uid as journey audio content.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        job_progress_uid (str): The uid of the job progress to update
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_journey_audio_content",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
        )

    async with success_or_failure_reporter(itgs, job_progress_uid=job_progress_uid):
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
                return await bounce()

            try:
                content = await process_audio(
                    stitched_path,
                    itgs=itgs,
                    gd=gd,
                    max_file_size=1024 * 1024 * 1024,
                    name_hint="journey_audio_content",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessAudioAbortedException:
                return await bounce()

        conn = await itgs.conn()
        cursor = conn.cursor()

        jac_uid = f"oseh_jac_{secrets.token_urlsafe(16)}"
        last_uploaded_at = time.time()
        await cursor.execute(
            """
            INSERT INTO journey_audio_contents (
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
                jac_uid,
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
