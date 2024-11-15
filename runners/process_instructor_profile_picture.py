"""Processes a raw image intended to be used as a instructor profile picture"""

import secrets
from typing import Optional
from error_middleware import handle_warning
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from images import process_image, ProcessImageAbortedException
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from temp_files import temp_file
from runners.check_profile_picture import TARGETS
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    instructor_uid: str,
    job_progress_uid: str,
):
    """Processes the s3 file upload with the given uid as an instructor profile
    picture and, if successful, associates it with the instructor with the given
    uid.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        instructor_uid (str): The uid of the instructor to associate the image with
        job_progress_uid (str): The uid of the job progress to update
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_instructor_profile_picture",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            instructor_uid=instructor_uid,
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
                await bounce()
                raise BouncedException()

            try:
                await process_instructor_profile_picture(
                    itgs,
                    gd,
                    stitched_path=stitched_path,
                    instructor_uid=instructor_uid,
                    uploaded_by_user_sub=uploaded_by_user_sub,
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )


async def process_instructor_profile_picture(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    stitched_path: str,
    uploaded_by_user_sub: Optional[str],
    instructor_uid: str,
    job_progress_uid: Optional[str],
) -> None:
    """Processes the instructor profile picture available at the given local path,
    uploading it for the instructor with the given uid.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): used for signalling when to stop early
        stitched_path (str): the local path to the stitched file
        uploaded_by_user_sub (str, None): the sub of the user who uploaded the file
        instructor_uid (str): the uid of the instructor to associate the image with
        job_progress_uid (str): the uid of the job progress to update
    """
    image = await process_image(
        stitched_path,
        TARGETS,
        itgs=itgs,
        gd=gd,
        max_width=8192,
        max_height=8192,
        max_area=4096 * 8192,
        max_file_size=1024 * 1024 * 512,
        name_hint="process_instructor_profile_picture",
        job_progress_uid=job_progress_uid,
    )

    conn = await itgs.conn()
    cursor = conn.cursor()

    ipp_uid = f"oseh_ipp_{secrets.token_urlsafe(16)}"
    response = await cursor.executemany3(
        (
            (
                """
                INSERT INTO instructor_profile_pictures (
                    uid,
                    image_file_id,
                    uploaded_by_user_id
                )
                SELECT
                    ?,
                    image_files.id,
                    users.id
                FROM image_files
                LEFT OUTER JOIN users ON users.sub = ?
                WHERE
                    image_files.uid = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM instructor_profile_pictures
                        WHERE instructor_profile_pictures.image_file_id = image_files.id
                    )
                """,
                (
                    ipp_uid,
                    uploaded_by_user_sub,
                    image.uid,
                ),
            ),
            (
                """
                UPDATE instructors
                SET picture_image_file_id = image_files.id
                FROM image_files
                WHERE
                    instructors.uid = ?
                    AND image_files.uid = ?
                    AND instructors.deleted_at IS NULL
                """,
                (
                    instructor_uid,
                    image.uid,
                ),
            ),
        )
    )

    if response[1].rows_affected is None or response[1].rows_affected < 1:
        await handle_warning(
            f"{__name__}:no_rows_affected",
            f"No instructors rows affected when setting {instructor_uid=} to {image.uid=}",
        )
