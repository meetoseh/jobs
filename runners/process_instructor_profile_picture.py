"""Processes a raw image intended to be used as a instructor profile picture"""
import asyncio
import secrets
from error_middleware import handle_warning
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from images import process_image, ProcessImageAbortedException
from temp_files import temp_file
from runners.check_profile_picture import TARGETS


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    instructor_uid: str,
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
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_instructor_profile_picture",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            instructor_uid=instructor_uid,
        )

    with temp_file() as stitched_path:
        try:
            await stitch_file_upload(file_upload_uid, stitched_path, itgs=itgs, gd=gd)
        except StitchFileAbortedException:
            return await bounce()

        try:
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
            )
        except ProcessImageAbortedException:
            return await bounce()

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
        asyncio.ensure_future(
            handle_warning(
                f"{__name__}:no_rows_affected",
                f"No instructors rows affected when setting {instructor_uid=} to {image.uid=}",
            )
        )

    jobs = await itgs.jobs()
    await jobs.enqueue("runners.delete_file_upload", file_upload_uid=file_upload_uid)
