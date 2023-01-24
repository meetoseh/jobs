"""Deletes the image file with the given uid"""
from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
from error_middleware import handle_warning
from images import get_image_file


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, uid: str, job_uid: Optional[str] = None
):
    """Deletes the image file with the given uid, but only if it's not in use

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        uid (str): The uid of the image file to delete
        job_uid (str): If specified, we send a message to ps:job:{job_uid} when done
    """
    image_file = await get_image_file(itgs, uid)
    if image_file is None:
        await handle_warning(f"{__name__}:image_file_not_found", f"{uid=} not found")
        return

    conn = await itgs.conn()
    cursor = conn.cursor()

    response = await cursor.execute(
        """
        DELETE FROM image_files
        WHERE
            image_files.uid = ?
            AND NOT EXISTS (
                SELECT 1 FROM users
                WHERE users.picture_image_file_id = image_files.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM journeys
                WHERE journeys.background_image_file_id = image_files.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM journeys
                WHERE journeys.blurred_background_image_file_id = image_files.id
            )
        """,
        (uid,),
    )
    if response.rows_affected is None or response.rows_affected < 1:
        await handle_warning(f"{__name__}:image_file_in_use", f"{uid=} is in use")
        return

    files = await itgs.files()
    s3_files_to_delete = [
        image_file.original_s3_file,
        *(export.s3_file for export in image_file.exports),
    ]

    for s3_file in s3_files_to_delete:
        await files.delete(bucket=s3_file.bucket, key=s3_file.key)
        await cursor.execute("DELETE FROM s3_files WHERE uid=?", (s3_file.uid,))

    if job_uid is not None:
        redis = await itgs.redis()
        await redis.publish(f"ps:job:{job_uid}", "done")
