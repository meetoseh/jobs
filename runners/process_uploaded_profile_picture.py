"""Processes a raw image intended to be used as a users profile picture"""

import json
import logging
import secrets
import time
from error_middleware import handle_warning
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from images import process_image, ProcessImageAbortedException
from temp_files import temp_file
from runners.check_profile_picture import TARGETS
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    user_sub: str,
):
    """Processes the s3 file upload with the given uid as a profile picture and, if
    successful, associates it with the user with the given sub.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        user_sub (str): The sub of the user this will be a profile picture for
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_uploaded_profile_picture",
            file_upload_uid=file_upload_uid,
            user_sub=user_sub,
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
                max_width=2048,
                max_height=2048,
                max_area=2048 * 2048,
                max_file_size=1024 * 1024 * 128,
                name_hint="uploaded_profile_picture",
            )
        except ProcessImageAbortedException:
            return await bounce()

    conn = await itgs.conn()
    cursor = conn.cursor()

    now = time.time()
    new_upp_uid = f"oseh_upp_{secrets.token_urlsafe(16)}"
    response = await cursor.executemany3(
        (
            (
                """
                    INSERT INTO user_profile_pictures (
                        uid, user_id, latest, image_file_id, source, created_at, last_processed_at
                    )
                    SELECT
                        ?, users.id, 0, image_files.id, ?, ?, ?
                    FROM users, image_files
                    WHERE
                        users.sub = ?
                        AND image_files.uid = ?
                        AND NOT EXISTS (
                            SELECT 1 FROM user_profile_pictures AS upp
                            WHERE 
                                upp.image_file_id = image_files.id
                                AND upp.user_id = users.id
                        )
                    """,
                (
                    new_upp_uid,
                    json.dumps({"src": "upload", "uploaded_at": now}),
                    now,
                    now,
                    user_sub,
                    image.uid,
                ),
            ),
            (
                """
                    UPDATE user_profile_pictures
                    SET latest=0
                    WHERE
                        EXISTS (
                            SELECT 1 FROM users
                            WHERE
                                users.id = user_profile_pictures.user_id
                                AND users.sub = ?
                        )
                        AND latest=1
                        AND EXISTS (
                            SELECT 1 FROM user_profile_pictures AS upp
                            WHERE upp.uid = ?
                        )
                    """,
                (user_sub, new_upp_uid),
            ),
            (
                """
                    UPDATE user_profile_pictures
                    SET latest=1
                    WHERE uid=?
                    """,
                (new_upp_uid,),
            ),
        )
    )

    if response[0].rows_affected is None or response[0].rows_affected < 1:
        await handle_warning(
            f"{__name__}:update_failed",
            f"Failed to insert uploaded profile picture for {user_sub} ({image.uid=})",
        )
        return

    logging.info(
        f"Updated {user_sub=} profile picture to {image.uid=} (via direct upload)"
    )
