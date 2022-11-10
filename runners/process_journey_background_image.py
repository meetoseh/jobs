"""Processes a raw image intended to be used as a journey background image"""
import secrets
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from images import process_image, ImageTarget, ProcessImageAbortedException
from temp_files import temp_file
import time
import os

RESOLUTIONS = [
    # MOBILE
    (360, 800),
    (414, 896),
    (360, 640),
    (390, 844),
    (412, 915),
    (360, 780),
    (375, 812),
    (375, 667),
    (360, 760),
    (393, 851),
    (393, 873),
    (412, 892),
    (428, 926),
    # DESKTOP
    (1920, 1080),
    (1366, 768),
    (1536, 864),
]


def get_jpg_settings(width: int, height: int) -> dict:
    if width * height < 500 * 500:
        return {"quality": 95, "optimize": True, "progressive": True}

    if width * height < 750 * 750:
        return {"quality": 90, "optimize": True, "progressive": True}

    return {"quality": 85, "optimize": True, "progressive": True}


def get_webp_settings(width: int, height: int) -> dict:
    if width * height < 500 * 500:
        return {"lossless": True, "quality": 100, "method": 6}

    if width * height < 750 * 750:
        return {"lossless": False, "quality": 95, "method": 4}

    return {"lossless": False, "quality": 90, "method": 4}


TARGETS = [
    *(
        ImageTarget(
            required=True,
            width=w,
            height=h,
            format="jpeg",
            quality_settings=get_jpg_settings(w, h),
        )
        for w, h in RESOLUTIONS
    ),
    *(
        ImageTarget(
            required=False,
            width=w,
            height=h,
            format="webp",
            quality_settings=get_webp_settings(w, h),
        )
        for w, h in RESOLUTIONS
    ),
]


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, file_upload_uid: str, uploaded_by_user_sub: str
):
    """Processes the s3 file upload with the given uid as a journey background image.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_journey_background_image", file_upload_uid=file_upload_uid
        )

    async with temp_file() as stitched_path:
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
                name_hint="profile_picture",
            )
        except ProcessImageAbortedException:
            return await bounce()

    conn = await itgs.conn()
    cursor = conn.cursor()

    jbi_uid = f"oseh_jbi_{secrets.token_urlsafe(16)}"
    await cursor.execute(
        """
        INSERT INTO journey_background_images (
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
        """,
        (
            jbi_uid,
            uploaded_by_user_sub,
            image.uid,
        ),
    )

    jobs = await itgs.jobs()
    await jobs.enqueue("runners.delete_file_upload", file_upload_uid=file_upload_uid)
