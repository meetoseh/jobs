"""Processes a raw image intended to be used as a journey background image"""
import os
import secrets
import time
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from images import process_image, ImageTarget, ProcessImageAbortedException
from temp_files import temp_file
from PIL import Image, ImageFilter, ImageEnhance
import threading
import asyncio
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST

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
    # MOBILE 2X
    (720, 1600),
    (828, 1792),
    (720, 1280),
    (780, 1688),
    (824, 1830),
    (720, 1560),
    (750, 1624),
    (750, 1334),
    (720, 1520),
    (786, 1702),
    (786, 1746),
    (824, 1784),
    (856, 1852),
    # MOBILE 3X; chose a few key ones
    (1080, 2400),
    (1242, 2745),
    # FROM TESTING WITH STATUS BARS
    (360, 736),
    (1472, 720),
    # DESKTOP
    (1920, 1080),
    (1366, 768),
    (1536, 864),
    # DESKTOP SELECT SCREEN
    (198, 334),  # X-SMALL 1x
    (264, 446),  # NORMAL 1x
    (396, 668),  # X-SMALL 2x
    (528, 892),  # NORMAL 2x
    (594, 1002),  # X-SMALL 3x
    (792, 1338),  # NORMAL 3x
    # SHARE TO INSTAGRAM
    (270, 470),  # PREVIEW 1x
    (540, 940),  # PREVIEW 2x
    (1080, 1920),
    # ADMIN PREVIEW
    (180, 368),  # preview mobile (2x already handled)
    (270, 480),  # preview desktop
    (540, 960),  # preview desktop 2x
    (480, 270),  # preview share to instagram
    (960, 540),  # preview share to instagram 2x
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
            "runners.process_journey_background_image",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
        )

    with temp_file() as stitched_path, temp_file() as blurred_path:
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
                max_width=16384,
                max_height=16384,
                max_area=8192 * 8192,
                max_file_size=1024 * 1024 * 512,
                name_hint="journey_background_image",
            )
        except ProcessImageAbortedException:
            return await bounce()

        # by blurring second we know the image meets requirements
        try:
            await blur_image(stitched_path, blurred_path)
        except ProcessImageAbortedException:
            return await bounce()

        try:
            blurred_image = await process_image(
                blurred_path,
                TARGETS,
                itgs=itgs,
                gd=gd,
                max_width=16384,
                max_height=16384,
                max_area=8192 * 8192,
                max_file_size=1024 * 1024 * 512,
                name_hint="blurred_journey_background_image",
            )
        except ProcessImageAbortedException:
            return await bounce()

    conn = await itgs.conn()
    cursor = conn.cursor()

    jbi_uid = f"oseh_jbi_{secrets.token_urlsafe(16)}"
    last_uploaded_at = time.time()
    await cursor.execute(
        """
        INSERT INTO journey_background_images (
            uid,
            image_file_id,
            blurred_image_file_id,
            uploaded_by_user_id,
            last_uploaded_at
        )
        SELECT
            ?,
            image_files.id,
            blurred_image_files.id,
            users.id,
            ?
        FROM image_files
        JOIN image_files AS blurred_image_files ON blurred_image_files.uid = ?
        LEFT OUTER JOIN users ON users.sub = ?
        WHERE
            image_files.uid = ?
        ON CONFLICT (image_file_id)
        DO UPDATE SET last_uploaded_at = ?
        ON CONFLICT (blurred_image_file_id)
        DO UPDATE SET last_uploaded_at = ?
        """,
        (
            jbi_uid,
            last_uploaded_at,
            blurred_image.uid,
            uploaded_by_user_sub,
            image.uid,
            last_uploaded_at,
            last_uploaded_at,
        ),
    )

    jobs = await itgs.jobs()
    await jobs.enqueue("runners.delete_file_upload", file_upload_uid=file_upload_uid)


async def blur_image(source_path: str, dest_path: str):
    """Blurs and darkens the image at the given path, as intended for blurred journey
    background images. The description of this blur is canonically at journeys.md
    in the backend database docs.

    Args:
        source_path (str): the path to the source image
        dest_path (str): the path to the destination image

    Raises:
        ProcessImageAbortedException: if a term signal was received while blurring the
            image, or we otherwise would like to retry the job on a different instance
    """
    loop = asyncio.get_running_loop()
    event = asyncio.Event()
    bknd_thread = threading.Thread(
        target=blur_image_blocking,
        kwargs={
            "source_path": source_path,
            "dest_path": dest_path,
            "loop": loop,
            "event": event,
        },
        daemon=True,
    )
    bknd_thread.start()

    await event.wait()
    if not os.path.exists(dest_path):
        raise Exception(f"blur image failed to produce output at {dest_path}")


def blur_image_blocking(
    source_path: str,
    dest_path: str,
    loop: asyncio.AbstractEventLoop,
    event: asyncio.Event,
):
    """Blurs and darkens the image at the given path, as intended for blurred journey
    background images. The description of this blur is canonically at journeys.md
    in the backend database docs.

    This is the synchronous version, which should be run on a different thread or process
    to allow weaving jobs

    Args:
        source_path (str): the path to the source image
        dest_path (str): the path to the destination image
        loop (asyncio.AbstractEventLoop): the event loop to use for setting the event
        event (asyncio.Event): the event to set when the image is done processing
    """
    try:
        im = Image.open(source_path)
        blur_radius = max(0.09 * min(im.width, im.height), 12)

        blurred_image = im.filter(ImageFilter.GaussianBlur(blur_radius))
        darkened_image = ImageEnhance.Brightness(blurred_image).enhance(0.7)
        darkened_image.save(
            dest_path, format="webp", lossless=True, quality=100, method=6
        )
    finally:
        loop.call_soon_threadsafe(event.set)
