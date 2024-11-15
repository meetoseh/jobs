"""Processes a raw image intended to be used as a journey background image"""

from decimal import Decimal
import os
import secrets
import time
from typing import List, Optional, Sequence, Tuple
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from images import process_image, ImageTarget, ProcessImageAbortedException
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from temp_files import temp_file
from PIL import Image, ImageFilter, ImageEnhance
import threading
import asyncio
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST

# When changing this, trigger runners.process_new_journey_background_image_targets
RESOLUTIONS = list(
    dict.fromkeys(
        [
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
            (2560, 1600),
            (1920, 1080),
            (1366, 768),
            (1536, 864),
            # DESKTOP SELECT SCREEN
            (300, 500),  # LARGE 1x
            (600, 1000),  # LARGE 2x
            (900, 1500),  # LARGE 3x
            (287, 500),  # SKINNY LARGE 1x
            (574, 1000),  # SKINNY LARGE 2x
            (861, 1500),  # SKINNY LARGE 3x
            (275, 457),  # MEDIUM 1x
            (550, 914),  # MEDIUM 2x
            (825, 1371),  # MEDIUM 3x
            (275, 419),  # SMALL 1x
            (550, 838),  # SMALL 2x
            (825, 1257),  # SMALL 3x
            (256, 500),  # ULTRA SKINNY LARGE 1x
            (512, 1000),  # ULTRA SKINNY LARGE 2x
            (768, 1500),  # ULTRA SKINNY LARGE 3x
            (256, 457),  # ULTRA SKINNY MEDIUM 1x
            (512, 914),  # ULTRA SKINNY MEDIUM 2x
            (768, 1371),  # ULTRA SKINNY MEDIUM 3x
            (256, 419),  # ULTRA SKINNY SMALL 1x
            (512, 838),  # ULTRA SKINNY SMALL 2x
            (768, 1257),  # ULTRA SKINNY SMALL 3x
            # SHARE TO INSTAGRAM
            (270, 470),  # PREVIEW 1x
            (540, 940),  # PREVIEW 2x
            (810, 1410),  # PREVIEW 3x
            (208, 357),  # PREVIEW X-SMALL 1x
            (416, 714),  # PREVIEW X-SMALL 2x
            (624, 1071),  # PREVIEW X-SMALL 3x
            (1080, 1920),
            # ADMIN PREVIEW
            (180, 368),  # preview mobile
            (360, 736),  # preview mobile 2x
            (270, 480),  # preview desktop
            (540, 960),  # preview desktop 2x
            (480, 270),  # preview share to instagram
            (960, 540),  # preview share to instagram 2x
            # ADMIN ICON
            (45, 90),  # icon 1x
            (90, 180),  # icon 2x
            (135, 270),  # icon 3x
            # ADMIN COMPACT JOURNEY
            (342, 72),  # compact journey, no feedback, 1x
            (684, 144),  # compact journey, no feedback 2x
            (1026, 216),  # compact journey, no feedback 3x
            (342, 90),  # compact journey, feedback, 1x
            (684, 180),  # compact journey, feedback 2x
            (1026, 270),  # compact journey, feedback 3x
            # FEEDBACK/SHARE SCREEN
            (342, 237),  # smallest 1x
            (342, 314),  # most common 1x
            (342, 390),  # largest 1x
            (684, 474),  # smallest 2x
            (684, 628),  # most common 2x
            (684, 780),  # largest 2x
            (1026, 711),  # smallest 3x
            (1026, 942),  # most common 3x
            (1026, 1170),  # largest 3x
            # SERIES CLASS LIST BACKGROUND
            (342, 150),  # 1x
            (684, 300),  # 2x
            (1026, 450),  # 3x
            # HISTORY BACKGROUND
            (342, 76),  # 1x
            (684, 152),  # 2x
            (1026, 228),  # 3x
        ]
    )
)


def get_png_settings(width: int, height: int) -> dict:
    return {"optimize": True}


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


def make_standard_targets(
    resolutions: Sequence[Tuple[int, int]], *, alpha: bool = False
) -> List[ImageTarget]:
    return [
        *(
            ImageTarget(
                required=True,
                width=w,
                height=h,
                format="jpeg" if not alpha else "png",
                quality_settings=(
                    get_jpg_settings(w, h) if not alpha else get_png_settings(w, h)
                ),
            )
            for w, h in resolutions
        ),
        *(
            ImageTarget(
                required=False,
                width=w,
                height=h,
                format="webp",
                quality_settings=get_webp_settings(w, h),
            )
            for w, h in resolutions
        ),
    ]


TARGETS = make_standard_targets(RESOLUTIONS)


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: Optional[str] = None,
):
    """Processes the s3 file upload with the given uid as a journey background image.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        job_progress_uid (str): the uid of the job progress to update
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_journey_background_image",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
        )

    async with success_or_failure_reporter(
        itgs, job_progress_uid=job_progress_uid
    ) as prog:
        with temp_file() as stitched_path, temp_file() as blurred_path, temp_file() as darkened_path:
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
                    name_hint="journey_background_image",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

            # by blurring second we know the image meets requirements
            await prog.push_progress("blurring image", indicator={"type": "spinner"})
            try:
                await blur_image(stitched_path, blurred_path)
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

            try:
                blurred_image = await process_image(
                    blurred_path,
                    TARGETS,
                    itgs=itgs,
                    gd=gd,
                    max_width=16384,
                    max_height=16384,
                    max_area=16384 * 16384,
                    max_file_size=1024 * 1024 * 512,
                    name_hint="blurred_journey_background_image",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

            await prog.push_progress("darkening image", indicator={"type": "spinner"})
            try:
                await darken_image(stitched_path, darkened_path)
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

            try:
                darkened_image = await process_image(
                    darkened_path,
                    TARGETS,
                    itgs=itgs,
                    gd=gd,
                    max_width=16384,
                    max_height=16384,
                    max_area=16384 * 16384,
                    max_file_size=1024 * 1024 * 512,
                    name_hint="darkened_journey_background_image",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

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
                darkened_image_file_id,
                uploaded_by_user_id,
                last_uploaded_at
            )
            SELECT
                ?,
                image_files.id,
                blurred_image_files.id,
                darkened_image_files.id,
                users.id,
                ?
            FROM image_files
            JOIN image_files AS blurred_image_files ON blurred_image_files.uid = ?
            JOIN image_files AS darkened_image_files ON darkened_image_files.uid = ?
            LEFT OUTER JOIN users ON users.sub = ?
            WHERE
                image_files.uid = ?
            ON CONFLICT (image_file_id)
            DO UPDATE SET last_uploaded_at = ?
            ON CONFLICT (blurred_image_file_id)
            DO UPDATE SET last_uploaded_at = ?
            ON CONFLICT (darkened_image_file_id)
            DO UPDATE SET last_uploaded_at = ?
            """,
            (
                jbi_uid,
                last_uploaded_at,
                blurred_image.uid,
                darkened_image.uid,
                uploaded_by_user_sub,
                image.uid,
                last_uploaded_at,
                last_uploaded_at,
                last_uploaded_at,
            ),
        )

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )


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


async def darken_image(
    source_path: str, dest_path: str, *, strength: Decimal = Decimal(0.3)
):
    """Darkens the image at the given path, as intended for darkened journey
    background images. The description of this blur is canonically at journeys.md
    in the backend database docs.

    Args:
        source_path (str): the path to the source image
        dest_path (str): the path to the destination image

    Raises:
        ProcessImageAbortedException: if a term signal was received while darkening the
            image, or we otherwise would like to retry the job on a different instance
    """
    loop = asyncio.get_running_loop()
    event = asyncio.Event()
    bknd_thread = threading.Thread(
        target=darken_image_blocking,
        kwargs={
            "source_path": source_path,
            "dest_path": dest_path,
            "loop": loop,
            "event": event,
            "strength": strength,
        },
        daemon=True,
    )
    bknd_thread.start()

    await event.wait()
    if not os.path.exists(dest_path):
        raise Exception(f"darken image failed to produce output at {dest_path}")


def darken_image_blocking(
    source_path: str,
    dest_path: str,
    loop: asyncio.AbstractEventLoop,
    event: asyncio.Event,
    strength: Decimal,
):
    """Darkens the image at the given path, as intended for darkened journey
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
        darkened_image = ImageEnhance.Brightness(im).enhance(
            float(Decimal(1) - strength)
        )
        darkened_image.save(
            dest_path, format="webp", lossless=True, quality=100, method=6
        )
    finally:
        loop.call_soon_threadsafe(event.set)
