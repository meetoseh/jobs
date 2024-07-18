"""Success handler for home screen images"""

from decimal import Decimal
import math
import secrets
import time
from typing import Optional
from file_uploads import StitchFileAbortedException, stitch_file_upload
from images import (
    ProcessImageAbortedException,
    process_image,
)
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
from lib.devices.ios_device_size_utils import get_sizes_for_devices_newer_than
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from runners.process_journey_background_image import darken_image, make_standard_targets
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


def _get_small_img_height(logical_height: int) -> int:
    return 258 + max(min(logical_height - 633, 92), 0)


RESOLUTIONS = list(
    dict.fromkeys(
        [
            # SMALL IMAGE SIZE (original home screen)
            # GENERIC
            # Mobile Small
            (360, 258),  # 1x
            (720, 516),  # 2x
            (1080, 774),  # 3x
            # Mobile, Medium
            (390, 304),  # 1x
            (780, 608),  # 2x
            (1170, 912),  # 3x
            # Mobile, Large
            (440, 350),  # 1x
            (880, 700),  # 2x
            (1320, 1050),  # 3x
            # IOS - EXACT - 2019 and newer
            *get_sizes_for_devices_newer_than(
                "2019-01-01",
                mapper=lambda lw, lh, pr: (
                    math.ceil(lw * pr),
                    math.ceil(_get_small_img_height(lh) * pr),
                ),
                exclude_families={"Apple Watch", "iPod touch"},
            ),
            # Desktop
            (1440, 350),  # 1x
            (2880, 700),  # 2x
            (1920, 350),  # 1x
            (3840, 700),  # 2x
            # LARGE IMAGE SIZE (new home screen; full height)
            # GENERIC
            # Mobile Small
            (360, 633),  # 1x
            (720, 1266),  # 2x
            (1080, 1899),  # 3x
            # Mobile, Medium
            (390, 844),  # 1x
            (780, 1688),  # 2x
            (1170, 2532),  # 3x
            # Mobile, Large
            (414, 915),  # 1x
            (828, 1830),  # 2x
            (1242, 2745),  # 3x
            # IOS - EXACT - 2019 and newer
            *get_sizes_for_devices_newer_than(
                "2019-01-01",
                mapper=lambda lw, lh, pr: (
                    math.ceil(lw * pr),
                    math.ceil(lh * pr),
                ),
                exclude_families={"Apple Watch", "iPod touch"},
            ),
            # Desktop
            (1280, 1024),  # 1x
            (2560, 2048),  # 2x
            (1600, 900),  # 1x
            (3200, 1800),  # 2x
            (1920, 1080),  # 1x
            (3840, 2160),  # 2x
        ]
    )
)

TARGETS = make_standard_targets(RESOLUTIONS)
for target in TARGETS:
    if target.required and target.width > 1920:
        target.required = False

LAST_TARGETS_CHANGED_AT = 1721250600


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: Optional[str] = None,
):
    """Processes the s3 file upload with the given uid as a course background image.

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
            "runners.process_home_screen_image",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
        )

    async with success_or_failure_reporter(
        itgs, job_progress_uid=job_progress_uid
    ) as prog:
        with temp_file() as stitched_path, temp_file() as darkened_path:
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
                    name_hint="home_screen_image",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

            await prog.push_progress("darkening image", indicator={"type": "spinner"})
            try:
                await darken_image(
                    stitched_path, darkened_path, strength=Decimal("0.3")
                )
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
                    name_hint="darkened_home_screen_image",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        conn = await itgs.conn()
        cursor = conn.cursor()

        await prog.push_progress("finishing up", indicator={"type": "spinner"})

        hsi_uid = f"oseh_hsi_{secrets.token_urlsafe(16)}"
        now = time.time()
        await cursor.execute(
            """
INSERT INTO home_screen_images (
    uid, 
    image_file_id, 
    darkened_image_file_id, 
    start_time, end_time, 
    flags, 
    dates, 
    created_at, 
    live_at,
    last_processed_at
)
SELECT
    ?, image_files.id, darkened_image_files.id, 0, 86400, 2097152, NULL, ?, ?, ?
FROM image_files, image_files AS darkened_image_files
WHERE
    image_files.uid = ?
    AND darkened_image_files.uid = ?
    AND NOT EXISTS (
        SELECT 1 FROM home_screen_images AS hsi WHERE hsi.image_file_id = image_files.id
    )
            """,
            (hsi_uid, now, now, now, image.uid, darkened_image.uid),
        )

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )
