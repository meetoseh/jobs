"""Success handler for course hero images"""

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
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from runners.process_journey_background_image import make_standard_targets
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


RESOLUTIONS = list(
    dict.fromkeys(
        [
            # MOBILE; full-width, square
            # 360: samsung galaxy series
            (360, 360),  # 1x
            (720, 720),  # 2x
            (1080, 1080),  # 3x
            # 375: iphone se
            (375, 375),  # 1x
            (750, 750),  # 2x
            (1125, 1125),  # 3x
            # 390: iphone 14, 13 pro, etc
            (390, 390),
            (780, 780),
            (1170, 1170),
            # 393: iphone 14 pro, 15 pro
            (393, 393),
            (786, 786),
            (1179, 1179),
            # 428: 14 plus, 13 pro max
            (428, 428),
            (856, 856),
            (1284, 1284),
            # 430: 14 pro max, 15 plus, 15 pro max
            (430, 430),
            (860, 860),
            (1290, 1290),
            # TABLET; 4:3, about 2/3 width
            # for 834 width, e.g., ipad pro 6th gen 11"
            (556, 417),  # 1x
            (1112, 834),  # 2x
            # for 1024 width, e.g, ipad pro 16th gen 12.9"
            (683, 513),  # 1x
            (1366, 1024),  # 2x
            # for 1366 width, e.g., ipad pro 6th gen 12.9" in landscape
            (911, 684),  # 1x
            (1822, 1368),  # 2x
            # DESKTOP; 4:3, about 2/3 width
            # for 1440 width, mac book pro 13"
            (960, 720),  # 1x
            (1920, 1440),  # 2x
            # for 1920 width, standard desktop
            (1280, 960),  # 1x
            (2560, 1920),  # 2x (aka 4k)
            # ADMIN PREVIEW AND THUMBHASH FOR COURSE PUBLIC PAGE (frontend-ssr-web)
            # square
            (180, 180),  # 1x
            (360, 360),  # 2x
            (540, 540),  # 3x
            # 4:3
            (180, 135),  # 1x
            (360, 270),  # 2x
            (540, 405),  # 3x
        ]
    )
)

TARGETS = make_standard_targets(RESOLUTIONS)


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: Optional[str] = None,
):
    """Processes the s3 file upload with the given uid as a course hero image.

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
            "runners.process_course_hero_image",
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
                image = await process_image(
                    stitched_path,
                    TARGETS,
                    itgs=itgs,
                    gd=gd,
                    max_width=16384,
                    max_height=16384,
                    max_area=8192 * 8192,
                    max_file_size=1024 * 1024 * 512,
                    name_hint="course_hero_image",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        conn = await itgs.conn()
        cursor = conn.cursor()

        await prog.push_progress("finishing up", indicator={"type": "spinner"})

        chi_uid = f"oseh_chi_{secrets.token_urlsafe(16)}"
        last_uploaded_at = time.time()
        await cursor.execute(
            """
            INSERT INTO course_hero_images (
                uid,
                image_file_id,
                uploaded_by_user_id,
                last_uploaded_at
            )
            SELECT
                ?,
                image_files.id,
                users.id,
                ?
            FROM image_files
            LEFT OUTER JOIN users ON users.sub = ?
            WHERE
                image_files.uid = ?
            ON CONFLICT(image_file_id)
            DO UPDATE SET last_uploaded_at = ?
            """,
            (
                chi_uid,
                last_uploaded_at,
                uploaded_by_user_sub,
                image.uid,
                last_uploaded_at,
            ),
        )

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )
