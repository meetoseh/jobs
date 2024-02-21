"""Success handler for course background images"""

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
from runners.process_journey_background_image import darken_image, make_standard_targets
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


RESOLUTIONS = list(
    dict.fromkeys(
        [
            # Admin preview
            (180, 225),  # 1x
            (360, 450),  # 2x
            (540, 675),  # 3x
            # Mobile
            (342, 427),  # 1x
            (684, 854),  # 2x
            (1026, 1281),  # 3x
            # Desktop
            (382, 539),  # 1x
            (764, 1078),  # 2x
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
            "runners.process_course_background_image",
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
                    name_hint="course_background_image",
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
                    name_hint="darkened_course_background_image",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        conn = await itgs.conn()
        cursor = conn.cursor()

        await prog.push_progress("finishing up", indicator={"type": "spinner"})

        chi_uid = f"oseh_cbi_{secrets.token_urlsafe(16)}"
        last_uploaded_at = time.time()
        await cursor.executemany3(
            (
                (
                    """
                    UPDATE course_background_images
                    SET
                        darkened_image_file_id = darkened_image_files.id,
                        last_uploaded_at = ?
                    FROM image_files AS original_image_files, image_files AS darkened_image_files
                    WHERE
                        original_image_files.uid = ?
                        AND darkened_image_files.uid = ?
                        AND course_background_images.original_image_file_id = original_image_files.id
                    """,
                    (
                        last_uploaded_at,
                        image.uid,
                        darkened_image.uid,
                    ),
                ),
                (
                    """
                    INSERT INTO course_background_images (
                        uid,
                        original_image_file_id,
                        darkened_image_file_id,
                        uploaded_by_user_id,
                        last_uploaded_at
                    )
                    SELECT
                        ?,
                        original_image_files.id,
                        darkened_image_files.id,
                        users.id,
                        ?
                    FROM image_files AS original_image_files, image_files AS darkened_image_files
                    LEFT OUTER JOIN users ON users.sub = ?
                    WHERE
                        original_image_files.uid = ?
                        AND darkened_image_files.uid = ?
                        AND NOT EXISTS (
                            SELECT 1 FROM course_background_images
                            WHERE
                                original_image_file_id = original_image_files.id
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM course_background_images
                            WHERE
                                darkened_image_file_id = darkened_image_files.id
                        )
                    """,
                    (
                        chi_uid,
                        last_uploaded_at,
                        uploaded_by_user_sub,
                        image.uid,
                        darkened_image.uid,
                    ),
                ),
            )
        )

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )
