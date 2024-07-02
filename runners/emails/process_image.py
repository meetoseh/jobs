"""Processes file uploads intended to be used as images in emails"""

import secrets
import time
from typing import Optional, TypedDict
from file_uploads import StitchFileAbortedException, stitch_file_upload
from images import ProcessImageAbortedException, ProcessImageSanity
from itgs import Itgs
from graceful_death import GracefulDeath

from jobs import JobCategory
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    CustomSuccessReasonException,
    success_or_failure_reporter,
)
from runners.screens.exact_dynamic_process_image import (
    get_targets,
    process_raster_image,
    process_svg_image,
)
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


class Size(TypedDict):
    width: int
    """The 1x logical width of the image in pixels"""

    height: int
    """The 1x logical height of the image in pixels"""


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: str,
    size: Size,
):
    """Processes a file upload intended to be used as an image in an email, where
    within the email it will have the given fixed width and height in CSS pixels

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        job_progress_uid (str): The uid of the job progress to update
        size (Size): The size of the image in the email
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.emails.process_image",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
            size=size,
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
                email_image_uid = await process_email_image(
                    itgs,
                    gd,
                    stitched_path=stitched_path,
                    uploaded_by_user_sub=uploaded_by_user_sub,
                    job_progress_uid=job_progress_uid,
                    size=size,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )

        raise CustomSuccessReasonException(f"{email_image_uid=}")


async def process_email_image(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    stitched_path: str,
    uploaded_by_user_sub: Optional[str],
    job_progress_uid: Optional[str],
    size: Size,
) -> Optional[str]:
    """Processes the image at the given filepath at the given size. This
    never needs to crop, and the resizing always exactly maintains the aspect
    ratio of the original image (which produces restrictions on the original image).

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): used for signalling when to stop early
        stitched_path (str): the local path to the stitched file
        uploaded_by_user_sub (str, None): the sub of the user who uploaded the file
        job_progress_uid (str): the uid of the job progress to update
        size (Size): The size of the image in CSS pixels
    """
    sanity: ProcessImageSanity = {
        "max_width": 8192,
        "max_height": 8192,
        "max_area": 4096 * 8192,
        "max_file_size": 1024 * 1024 * 512,
    }

    targets = await get_targets(
        stitched_path,
        itgs=itgs,
        gd=gd,
        job_progress_uid=job_progress_uid,
        **sanity,
        dynamic_size=size,
        pedantic=False,
    )
    if targets is None:
        return

    uploaded_at = time.time()

    if targets.type == "raster_resize":
        image = await process_raster_image(
            targets,
            itgs=itgs,
            gd=gd,
            job_progress_uid=job_progress_uid,
            uploaded_at=uploaded_at,
        )
    else:
        image = await process_svg_image(
            targets,
            itgs=itgs,
            gd=gd,
            job_progress_uid=job_progress_uid,
            uploaded_at=uploaded_at,
        )

    conn = await itgs.conn()
    cursor = conn.cursor()

    email_image_uid = f"oseh_eim_{secrets.token_urlsafe(16)}"
    await cursor.execute(
        """
INSERT INTO email_images (
    uid,
    image_file_id,
    width,
    height,
    created_at
)
SELECT
    ?,
    image_files.id,
    ?,
    ?,
    ?
FROM image_files
WHERE image_files.uid = ?
        """,
        (
            email_image_uid,
            size["width"],
            size["height"],
            time.time(),
            image.uid,
        ),
    )
    return email_image_uid
