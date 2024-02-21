"""Success handler for course logo images"""

from decimal import Decimal
from fractions import Fraction
import os
import secrets
import time
from typing import List, Optional, Sequence, Tuple
from file_uploads import StitchFileAbortedException, stitch_file_upload
from images import (
    ImageTarget,
    ProcessImageAbortedException,
    SvgAspectRatio,
    get_svg_natural_aspect_ratio,
    process_image,
)
from itgs import Itgs
from graceful_death import GracefulDeath
from PIL import Image
from jobs import JobCategory
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    CustomFailureReasonException,
    success_or_failure_reporter,
)
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST

TARGET_WIDTHS: Sequence[int] = (
    310,  # 1x
    620,  # 2x
    930,  # 3x
)


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: Optional[str] = None,
):
    """Processes the s3 file upload with the given uid as a course logo image. Unlike
    with a typical image which is cropped to specific resolutions, this will preserve
    the natural aspect ratio of the provided image.

    SVGs are preferred for course logos as they typically involve text, and text requires
    subpixel rendering to be perfectly crisp. However, this will accept rasterized images
    that are sufficiently large, and will serve rasterized images to clients that don't
    support SVGs.

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
            "runners.process_course_logo",
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

            if os.path.getsize(stitched_path) > 1024 * 1024 * 512:
                jobs = await itgs.jobs()
                await jobs.enqueue(
                    "runners.delete_file_upload", file_upload_uid=file_upload_uid
                )
                raise CustomFailureReasonException("image file size too large")

            await prog.push_progress(
                "determining exports based on image natural aspect ratio",
                indicator={"type": "spinner"},
            )

            aspect_ratio_info = await get_svg_natural_aspect_ratio(stitched_path)
            if aspect_ratio_info is None:
                try:
                    im = Image.open(stitched_path)
                    aspect_ratio_info = SvgAspectRatio(
                        ratio=im.width / im.height,
                        width=im.width,
                        height=im.height,
                        width_exact=str(im.width),
                        height_exact=str(im.height),
                    )
                except Exception:
                    jobs = await itgs.jobs()
                    await jobs.enqueue(
                        "runners.delete_file_upload", file_upload_uid=file_upload_uid
                    )
                    raise CustomFailureReasonException("image file not recognized")

            try:
                aspect_ratio = Fraction.from_decimal(
                    Decimal(aspect_ratio_info.height_exact)
                ) / Fraction.from_decimal(Decimal(aspect_ratio_info.width_exact))
            except:
                jobs = await itgs.jobs()
                await jobs.enqueue(
                    "runners.delete_file_upload", file_upload_uid=file_upload_uid
                )
                raise CustomFailureReasonException("0 width image or invalid SVG")

            if aspect_ratio < 0.2 or aspect_ratio > 5:
                jobs = await itgs.jobs()
                await jobs.enqueue(
                    "runners.delete_file_upload", file_upload_uid=file_upload_uid
                )
                raise CustomFailureReasonException(
                    "image aspect ratio out of bounds (1:5 to 5:1)"
                )

            target_resolutions: List[Tuple[int, int]] = [
                (width, int(width * aspect_ratio)) for width in TARGET_WIDTHS
            ]

            targets = [
                *(
                    ImageTarget(
                        required=True,
                        width=w,
                        height=h,
                        format="png",
                        quality_settings={"optimize": True},
                    )
                    for (w, h) in target_resolutions
                ),
                *(
                    ImageTarget(
                        required=True,
                        width=w,
                        height=h,
                        format="webp",
                        quality_settings={
                            "lossless": True,
                            "quality": 100,
                            "method": 6,
                        },
                    )
                    for (w, h) in target_resolutions
                ),
            ]

            try:
                image = await process_image(
                    stitched_path,
                    targets,
                    itgs=itgs,
                    gd=gd,
                    max_width=16384,
                    max_height=16384,
                    max_area=8192 * 8192,
                    max_file_size=1024 * 1024 * 512,
                    name_hint="course_logo",
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        conn = await itgs.conn()
        cursor = conn.cursor()

        await prog.push_progress("finishing up", indicator={"type": "spinner"})

        cli_uid = f"oseh_cli_{secrets.token_urlsafe(16)}"
        last_uploaded_at = time.time()
        await cursor.execute(
            """
            INSERT INTO course_logo_images (
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
                cli_uid,
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
