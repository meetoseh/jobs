"""Processes a raw image intended to be used for an upgrade screen"""

from fractions import Fraction
import math
import secrets
import time
from typing import Optional, Tuple
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from images import (
    ProcessImageSanity,
    peek_if_alpha_required,
    process_image,
    ProcessImageAbortedException,
)
from lib.devices.ios_device_size_utils import get_sizes_for_devices_newer_than
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from runners.process_journey_background_image import make_standard_targets
from temp_files import temp_file
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


def _get_image_width(lw: int, lh: int, pr: Fraction) -> Fraction:
    return lw * pr


def _get_img_height(lw: int, lh: int, pr: Fraction) -> Fraction:
    return (lh - 410) * pr


def _get_img_size(lw: int, lh: int, pr: Fraction) -> Tuple[int, int]:
    return (
        math.ceil(_get_image_width(lw, lh, pr)),
        math.ceil(_get_img_height(lw, lh, pr)),
    )


RESOLUTIONS = list(
    dict.fromkeys(
        [
            (342, 223),  # preview, thumbhash
            _get_img_size(1920, 1080, Fraction(1)),
            _get_img_size(1366, 768, Fraction(1)),
            _get_img_size(1536, 864, Fraction(1)),
            _get_img_size(1440, 900, Fraction(1)),
            *get_sizes_for_devices_newer_than(
                "2019-01-01",
                mapper=lambda lw, lh, pr: (
                    math.ceil(_get_image_width(lw, lh, pr)),
                    math.ceil(_get_img_height(lw, lh, pr)),
                ),
                exclude_families={"Apple Watch", "iPod touch"},
            ),
        ]
    )
)

NO_ALPHA_TARGETS = make_standard_targets(RESOLUTIONS, alpha=False)
ALPHA_TARGETS = make_standard_targets(RESOLUTIONS, alpha=True)


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: str,
):
    """Processes the s3 file upload with the given uid for the image on an upgrade screen.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        job_progress_uid (str): The uid of the job progress to update
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.screens.upgrade_process_image",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
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
                await process_upgrade_image(
                    itgs,
                    gd,
                    stitched_path=stitched_path,
                    uploaded_by_user_sub=uploaded_by_user_sub,
                    job_progress_uid=job_progress_uid,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )


async def process_upgrade_image(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    stitched_path: str,
    uploaded_by_user_sub: Optional[str],
    job_progress_uid: Optional[str],
) -> None:
    """Processes the image at the given filepath to be used as an image in an
    upgrade screen. This will attempt to detect if alpha is needed
    and use the appropriate targets.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): used for signalling when to stop early
        stitched_path (str): the local path to the stitched file
        uploaded_by_user_sub (str, None): the sub of the user who uploaded the file
        job_progress_uid (str): the uid of the job progress to update
    """
    sanity: ProcessImageSanity = {
        "max_width": 8192,
        "max_height": 8192,
        "max_area": 4096 * 8192,
        "max_file_size": 1024 * 1024 * 512,
    }

    is_alpha = await peek_if_alpha_required(
        stitched_path, itgs=itgs, gd=gd, job_progress_uid=job_progress_uid, **sanity
    )
    targets = ALPHA_TARGETS if is_alpha else NO_ALPHA_TARGETS
    image = await process_image(
        stitched_path,
        targets,
        itgs=itgs,
        gd=gd,
        **sanity,
        name_hint="upgrade_process_image",
        job_progress_uid=job_progress_uid,
    )

    conn = await itgs.conn()
    cursor = conn.cursor()

    cfi_uid = f"oseh_cfi_{secrets.token_urlsafe(16)}"
    list_slug = "upgrade"
    uploaded_at = time.time()
    response = await cursor.executemany3(
        (
            (
                """
                INSERT INTO client_flow_images (
                    uid,
                    list_slug,
                    image_file_id,
                    original_s3_file_id,
                    original_sha512,
                    uploaded_by_user_id,
                    last_uploaded_at
                )
                SELECT
                    ?,
                    ?,
                    image_files.id,
                    image_files.original_s3_file_id,
                    image_files.original_sha512,
                    users.id,
                    ?
                FROM image_files
                LEFT OUTER JOIN users ON users.sub = ?
                WHERE
                    image_files.uid = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM client_flow_images
                        WHERE 
                            client_flow_images.image_file_id = image_files.id
                            AND client_flow_images.list_slug = ?
                    )
                """,
                (
                    cfi_uid,
                    list_slug,
                    uploaded_at,
                    uploaded_by_user_sub,
                    image.uid,
                    list_slug,
                ),
            ),
            (
                """
                UPDATE client_flow_images
                SET
                    last_uploaded_at = ?
                WHERE
                    image_file_id = (
                        SELECT image_files.id FROM image_files
                        WHERE image_files.uid = ?
                    )
                    AND list_slug = ?
                    AND uid <> ?
                """,
                (
                    uploaded_at,
                    image.uid,
                    list_slug,
                    cfi_uid,
                ),
            ),
        )
    )

    did_insert = response[0].rows_affected is not None and response[0].rows_affected > 0
    did_update = response[1].rows_affected is not None and response[1].rows_affected > 0

    assert did_insert or did_update, f"no insert or update? {response=}"
    assert not (did_insert and did_update), f"both insert and update? {response=}"
