"""Redoes a single journey background image whose original file is still available.
This is typically queued by the file with the plural name, which will queue this
job for each journey background image which still has an original file available
"""
from itgs import Itgs
from graceful_death import GracefulDeath
from error_middleware import handle_warning
from temp_files import temp_file
from content import hash_content
from .process_journey_background_image import TARGETS, blur_image, darken_image
from images import process_image, ProcessImageAbortedException
import logging
import aiofiles
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, journey_background_image_uid: str):
    """Completes any missing exports for the journey background image with the given uid.
    This will reblur the original image with current settings and use that as the source
    for the blurred image file; updating any journeys using the base image file to use the
    new blurred image file for its blurred version.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_background_image_uid (str): the uid of the journey background image to redo
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.redo_journey_background_image",
            journey_background_image_uid=journey_background_image_uid,
        )

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            s3_files.key,
            image_files.original_sha512
        FROM image_files
        JOIN s3_files ON s3_files.id = image_files.original_s3_file_id
        WHERE
            EXISTS (
                SELECT 1 FROM journey_background_images
                WHERE journey_background_images.image_file_id = image_files.id
                  AND journey_background_images.uid = ?
            )
        """,
        (journey_background_image_uid,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}:missing",
            f"The journey background image with the uid {journey_background_image_uid} either "
            "does not exist or its original file has been deleted",
        )
        return

    s3_key: str = response.results[0][0]
    original_sha512: str = response.results[0][1]

    file_service = await itgs.files()

    with temp_file() as tmp_filepath, temp_file() as blurred_filepath, temp_file() as darkened_path:
        async with aiofiles.open(tmp_filepath, "wb") as tmp_file:
            success = await file_service.download(
                tmp_file, bucket=file_service.default_bucket, key=s3_key, sync=False
            )

        if not success:
            await handle_warning(
                f"{__name__}:gone",
                f"The original file for the journey background image with the uid "
                f"{journey_background_image_uid} is no longer available",
            )
            return

        if gd.received_term_signal:
            await bounce()
            return

        file_hash = await hash_content(tmp_filepath)
        if file_hash != original_sha512:
            await handle_warning(
                f"{__name__}:corrupt",
                f"The original file for the journey background image with the uid "
                f"{journey_background_image_uid} is corrupt (hash mismatch; expected "
                f"{original_sha512}, got {file_hash})",
            )
            return

        if gd.received_term_signal:
            await bounce()
            return

        try:
            image = await process_image(
                tmp_filepath,
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

        try:
            await blur_image(tmp_filepath, blurred_filepath)
        except ProcessImageAbortedException:
            return await bounce()

        if gd.received_term_signal:
            await bounce()
            return

        try:
            blurred_image = await process_image(
                blurred_filepath,
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

        try:
            await darken_image(tmp_filepath, darkened_path)
        except ProcessImageAbortedException:
            return await bounce()

        try:
            darkened_image = await process_image(
                darkened_path,
                TARGETS,
                itgs=itgs,
                gd=gd,
                max_width=16384,
                max_height=16384,
                max_area=8192 * 8192,
                max_file_size=1024 * 1024 * 512,
                name_hint="darkened_journey_background_image",
            )
        except ProcessImageAbortedException:
            return await bounce()

        await cursor.executemany3(
            (
                (
                    """
                    UPDATE journey_background_images 
                    SET 
                        blurred_image_file_id = blurred_image_files.id,
                        darkened_image_file_id = darkened_image_files.id
                    FROM image_files AS blurred_image_files, image_files AS darkened_image_files
                    WHERE
                        journey_background_images.uid = ?
                        AND blurred_image_files.uid = ?
                        AND darkened_image_files.uid = ?
                    """,
                    (
                        journey_background_image_uid,
                        blurred_image.uid,
                        darkened_image.uid,
                    ),
                ),
                (
                    """
                    UPDATE journeys
                    SET 
                        blurred_background_image_file_id = blurred_image_files.id,
                        darkened_background_image_file_id = darkened_image_files.id
                    FROM image_files, image_files AS blurred_image_files, image_files AS darkened_image_files
                    WHERE
                        journeys.background_image_file_id = image_files.id
                        AND image_files.uid = ?
                        AND blurred_image_files.uid = ?
                        AND darkened_image_files.uid = ?
                    """,
                    (
                        image.uid,
                        blurred_image.uid,
                        darkened_image.uid,
                    ),
                ),
            )
        )
        logging.info(
            f"Redid journey background image with uid {journey_background_image_uid}"
        )
