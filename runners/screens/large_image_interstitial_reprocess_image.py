"""Reprocesses an existing image with the large image interstitial targets"""

from typing import cast
from content import hash_content_sync
from error_middleware import handle_warning
from images import ProcessImageSanity, peek_if_alpha_required, process_image
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory
from runners.screens.large_image_interstitial_process_image import (
    ALPHA_TARGETS,
    NO_ALPHA_TARGETS,
)
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, image_file_uid: str):
    """Reprocesses the image file with the given uid, using the current targets for
    the large image interstitial job.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        image_file_uid (str): the uid of the image file to reprocess
    """

    async def _bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.screens.large_image_interstitial_reprocess_image",
            image_file_uid=image_file_uid,
        )

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
SELECT
    image_files.original_sha512,
    s3_files.key,
    s3_files.file_size
FROM image_files, s3_files
WHERE
    image_files.uid = ?
    AND image_files.original_s3_file_id = s3_files.id
        """,
        (image_file_uid,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}:missing_image_file",
            f"image file with uid {image_file_uid} not found",
        )
        return

    original_sha512 = cast(str, response.results[0][0])
    original_s3_key = cast(str, response.results[0][1])
    original_file_size = cast(int, response.results[0][2])

    files = await itgs.files()
    with temp_file() as original_path:
        logging.debug(
            f"downloading {original_s3_key} (expected size: {original_file_size} bytes)"
        )
        with open(original_path, "wb") as f:
            await files.download(
                f, bucket=files.default_bucket, key=original_s3_key, sync=True
            )

        if gd.received_term_signal:
            logging.info("Term signal received, bouncing")
            return await _bounce()

        logging.debug(
            f"verifying file from {original_s3_key} against {original_sha512}"
        )
        hashed_sha512 = hash_content_sync(original_path)
        if hashed_sha512 != original_sha512:
            await handle_warning(
                f"{__name__}:integrity_error",
                f"original file at {original_s3_key} has incorrect hash {hashed_sha512} (expected {original_sha512})",
            )
            return

        logging.debug(f"file from {original_s3_key} verified, reprocessing")

        sanity: ProcessImageSanity = {
            "max_width": 8192,
            "max_height": 8192,
            "max_area": 4096 * 8192,
            "max_file_size": 1024 * 1024 * 512,
        }
        is_alpha = await peek_if_alpha_required(
            original_path, itgs=itgs, gd=gd, **sanity
        )
        logging.debug(f"file from {original_s3_key} is alpha: {is_alpha}")

        targets = ALPHA_TARGETS if is_alpha else NO_ALPHA_TARGETS
        image = await process_image(
            original_path,
            targets,
            itgs=itgs,
            gd=gd,
            **sanity,
            name_hint="large_image_interstitial_process_image",
        )

        logging.info(
            f"reprocessed image file {image_file_uid}; new image uid: {image.uid} (matches: {image.uid == image_file_uid})"
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        image_uid = input("Image UID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.screens.large_image_interstitial_reprocess_image",
                image_file_uid=image_uid,
            )

    asyncio.run(main())
