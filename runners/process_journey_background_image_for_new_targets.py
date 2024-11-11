"""Primarily called by process_new_journey_background_image_targets"""

import os
from typing import cast
from content import hash_content
from error_middleware import handle_warning
from images import ProcessImageAbortedException, process_image
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import asyncio
import aiofiles

from jobs import JobCategory
from runners.process_journey_background_image import TARGETS
from temp_files import temp_dir

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, journey_background_image_uid: str):
    """Ensures all targets are built on the journey background image with the given uid.
    This is optimized for the case where there are new targets as it's generally triggered
    manually.

    Unlike `redo_journey_background_image`, this skips expensive operations like blurring
    by reusing the existing blurred image file. This will still verify that the original
    file is uncorrupted (sha512 hash) and will error if it is. This will do nothing if
    any of the original files are missing.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    async def bounce():
        logging.info(f"{__name__} bouncing")
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_journey_background_image_for_new_targets",
            journey_background_image_uid=journey_background_image_uid,
        )

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            original.original_sha512,
            original_s3_files.key,
            blurred.original_sha512,
            blurred_s3_files.key,
            darkened.original_sha512,
            darkened_s3_files.key
        FROM 
            journey_background_images, 
            image_files AS original, 
            s3_files AS original_s3_files,
            image_files AS blurred,
            s3_files AS blurred_s3_files,
            image_files AS darkened,
            s3_files AS darkened_s3_files
        WHERE
            original.id = journey_background_images.image_file_id
            AND original_s3_files.id = original.original_s3_file_id
            AND blurred.id = journey_background_images.blurred_image_file_id
            AND blurred_s3_files.id = blurred.original_s3_file_id
            AND darkened.id = journey_background_images.darkened_image_file_id
            AND darkened_s3_files.id = darkened.original_s3_file_id
            AND journey_background_images.uid = ?
        """,
        (journey_background_image_uid,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}:missing",
            f"{journey_background_image_uid=} is either missing originals or does not exist",
        )
        return

    original_sha512 = cast(str, response.results[0][0])
    original_s3_key = cast(str, response.results[0][1])
    blurred_sha512 = cast(str, response.results[0][2])
    blurred_s3_key = cast(str, response.results[0][3])
    darkened_sha512 = cast(str, response.results[0][4])
    darkened_s3_key = cast(str, response.results[0][5])

    files = await itgs.files()

    with temp_dir() as dirpath:

        async def download_to(key: str, filename: str, expected_sha512: str):
            filepath = os.path.join(dirpath, filename)

            async with aiofiles.open(filepath, "wb") as f:
                await files.download(
                    f, bucket=files.default_bucket, key=key, sync=False
                )

            file_hash = await hash_content(filepath)

            if file_hash != expected_sha512:
                await handle_warning(
                    f"{__name__}:corrupted",
                    f"{s3_key=} is corrupted ({file_hash=}, {expected_sha512=})",
                )
                return False

            return True

        valid_hashes = await asyncio.gather(
            download_to(original_s3_key, "original", original_sha512),
            download_to(blurred_s3_key, "blurred", blurred_sha512),
            download_to(darkened_s3_key, "darkened", darkened_sha512),
        )

        for s3_key, is_valid, hint_prefix, filename in [
            (original_s3_key, valid_hashes[0], "", "original"),
            (blurred_s3_key, valid_hashes[1], "blurred_", "blurred"),
            (darkened_s3_key, valid_hashes[2], "darkened_", "darkened"),
        ]:
            if not is_valid:
                continue

            if gd.received_term_signal:
                await bounce()
                return

            filepath = os.path.join(dirpath, filename)
            try:
                await process_image(
                    filepath,
                    TARGETS,
                    itgs=itgs,
                    gd=gd,
                    max_width=16384,
                    max_height=16384,
                    max_area=8192 * 8192,
                    max_file_size=1024 * 1024 * 512,
                    name_hint=f"{hint_prefix}journey_background_image",
                )
            except ProcessImageAbortedException:
                return await bounce()


if __name__ == "__main__":

    async def main():
        journey_background_image_uid = input("Journey Background Image UID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.process_journey_background_image_for_new_targets",
                journey_background_image_uid=journey_background_image_uid,
            )

    asyncio.run(main())
