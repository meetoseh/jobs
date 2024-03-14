"""Makes some progress re-processing profile pictures that were processed before
the most recent version of profile picture targets
"""

import time
from typing import Optional, cast
from content import hash_content_sync
from error_middleware import handle_warning
from images import ProcessImageAbortedException, process_image
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory
from runners.check_profile_picture import LAST_TARGETS_CHANGED_AT, TARGETS
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 300
"""The maximum amount of time to spend in a single job execution on reprocessing
profile pictures
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Reprocesses profile pictures that haven't been reprocessed since the last time
    the profile picture targets were updated.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    started_at = time.time()
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    while True:
        if gd.received_term_signal:
            logging.info("Term signal received, stopping early")
            break

        if time.time() - started_at > MAX_JOB_TIME_SECONDS:
            logging.info("Reached maximum job time, stopping early")
            break

        response = await cursor.execute(
            """
            SELECT
                user_profile_pictures.uid,
                users.sub,
                image_files.uid,
                image_files.original_sha512,
                s3_files.key,
                s3_files.file_size
            FROM user_profile_pictures, users, image_files
            LEFT OUTER JOIN s3_files ON image_files.original_s3_file_id = s3_files.id
            WHERE
                user_profile_pictures.latest = 1
                AND user_profile_pictures.user_id = users.id
                AND user_profile_pictures.image_file_id = image_files.id
                AND user_profile_pictures.last_processed_at < ?
            ORDER BY user_profile_pictures.last_processed_at ASC
            LIMIT 1
            """,
            (LAST_TARGETS_CHANGED_AT,),
        )

        if not response.results:
            logging.info("No more profile pictures to process")
            break

        row = response.results[0]
        upp_uid = cast(str, row[0])
        user_sub = cast(str, row[1])
        image_file_uid = cast(str, row[2])
        original_sha512 = cast(str, row[3])
        original_s3_key = cast(Optional[str], row[4])
        original_file_size = cast(Optional[int], row[5])

        logging.debug(
            f"Handling row: {upp_uid=}, {user_sub=}, {image_file_uid=}, {original_sha512=}, {original_s3_key=}, {original_file_size=}"
        )

        if original_s3_key is None:
            assert original_file_size is None, row
            logging.debug(
                f"{upp_uid=} is missing its original export, marking reprocessed and continuing"
            )
            await cursor.execute(
                "UPDATE user_profile_pictures SET last_processed_at = ? WHERE uid = ?",
                (time.time(), upp_uid),
            )
            continue

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
                logging.info("Term signal received, stopping early")
                break

            logging.debug(
                f"verifying file from {original_s3_key} against {original_sha512}"
            )
            hashed_sha512 = hash_content_sync(original_path)
            if hashed_sha512 != original_sha512:
                await handle_warning(
                    f"{__name__}:integrity_error",
                    f"original file at {original_s3_key} has incorrect hash {hashed_sha512} (expected {original_sha512})",
                )
                await cursor.execute(
                    "UPDATE user_profile_pictures SET last_processed_at = ? WHERE uid = ?",
                    (time.time(), upp_uid),
                )
                continue

            logging.debug(f"file from {original_s3_key} verified, reprocessing")
            try:
                image = await process_image(
                    original_path,
                    TARGETS,
                    itgs=itgs,
                    gd=gd,
                    max_width=16384,
                    max_height=16384,
                    max_area=8192 * 8192,
                    max_file_size=1024 * 1024 * 512,
                    name_hint="reprocess_profile_picture",
                )
            except ProcessImageAbortedException:
                logging.debug("signal received while reprocessing, stopping early")
                break

            logging.debug(
                f"done reprocessing {original_s3_key} into {image.uid} (expected: {image_file_uid})"
            )
            if image.uid != image_file_uid:
                await handle_warning(
                    f"{__name__}:zombied_image",
                    f"Processing image from {original_s3_key} resulted in {image.uid} (expected {image_file_uid})",
                )

            await cursor.execute(
                "UPDATE user_profile_pictures SET last_processed_at = ? WHERE uid = ?",
                (time.time(), upp_uid),
            )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.reprocess_profile_pictures")

    asyncio.run(main())
