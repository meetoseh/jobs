"""a temporary job to add thumbhashes to image exports now that we want them"""
import base64
import os
import socket
import time
from typing import List, Tuple
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from temp_files import temp_dir
from lib.thumbhash import image_to_thumb_hash
import asyncio

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """an example job execute - this is invoked when 'runners.example' is the name of the job

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    files = await itgs.files()
    db_batch_size = 10

    while not gd.received_term_signal:
        response = await cursor.execute(
            """
            SELECT
                image_file_exports.uid,
                s3_files.key
            FROM image_file_exports, s3_files
            WHERE 
                image_file_exports.thumbhash IS NULL
                AND s3_files.id = image_file_exports.s3_file_id
            ORDER BY image_file_exports.uid ASC
            LIMIT ?
            """,
            (db_batch_size,),
        )

        if not response.results:
            slack = await itgs.slack()
            await slack.send_ops_message(
                f"{socket.gethostname()} Finished adding thumbhashes to all images"
            )
            return

        row_thumbhashes: List[Tuple[str, str]] = []

        started_at = time.perf_counter()
        with temp_dir() as tempdir:

            async def download_row(uid: str, key: str):
                with open(os.path.join(tempdir, uid), "wb") as f:
                    await files.download(
                        f, bucket=files.default_bucket, key=key, sync=True
                    )

            await asyncio.gather(*[download_row(*row) for row in response.results])

            for row in response.results:
                uid, key = row
                logging.info(f"Adding thumbhash to image file export at {key} ({uid=})")
                filepath = os.path.join(tempdir, uid)
                thumbhash_bytes_list = image_to_thumb_hash(filepath)
                thumbhash_bytes = bytes(thumbhash_bytes_list)
                thumbhash_b64url = base64.urlsafe_b64encode(thumbhash_bytes).decode(
                    "ascii"
                )
                row_thumbhashes.append((uid, thumbhash_b64url))

        await cursor.execute(
            "WITH batch(uid, thumbhash) AS (VALUES "
            + ",".join(["(?, ?)"] * len(row_thumbhashes))
            + ") "
            "UPDATE image_file_exports "
            "SET thumbhash = batch.thumbhash "
            "FROM batch "
            "WHERE image_file_exports.uid = batch.uid",
            tuple(v for row in row_thumbhashes for v in row),
        )

        finished_at = time.perf_counter()
        logging.debug(
            f"Downloaded, computed and assigned {len(response.results)} thumbhashes in {finished_at - started_at:.2f}s"
        )


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.add_missing_thumbhashes")

    asyncio.run(main())
