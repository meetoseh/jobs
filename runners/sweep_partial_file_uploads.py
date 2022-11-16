"""Sweeps through any failed partial file uploads and cleans up artifacts"""
from itgs import Itgs
from typing import TypedDict
from graceful_death import GracefulDeath
from error_middleware import handle_warning
import logging
import time
import json


class PartialFileItem(TypedDict):
    bucket: str
    key: str
    expected: bool


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Cleans up any partial file uploads which have timed out

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    redis = await itgs.redis()
    files = await itgs.files()
    now = time.time()

    while True:
        response = await redis.zrange(
            "files:purgatory",
            "-inf",
            now,
            byscore=True,
            offset=0,
            num=5,
        )

        if not response:
            break

        conn = await itgs.conn()
        cursor = conn.cursor()
        for json_item in response:
            item: PartialFileItem = json.loads(json_item)
            expected = item.get("expected", False)
            logging.log(
                logging.DEBUG if expected else logging.WARNING,
                f"File upload timed out: {item=}",
            )
            if not expected:
                await handle_warning(f"{__name__}:file_upload_timed_out", f"{item=}")

            bucket = item["bucket"]
            key = item["key"]

            logging.debug(f"Deleting timed out file upload @ {item=} - {success=}")
            # doing the sql delete first ensures that if we have a ON DELETE RESTRICT,
            # it can be used to prevent this behavior - though it will cause errors to
            # be sent to slack
            await cursor.execute("DELETE FROM s3_files WHERE key = ?", (key,))
            success = await files.delete(bucket=bucket, key=key)
            await redis.zrem("files:purgatory", json_item)
