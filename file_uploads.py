"""Contains helper functions for working with file uploads"""

import asyncio
import logging
import os
import secrets
import shutil
import time
import aiofiles
from typing import Dict, Optional, Set, Tuple, cast as typing_cast
from file_service import AsyncWritableBytesIO
from graceful_death import GracefulDeath
from itgs import Itgs
from lib.progressutils.progress_helper import ProgressHelper
from log_helpers import format_bps
from si_prefix import si_format


class StitchFileAbortedException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


async def stitch_file_upload(
    file_upload_uid: str,
    out: str,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    parallel: int = 10,
    job_progress_uid: Optional[str] = None,
) -> None:
    """Stitches the file upload into the target local filepath

    This will download several files concurrently, in advance of writing them to
    the output stream.

    Args:
        itgs (Itgs): The integrations to use to connect to networked services.
        file_upload_uid (str): The uid of the file upload to stitch.
        out (str): The file path to write the stitched file to.
        parallel (int): The maximum number of concurrent file downloads
        job_progress_uid (Optional[str]): The uid of the job progress to update

    Raises:
        ValueError: If no such file upload exists.
        StitchFileAbortedException: if a term signal is received before we're done
    """
    prog = ProgressHelper(itgs, job_progress_uid)
    await prog.push_progress(f"preparing to stitch file with {parallel} workers")
    started_stitching_at = time.perf_counter()
    await _stitch_file_upload(
        file_upload_uid, out, itgs=itgs, gd=gd, parallel=parallel, prog=prog
    )
    stitching_time = time.perf_counter() - started_stitching_at
    file_size = os.path.getsize(out)
    msg = f"stitched a {si_format(file_size, precision=3)}b file in {stitching_time:.3f}s: {format_bps(stitching_time, file_size)}"
    logging.info(msg)
    await prog.push_progress(msg)


async def _stitch_file_upload(
    file_upload_uid: str,
    out: str,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    parallel: int = 10,
    prog: ProgressHelper,
) -> None:
    conn = await itgs.conn()
    cursor = conn.cursor("strong")

    tmp_folder = os.path.join("tmp", secrets.token_urlsafe(8))
    os.makedirs(tmp_folder, exist_ok=True)

    try:
        known_part_s3_keys: Dict[int, str] = {}
        # maps from part number to s3 key, for the ones we already know about

        ready_parts: Dict[int, str] = {}
        # the parts which we have downloaded but not yet written to the output stream.
        # the keys are the part numbers, and the values are the paths to the downloaded files.

        next_s3_key_part_number: int = 1
        # the next part number we need to get the s3 key for

        next_write_part_number: int = 1
        # the next part number we want to write to the output stream

        next_download_part_number: int = 1
        # the next part number we want to start downloading

        get_s3_keys_task: Optional[asyncio.Task] = None
        # the task which is currently fetching s3 keys. The result will
        # be a dict which can be merged with known_part_s3_keys

        upload_part_task: Optional[asyncio.Task] = None
        # the task which is currently writing to the output stream. this will be
        # writing next_write_part_number - 1

        download_part_tasks: Set[asyncio.Task] = set()
        # the tasks which are currently downloading files. the tasks will
        # return the part number they downloaded, and the path to the downloaded file,
        # as a tuple

        progress_task: Optional[asyncio.Task] = None

        response = await cursor.execute(
            """
            SELECT
                MAX(part_number)
            FROM s3_file_upload_parts
            WHERE
                EXISTS (
                    SELECT 1 FROM s3_file_uploads
                    WHERE s3_file_uploads.id = s3_file_upload_parts.s3_file_upload_id
                        AND s3_file_uploads.uid = ?
                )
            """,
            (file_upload_uid,),
        )
        if (
            not response.results
            or response.results[0][0] is None
            or response.results[0][0] < 1
        ):
            raise ValueError("Invalid file upload uid")

        last_part_number: int = response.results[0][0]
        progress_task = asyncio.create_task(
            prog.push_progress(
                "stitching", indicator={"type": "bar", "at": 0, "of": last_part_number}
            )
        )

        async with aiofiles.open(out, "wb") as out_stream:
            while (
                upload_part_task is not None
                or next_write_part_number <= last_part_number
            ):
                if gd.received_term_signal:
                    await asyncio.wait(
                        [
                            t
                            for t in [
                                get_s3_keys_task,
                                upload_part_task,
                                progress_task,
                                *download_part_tasks,
                            ]
                            if t is not None
                        ],
                        return_when=asyncio.ALL_COMPLETED,
                    )
                    raise StitchFileAbortedException()

                if (
                    get_s3_keys_task is None
                    and next_s3_key_part_number <= last_part_number
                    and (next_download_part_number - next_s3_key_part_number) < 100
                ):
                    end_part_number = min(
                        next_s3_key_part_number + 100, last_part_number + 1
                    )
                    get_s3_keys_task = asyncio.create_task(
                        _get_part_s3_keys(
                            itgs,
                            file_upload_uid,
                            next_s3_key_part_number,
                            end_part_number,
                        )
                    )
                    next_s3_key_part_number = end_part_number

                while (
                    len(download_part_tasks) < parallel
                    and next_download_part_number in known_part_s3_keys
                ):
                    download_part_tasks.add(
                        asyncio.create_task(
                            _download_part(
                                itgs,
                                known_part_s3_keys[next_download_part_number],
                                next_download_part_number,
                                tmp_folder,
                            )
                        )
                    )
                    del known_part_s3_keys[next_download_part_number]
                    next_download_part_number += 1

                if upload_part_task is None and next_write_part_number in ready_parts:
                    upload_part_task = asyncio.create_task(
                        _upload_part(
                            itgs, ready_parts[next_write_part_number], out_stream
                        )
                    )
                    del ready_parts[next_write_part_number]
                    next_write_part_number += 1

                done, _ = await asyncio.wait(
                    [
                        t
                        for t in [
                            get_s3_keys_task,
                            upload_part_task,
                            *download_part_tasks,
                        ]
                        if t is not None
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if get_s3_keys_task is not None and get_s3_keys_task in done:
                    get_s3_keys_task_exc = get_s3_keys_task.exception()
                    if get_s3_keys_task_exc is not None:
                        await asyncio.wait(
                            [
                                t
                                for t in [upload_part_task, *download_part_tasks]
                                if t is not None
                            ],
                            return_when=asyncio.ALL_COMPLETED,
                        )
                        raise get_s3_keys_task_exc

                    known_part_s3_keys.update(get_s3_keys_task.result())
                    get_s3_keys_task = None

                if upload_part_task in done:
                    if progress_task is None or progress_task.done():
                        progress_task = asyncio.create_task(
                            prog.push_progress(
                                "stitching",
                                indicator={
                                    "type": "bar",
                                    "at": next_write_part_number - 1,
                                    "of": last_part_number,
                                },
                            )
                        )

                    upload_part_task = None

                for task in done:
                    if task in download_part_tasks:
                        part_number, path = task.result()
                        ready_parts[part_number] = path
                        download_part_tasks.remove(task)

        if progress_task is not None:
            await progress_task
    finally:
        shutil.rmtree(tmp_folder)


async def _get_part_s3_keys(
    itgs: Itgs,
    file_upload_uid: str,
    start_part_number: int,
    end_part_number: int,
) -> Dict[int, str]:
    conn = await itgs.conn()
    cursor = conn.cursor("strong")

    response = await cursor.execute(
        """
        SELECT
            s3_file_upload_parts.part_number,
            s3_files.key
        FROM s3_file_upload_parts
        JOIN s3_files ON s3_files.id = s3_file_upload_parts.s3_file_id
        WHERE
            EXISTS (
                SELECT 1 FROM s3_file_uploads
                WHERE s3_file_uploads.id = s3_file_upload_parts.s3_file_upload_id
                  AND s3_file_uploads.uid = ?
            )
            AND s3_file_upload_parts.part_number BETWEEN ? AND ?
        """,
        (file_upload_uid, start_part_number, end_part_number - 1),
    )

    res = typing_cast(Dict[int, str], dict(response.results or []))
    assert len(res) == end_part_number - start_part_number
    return res


async def _download_part(
    itgs: Itgs,
    key: str,
    part_number: int,
    tmp_folder: str,
) -> Tuple[int, str]:
    files = await itgs.files()
    out_path = os.path.join(tmp_folder, secrets.token_urlsafe(8))
    async with aiofiles.open(out_path, "wb") as out_stream:
        await files.download(
            out_stream, bucket=files.default_bucket, key=key, sync=False
        )
    return part_number, out_path


async def _upload_part(itgs: Itgs, from_path: str, to: AsyncWritableBytesIO):
    async with aiofiles.open(from_path, "rb") as from_stream:
        while True:
            chunk = await from_stream.read(8192)
            if not chunk:
                break
            await to.write(chunk)
