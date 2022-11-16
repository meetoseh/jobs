"""Helper module for working with content files in general. For working with
audio content specifically, prefer the audio module.
"""
from dataclasses import dataclass
import hashlib
import json
import time
from typing import Any, Dict, List, Optional
import aiofiles
import multiprocessing.pool
from itgs import Itgs
import asyncio


@dataclass
class S3File:
    """An s3 file, see backend/docs/db/s3_files.md for more info"""

    uid: str
    bucket: str
    key: str
    file_size: int
    content_type: str
    created_at: float


@dataclass
class ContentFileExportPart:
    """see backend/docs/db/content_file_export_parts.md"""

    uid: str
    s3_file: S3File
    position: int
    duration_seconds: float
    created_at: float


@dataclass
class ContentFileExport:
    """see backend/docs/db/content_file_exports.md"""

    uid: str
    format: str
    bandwidth: int
    codecs: List[str]
    target_duration: int
    quality_parameters: Dict[str, Any]
    created_at: float
    parts: List[ContentFileExportPart]
    """in order, position ascending"""


@dataclass
class ContentFile:
    """see backend/docs/db/content_files.md"""

    uid: str
    name: str
    original: Optional[S3File]
    original_sha512: str
    duration_seconds: float
    created_at: float

    exports: List[ContentFileExport]
    """not necessarily in any particular order"""


async def get_content_file(
    itgs: Itgs, uid: str, consistency_level: str = "strong"
) -> Optional[ContentFile]:
    """Gets information on the content file with the given uid, if it exists,
    otherwise returns null.

    Is only guarranteed to return exports and parts which exist throughout
    the entire function call.

    Args:
        itgs (Itgs): The integrations for networked services
        uid (str): The uid of the content file to get
        consistency_level (str): The consistency level to use for the queries

    Returns:
        ContentFile, None: The content file, or None if it doesn't exist
    """
    conn = await itgs.conn()
    cursor = conn.cursor(consistency_level)

    response = await cursor.execute(
        """
        SELECT
            content_files.name,
            s3_files.uid,
            s3_files.key,
            s3_files.file_size,
            s3_files.content_type,
            s3_files.created_at,
            content_files.original_sha512,
            content_files.duration_seconds,
            content_files.created_at
        FROM content_files
        LEFT JOIN s3_files ON s3_files.id = content_files.original_s3_file_id
        WHERE
            content_files.uid = ?
        """,
        (uid,),
    )
    if not response.results:
        return None

    content_file = ContentFile(
        uid=uid,
        name=response.results[0][0],
        original=S3File(
            uid=response.results[0][1],
            key=response.results[0][2],
            file_size=response.results[0][3],
            content_type=response.results[0][4],
            created_at=response.results[0][5],
        ),
        original_sha512=response.results[0][6],
        duration_seconds=response.results[0][7],
        created_at=response.results[0][8],
        exports=[],
    )

    response = await cursor.execute(
        """
        SELECT
            content_file_exports.uid,
            content_file_exports.format,
            content_file_exports.bandwidth,
            content_file_exports.codecs,
            content_file_exports.target_duration,
            content_file_exports.quality_parameters,
            content_file_exports.created_at,
        FROM content_file_exports
        WHERE
            EXISTS (
                SELECT 1 FROM content_files
                WHERE content_files.id = content_file_exports.content_file_id
                  AND content_files.uid = ?
            )
        """,
        (uid,),
    )
    for row in response.results:
        content_file.exports.append(
            ContentFileExport(
                uid=row[0],
                format=row[1],
                bandwidth=row[2],
                codecs=row[3],
                target_duration=row[4],
                quality_parameters=row[5],
                created_at=row[6],
                parts=[],
            )
        )

    for exp in content_file.exports:
        response = await cursor.execute(
            """
            SELECT
                content_file_export_parts.uid,
                s3_files.uid,
                s3_files.key,
                s3_files.file_size,
                s3_files.content_type,
                s3_files.created_at,
                content_file_export_parts.position,
                content_file_export_parts.duration_seconds,
                content_file_export_parts.created_at
            FROM content_file_export_parts
            JOIN s3_files ON s3_files.id = content_file_export_parts.s3_file_id
            WHERE
                EXISTS (
                    SELECT 1 FROM content_file_exports
                    WHERE content_file_exports.id = content_file_export_parts.content_file_export_id
                      AND content_file_exports.uid = ?
                )
            ORDER BY content_file_export_parts.position ASC
            """,
            (exp.uid,),
        )
        for row in response.results:
            exp.parts.append(
                ContentFileExportPart(
                    uid=row[0],
                    s3_file=S3File(
                        uid=row[1],
                        key=row[2],
                        file_size=row[3],
                        content_type=row[4],
                        created_at=row[5],
                    ),
                    position=row[6],
                    duration_seconds=row[7],
                    created_at=row[8],
                )
            )

    return content_file


async def upload_s3_file_and_put_in_purgatory(
    file: S3File,
    local_filepath: str,
    *,
    itgs: Itgs,
    protect_for: int,
) -> None:
    """Uploads the file at the given local filepath to s3. The file is stored in the
    database and files:purgatory

    Args:
        file (S3File): The information to store in the database, including where
            to upload the file to
        local_filepath (str): Where the file is available locally
        itgs (Itgs): The integrations for networked services
        protect_for (int): The number of seconds to protect the file for, before
            it will be automatically deleted unless the appropriate key has been
            removed from files:purgatory
    """
    files = await itgs.files()
    conn = await itgs.conn()
    cursor = conn.cursor()
    redis = await itgs.redis()

    purgatory_key = json.dumps({"key": file.key, "bucket": file.bucket}, sort_keys=True)
    await redis.zadd(
        "files:purgatory", mapping={purgatory_key: time.time() + protect_for}
    )
    async with aiofiles.open(local_filepath, "rb") as f:
        await files.upload(f, bucket=file.bucket, key=file.key, sync=False)

    await cursor.execute(
        """
        INSERT INTO s3_files (
            uid, key, file_size, content_type, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            file.uid,
            file.key,
            file.file_size,
            file.content_type,
            file.created_at,
        ),
    )


async def hash_content(local_filepath: str) -> str:
    """Hashes the content at the given filepath using sha512. This will read
    asynchronously but compute the hash synchronously - so it can be better
    to use a threadpool/process pool and use hash_content_sync instead.
    """
    sha512 = hashlib.sha512()
    async with aiofiles.open(local_filepath, mode="rb") as f:
        while True:
            chunk = await f.read(8192)
            if not chunk:
                break
            sha512.update(chunk)
    return sha512.hexdigest()


async def hash_content_using_pool(
    local_filepath: str, *, pool: multiprocessing.pool.Pool
) -> str:
    """Hashes the given content synchronously in another process using the
    given multiprocessing pool
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def _on_done(result):
        loop.call_soon_threadsafe(fut.set_result, result)

    def _on_error(err):
        loop.call_soon_threadsafe(fut.set_exception, err)

    pool.apply_async(
        hash_content_sync,
        args=(local_filepath,),
        callback=_on_done,
        error_callback=_on_error,
    )
    return await fut


def hash_content_sync(local_filepath: str) -> str:
    """Hashes the content at the given filepath using sha512, synchronously"""
    sha512 = hashlib.sha512()
    with open(local_filepath, mode="rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            sha512.update(chunk)
    return sha512.hexdigest()
