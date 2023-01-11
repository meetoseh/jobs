"""Module for producing content files that contain audio content, using ffmpeg"""


import logging
import logging.config
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from content import (
    ContentFile,
    ContentFileExport,
    ContentFileExportPart,
    S3File,
    hash_content_using_pool,
    upload_s3_file_and_put_in_purgatory,
)
from graceful_death import GracefulDeath
from itgs import Itgs
from dataclasses import dataclass
import shutil
import secrets
import subprocess
import os
import itertools
import multiprocessing
import multiprocessing.pool
import asyncio
import json
import time

from m3u8 import (
    M3UContent,
    M3UPlaylist,
    M3UVodReference,
    get_m3u_local_filepath,
    parse_m3u_playlist,
)


class ProcessAudioAbortedException(Exception):
    """Raised if process_audio aborted due to a term signal"""

    pass


AUDIO_BITRATES = (32, 64, 90, 128, 256, 512, 1028, 1411)
"""The bitrates we try to encode at"""


@dataclass
class _PreparedMP4:
    """An mp4 file that we've finished processing locally and we've decided
    keys/uids/timestamps for, but have not necessarily finished uploading/storing
    in the database
    """

    local_filepath: str
    """Where the mp4 is available locally"""

    export: ContentFileExport
    """The export we've decided on, will have exactly one part"""


@dataclass
class _PreparedM3UVodExportPart:
    """Acts like a ContentFileExportPart but for a prepared m3u vod, which includes a
    local filepath
    """

    uid: str
    s3_file: S3File
    position: int
    duration_seconds: float
    created_at: float

    m3u_content: M3UContent
    """The underlying m3u content; relative filepaths are relative to the
    directory containing the m3u vod file
    """


@dataclass
class _PreparedM3UVodExport:
    """Acts like a ContentFileExport, but for a prepared m3u vod, which means it's
    for a specific
    """

    uid: str
    format: str
    bandwidth: int
    codecs: List[str]
    target_duration: int
    quality_parameters: Dict[str, Any]
    created_at: float

    parts: List[_PreparedM3UVodExportPart]
    """in order, position ascending"""

    vod_ref: M3UVodReference
    """The local vod reference. Relative paths will be relative to the directory
    containg the master_file_path for the _PreparedM3UPlaylist
    """


@dataclass
class _PreparedM3UPlaylist:
    """An m3u playlist that we've finished processing locally and we've decided
    keys/uids/timestamps for, but have not necessarily finished uploading/storing
    in the database. The playlist itself is not actually stored to the database,
    as it can be generated from the vods which are available - which might be more
    than what we just exported locally if there were already some vods available
    """

    master_file_path: str
    """Where the master hls file is stored"""

    playlist: M3UPlaylist
    """The parsed m3u playlist. The relative paths are relative to the directory
    containing `master_file_path`
    """

    vods: List[_PreparedM3UVodExport]
    """The hls vods we've decided to export. Note we don't need to export the
    actual master file, since it's produced from the available vods
    """


@dataclass
class _PreparedAudioContent:
    """Describes audio content we've finished processing locally and we've
    decided keys/uids for, but have not necessarily uploaded to s3/stored in
    the database
    """

    content_file_uid: str
    """The uid we've chosen for the content file"""

    original_filepath: str
    """Where the original file is located locally"""

    original: S3File
    """Where we intend to store the original audio file"""

    original_sha512: str
    """The sha512 of the original audio file"""

    name: str
    """The name we selected for the content file"""

    duration_seconds: float
    """The duration of the audio file"""

    created_at: float
    """The time we created the content file"""

    mp4s: List[_PreparedMP4]
    """The mp4s we've decided to export"""

    hls: _PreparedM3UPlaylist
    """The hls playlist we've decided to export"""


@dataclass
class _Mp4Info:
    duration: float
    """The duration of the mp4 in seconds"""

    bit_rate: int
    """The true, post-encoding bit rate of the mp4 in bits per second"""


async def process_audio(
    local_filepath: str,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    max_file_size: int,
    name_hint: str,
) -> ContentFile:
    """Processes the audio file at the given local filepath into the standard exports,
    then returns the content file that contains the exports.

    If the given file has already been processed, as dictated by it's sha512
    hash, then any missing exports are produced and **only those newly produced
    exports are returned**. Use content.get_content_file to get the full
    content file.

    Args:
        local_filepath (str): The local filepath of the audio file to process.
        itgs (Itgs): the integration to use for networked services
        gd (GracefulDeath): the signal tracker
        max_file_size (int): The maximum size of the content file, in bytes.
        name_hint (str): A hint for the name of the content file.

    Raises:
        ProcessAudioAbortedException: if the process was aborted due to a term signal.
            Should retry soon on a fresh process.
    """
    ffmpeg = shutil.which("ffmpeg")
    assert ffmpeg is not None, "ffmpeg not found"

    file_size = os.path.getsize(local_filepath)
    if file_size > max_file_size:
        raise ValueError(f"{file_size=} exceeds {max_file_size=}")

    name = name_hint[:64] + "-" + secrets.token_urlsafe(16)

    temp_folder = os.path.join("tmp", "process_audio", secrets.token_urlsafe(8))
    os.makedirs(temp_folder, exist_ok=True)
    try:
        return await process_audio_into(
            local_filepath,
            itgs=itgs,
            gd=gd,
            temp_folder=temp_folder,
            name=name,
            ffmpeg=ffmpeg,
        )
    finally:
        shutil.rmtree(temp_folder)


async def process_audio_into(
    local_filepath: str,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    temp_folder: str,
    name: str,
    ffmpeg: str,
) -> ContentFile:
    """Processes the audio file at the given local filepath into the standard
    exports, using the specified temporary folder. The produced content file is
    given the specified name if it does not already exist. If the file has already
    been processed, as dictated by it's sha512 hash, then any missing exports are
    produced and the returned content file **ONLY HAS THE NEWLY PRODUCED EXPORTS**.

    PERF:
        This currently always produces all of the exports, even if they already
        exist, but will delete any that are not needed. This is just for simplicity
        and can be swapped out later if it becomes a performance issue.

    Args:
        local_filepath (str): The path to where the audio file to process is located.
        itgs (Itgs): the integration to use for networked services
        gd (GracefulDeath): the signal tracker
        temp_folder (str): A folder to use for temporary files. This will not be
            cleaned up.
        name (str): The name of the content file to produce.
        ffmpeg (str): The path to the ffmpeg executable.
    """
    mp4_folder = os.path.join(temp_folder, "mp4")
    os.makedirs(mp4_folder, exist_ok=True)
    mp4_bitrates_to_local_files: Dict[int, str] = dict()
    mp4_bitrates_to_info: Dict[int, _Mp4Info] = dict()
    with multiprocessing.Pool(processes=2) as pool:
        original_sha512_task = asyncio.create_task(
            hash_content_using_pool(local_filepath, pool=pool)
        )
        for bitrate in AUDIO_BITRATES:
            if gd.received_term_signal:
                raise ProcessAudioAbortedException()

            target_filepath = os.path.join(mp4_folder, f"{bitrate}.mp4")
            mp4_bitrates_to_local_files[bitrate] = target_filepath
            await produce_mp4_async(
                local_filepath,
                target_filepath,
                ffmpeg=ffmpeg,
                bitrate_kbps=bitrate,
                pool=pool,
            )
            with open(f"{target_filepath}.json", "r") as f:
                mp4_bitrates_to_info[bitrate] = _Mp4Info(**json.load(f))

        if gd.received_term_signal:
            raise ProcessAudioAbortedException()

        os.makedirs(os.path.join(temp_folder, "hls"), exist_ok=True)
        hls_master_file_path = await produce_m3u8_async(
            local_filepath, os.path.join(temp_folder, "hls"), ffmpeg=ffmpeg, pool=pool
        )
        if gd.received_term_signal:
            raise ProcessAudioAbortedException()

        master_m3u = await parse_m3u_playlist(hls_master_file_path)
        original_sha512 = await original_sha512_task

    now = time.time()
    files = await itgs.files()
    content_file_uid = f"oseh_cf_{secrets.token_urlsafe(16)}"
    prepared = _PreparedAudioContent(
        content_file_uid=content_file_uid,
        original_filepath=local_filepath,
        original=S3File(
            uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
            bucket=files.default_bucket,
            key=f"s3_files/audio/originals/{content_file_uid}/{name}",
            file_size=os.path.getsize(local_filepath),
            content_type="application/octet-stream",
            created_at=now,
        ),
        original_sha512=original_sha512,
        name=name,
        duration_seconds=mp4_bitrates_to_info[AUDIO_BITRATES[0]].duration,
        created_at=now,
        mp4s=[
            _PreparedMP4(
                local_filepath=local_filepath,
                export=ContentFileExport(
                    uid=f"oseh_cfe_{secrets.token_urlsafe(16)}",
                    format="mp4",
                    bandwidth=mp4_bitrates_to_info[bitrate].bit_rate,
                    codecs=["aac"],
                    target_duration=int(mp4_bitrates_to_info[bitrate].duration),
                    quality_parameters={"bitrate_kbps": bitrate, "faststart": True},
                    created_at=now,
                    parts=[
                        ContentFileExportPart(
                            uid=f"oseh_cfep_{secrets.token_urlsafe(16)}",
                            s3_file=S3File(
                                uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
                                bucket=files.default_bucket,
                                key=f"s3_files/audio/{content_file_uid}/mp4/{bitrate}/{secrets.token_urlsafe(8)}.mp4",
                                file_size=os.path.getsize(local_filepath),
                                content_type="audio/mp4",
                                created_at=now,
                            ),
                            position=0,
                            duration_seconds=mp4_bitrates_to_info[bitrate].duration,
                            created_at=now,
                        )
                    ],
                ),
            )
            for bitrate, local_filepath in mp4_bitrates_to_local_files.items()
        ],
        hls=_PreparedM3UPlaylist(
            master_file_path=hls_master_file_path,
            playlist=master_m3u,
            vods=[
                _PreparedM3UVodExport(
                    uid=f"oseh_cfe_{secrets.token_urlsafe(16)}",
                    format="m3u8",
                    bandwidth=vod.bandwidth,
                    codecs=vod.codecs,
                    target_duration=vod.vod.target_duration,
                    quality_parameters={"audio_bitrate_kbps": AUDIO_BITRATES[vod_idx]},
                    created_at=now,
                    parts=[
                        _PreparedM3UVodExportPart(
                            uid=f"oseh_cfep_{secrets.token_urlsafe(16)}",
                            s3_file=S3File(
                                uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
                                bucket=files.default_bucket,
                                key=f"s3_files/audio/{content_file_uid}/hls/{secrets.token_urlsafe(8)}.ts",
                                file_size=os.path.getsize(
                                    get_m3u_local_filepath(
                                        hls_master_file_path, vod, part
                                    )
                                ),
                                content_type="video/MP2T",
                                created_at=now,
                            ),
                            position=part_idx,
                            duration_seconds=part.runtime_seconds,
                            created_at=now,
                            m3u_content=part,
                        )
                        for part_idx, part in enumerate(vod.vod.content)
                    ],
                    vod_ref=vod,
                )
                for vod_idx, vod in enumerate(master_m3u.vods)
            ],
        ),
    )

    await _upload_all(
        prepared,
        itgs=itgs,
        gd=gd,
    )
    return await _upsert_prepared(
        prepared,
        itgs=itgs,
        gd=gd,
    )


async def _upload_all(
    prepared: _PreparedAudioContent,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    parallelism: int = 8,
) -> None:
    """Performs all the necessary file uploads to s3, marking all of the files in
    files:purgatory
    """
    remaining: List[Tuple[str, S3File]] = [
        (prepared.original_filepath, prepared.original),
        *[(mp4.local_filepath, mp4.export.parts[0].s3_file) for mp4 in prepared.mp4s],
        *[
            (
                get_m3u_local_filepath(
                    prepared.hls.master_file_path, vod.vod_ref, part.m3u_content
                ),
                part.s3_file,
            )
            for vod in prepared.hls.vods
            for part in vod.parts
        ],
    ]

    pending: Set[asyncio.Task] = set()
    while remaining or pending:
        while remaining and len(pending) < parallelism:
            local_filepath, s3_file = remaining.pop()
            pending.add(
                asyncio.create_task(
                    upload_s3_file_and_put_in_purgatory(
                        s3_file,
                        local_filepath=local_filepath,
                        itgs=itgs,
                        protect_for=60 * 60,
                    )
                )
            )

        _, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)


async def _upsert_prepared(
    prepared: _PreparedAudioContent,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
) -> ContentFile:
    """Upserts the given prepared audio content into the database. This is done via
    optimistic locking, and will abort on failure.

    Any files which have references stored in the database are removed from files:purgatory
    """

    conn = await itgs.conn()
    cursor = conn.cursor("strong")
    redis = await itgs.redis()
    files = await itgs.files()

    response = await cursor.execute(
        """
        SELECT
            content_files.uid,
            content_files.name,
            s3_files.uid,
            s3_files.key,
            s3_files.file_size,
            s3_files.content_type,
            s3_files.created_at
        FROM content_files
        LEFT OUTER JOIN s3_files ON s3_files.id = content_files.original_s3_file_id
        WHERE
            content_files.original_sha512 = ?
        """,
        (prepared.original_sha512,),
    )

    if response.results:
        content_file_uid: str = response.results[0][0]
        content_file_name: str = response.results[0][1]
        original_s3_file_uid: Optional[str] = response.results[0][2]
        original_s3_file_key: Optional[str] = response.results[0][3]
        original_s3_file_size: Optional[int] = response.results[0][4]
        original_s3_file_content_type: Optional[str] = response.results[0][5]
        original_s3_file_created_at: Optional[float] = response.results[0][6]

        if original_s3_file_uid is None:
            response = await cursor.execute(
                """
                UPDATE content_files
                SET original_s3_file_id = s3_files.id
                FROM s3_files
                WHERE
                    s3_files.uid = ?
                    AND content_files.uid = ?
                    AND content_files.original_s3_file_id IS NULL
                """,
                (prepared.original.uid, prepared.content_file_uid),
            )

            if response.rows_affected is None or response.rows_affected < 1:
                raise ProcessAudioAbortedException()

            original_s3_file_uid = prepared.original.uid
            original_s3_file_key = prepared.original.key
            original_s3_file_size = prepared.original.file_size
            original_s3_file_content_type = prepared.original.content_type
            original_s3_file_created_at = prepared.original.created_at

        assert original_s3_file_uid is not None
        assert original_s3_file_key is not None
        assert original_s3_file_size is not None
        assert original_s3_file_content_type is not None
        assert original_s3_file_created_at is not None
    else:
        response = await cursor.execute(
            """
            INSERT INTO content_files (
                uid,
                name,
                original_s3_file_id,
                original_sha512,
                duration_seconds,
                created_at
            )
            SELECT
                ?, ?, s3_files.id, ?, ?, ?
            FROM s3_files
            WHERE
                s3_files.uid = ?
                AND NOT EXISTS (
                    SELECT 1 FROM content_files AS cf
                    WHERE cf.original_sha512 = ?
                )
            """,
            (
                prepared.content_file_uid,
                prepared.name,
                prepared.original_sha512,
                prepared.duration_seconds,
                prepared.created_at,
                prepared.original.uid,
                prepared.original_sha512,
            ),
        )
        if response.rows_affected is None or response.rows_affected < 1:
            raise ProcessAudioAbortedException()

        await redis.zrem(
            "files:purgatory",
            json.dumps(
                {"bucket": prepared.original.bucket, "key": prepared.original.key},
                sort_keys=True,
            ),
        )
        content_file_uid = prepared.content_file_uid
        content_file_name = prepared.name
        original_s3_file_uid = prepared.original.uid
        original_s3_file_key = prepared.original.key
        original_s3_file_size = prepared.original.file_size
        original_s3_file_content_type = prepared.original.content_type
        original_s3_file_created_at = prepared.original.created_at

    exports: List[ContentFileExport] = []
    for mp4 in prepared.mp4s:
        response = await cursor.executemany3(
            (
                (
                    """
                    INSERT INTO content_file_exports (
                        uid, content_file_id, format, bandwidth,
                        codecs, target_duration, quality_parameters,
                        created_at
                    )
                    SELECT
                        ?, content_files.id, ?, ?,
                        ?, ?, ?,
                        ?
                    FROM content_files
                    WHERE
                        content_files.uid = ?
                        AND NOT EXISTS (
                            SELECT 1 FROM content_file_exports AS cfe
                            WHERE cfe.content_file_id = content_files.id
                            AND cfe.format = ?
                            AND cfe.quality_parameters = ?
                        )
                    """,
                    (
                        mp4.export.uid,
                        mp4.export.format,
                        mp4.export.bandwidth,
                        ",".join(sorted(mp4.export.codecs)),
                        mp4.export.target_duration,
                        json.dumps(mp4.export.quality_parameters, sort_keys=True),
                        mp4.export.created_at,
                        content_file_uid,
                        mp4.export.format,
                        json.dumps(mp4.export.quality_parameters, sort_keys=True),
                    ),
                ),
                (
                    """
                    INSERT INTO content_file_export_parts (
                        uid, content_file_export_id, s3_file_id,
                        position, duration_seconds, created_at
                    )
                    SELECT
                        ?, content_file_exports.id, s3_files.id,
                        ?, ?, ?
                    FROM content_file_exports, s3_files
                    WHERE
                        content_file_exports.uid = ?
                        AND s3_files.uid = ?
                    """,
                    (
                        mp4.export.parts[0].uid,
                        mp4.export.parts[0].position,
                        mp4.export.parts[0].duration_seconds,
                        mp4.export.parts[0].created_at,
                        mp4.export.uid,
                        mp4.export.parts[0].s3_file.uid,
                    ),
                ),
            )
        )
        if (
            response.items[0].rows_affected is None
            or response.items[0].rows_affected < 1
        ):
            assert (
                response.items[1].rows_affected is None
                or response.items[1].rows_affected < 1
            )
            continue

        assert (
            response.items[1].rows_affected is not None
            and response.items[1].rows_affected > 0
        )
        await redis.zrem(
            "files:purgatory",
            json.dumps(
                {
                    "bucket": mp4.export.parts[0].s3_file.bucket,
                    "key": mp4.export.parts[0].s3_file.key,
                },
                sort_keys=True,
            ),
        )
        exports.append(mp4.export)

    for vod in prepared.hls.vods:
        response = await cursor.executemany3(
            (
                (
                    """
                    INSERT INTO content_file_exports (
                        uid, content_file_id, format, bandwidth,
                        codecs, target_duration, quality_parameters,
                        created_at
                    )
                    SELECT
                        ?, content_files.id, ?, ?,
                        ?, ?, ?,
                        ?
                    FROM content_files
                    WHERE
                        content_files.uid = ?
                        AND NOT EXISTS (
                            SELECT 1 FROM content_file_exports AS cfe
                            WHERE cfe.content_file_id = content_files.id
                                AND cfe.format = ?
                                AND cfe.quality_parameters = ?
                        )
                    """,
                    (
                        vod.uid,
                        vod.format,
                        vod.bandwidth,
                        ",".join(sorted(vod.codecs)),
                        vod.target_duration,
                        json.dumps(vod.quality_parameters, sort_keys=True),
                        vod.created_at,
                        content_file_uid,
                        vod.format,
                        json.dumps(vod.quality_parameters, sort_keys=True),
                    ),
                ),
                *[
                    (
                        """
                        INSERT INTO content_file_export_parts (
                            uid, content_file_export_id, s3_file_id,
                            position, duration_seconds, created_at
                        )
                        SELECT
                            ?, content_file_exports.id, s3_files.id,
                            ?, ?, ?
                        FROM content_file_exports, s3_files
                        WHERE
                            content_file_exports.uid = ?
                            AND s3_files.uid = ?
                        """,
                        (
                            part.uid,
                            part.position,
                            part.duration_seconds,
                            part.created_at,
                            vod.uid,
                            part.s3_file.uid,
                        ),
                    )
                    for part in vod.parts
                ],
            )
        )
        if (
            response.items[0].rows_affected is None
            or response.items[0].rows_affected < 1
        ):
            for item in response.items[1:]:
                assert item.rows_affected is None or item.rows_affected < 1
            continue

        assert len(response.items) == len(vod.parts) + 1
        for item, part in zip(response.items[1:], vod.parts):
            assert item.rows_affected is not None and item.rows_affected > 0
            await redis.zrem(
                "files:purgatory",
                json.dumps(
                    {
                        "bucket": part.s3_file.bucket,
                        "key": part.s3_file.key,
                    },
                    sort_keys=True,
                ),
            )

        exports.append(vod)

    return ContentFile(
        uid=content_file_uid,
        name=content_file_name,
        original=S3File(
            uid=original_s3_file_uid,
            bucket=files.default_bucket,
            key=original_s3_file_key,
            file_size=original_s3_file_size,
            content_type=original_s3_file_content_type,
            created_at=original_s3_file_created_at,
        ),
        original_sha512=prepared.original_sha512,
        duration_seconds=prepared.duration_seconds,
        created_at=prepared.created_at,
        exports=exports,
    )


def produce_mp4(
    local_filepath: str, target_filepath: str, *, ffmpeg: str, bitrate_kbps: int
) -> None:
    """Attempts to produce an mp4 file from the given audio file. An exception
    is raised if the process fails.

    Args:
        local_filepath (str): The path to where the audio file to process is located.
        target_filepath (str): The path to where the mp4 file should be produced. Should
            include the file extension.
        ffmpeg (str): The path to the ffmpeg executable.
        bitrate_kbps (int): The bitrate to use for the mp4 file, in kilobits per second.
    """
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)

    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostats",
        "-i",
        local_filepath,
        "-vn",
        "-acodec",
        "aac",
        "-b:a",
        f"{bitrate_kbps}k",
        "-movflags",
        "faststart",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        target_filepath,
    ]

    logging.info(f"Running command: {json.dumps(cmd)}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise Exception(
            "ffmpeg failed\n\nstdout: ```\n"
            + result.stdout.decode("utf-8")
            + "\n```\n\nstderr: ```\n"
            + result.stderr.decode("utf-8")
            + "\n```"
        )

    cmd = [
        "ffprobe",
        "-v",
        "warning",
        "-show_entries",
        "stream=bit_rate,duration",
        target_filepath,
    ]
    logging.info(f"Running command: {json.dumps(cmd)}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise Exception(
            "ffprobe failed\n\nstdout: ```\n"
            + result.stdout.decode("utf-8")
            + "\n```\n\nstderr: ```\n"
            + result.stderr.decode("utf-8")
            + "\n```"
        )

    duration: Optional[float] = None
    bit_rate: Optional[int] = None
    decoded_out = result.stdout.decode("utf-8")
    for line in decoded_out.splitlines(keepends=False):
        if line.startswith("duration="):
            duration = float(line[len("duration=") :])
        elif line.startswith("bit_rate="):
            bit_rate = int(line[len("bit_rate=") :])

    if duration is None:
        raise Exception(f"ffprobe did not report duration: {decoded_out}")

    if bit_rate is None:
        raise Exception(f"ffprobe did not report bitrate: {decoded_out}")

    logging.info(f"Produced mp4 file: {target_filepath=} with {duration=}, {bit_rate=}")

    with open(f"{target_filepath}.json", "w") as f:
        json.dump(
            {
                "duration": duration,
                "bit_rate": bit_rate,
            },
            f,
            sort_keys=True,
            indent=2,
        )


async def produce_mp4_async(
    local_filepath: str,
    target_filepath: str,
    *,
    ffmpeg: str,
    bitrate_kbps: int,
    pool: multiprocessing.pool.Pool,
) -> None:
    """Async version of produce_mp4, using the given multiprocessing pool to
    produce the mp4 file.
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def _on_done(result):
        loop.call_soon_threadsafe(fut.set_result, result)

    def _on_error(err):
        loop.call_soon_threadsafe(fut.set_exception, err)

    pool.apply_async(
        produce_mp4,
        args=(local_filepath, target_filepath),
        kwds={"ffmpeg": ffmpeg, "bitrate_kbps": bitrate_kbps},
        callback=_on_done,
        error_callback=_on_error,
    )
    return await fut


def produce_m3u8(
    local_filepath: str,
    target_folder: str,
    *,
    ffmpeg: str,
) -> str:
    """Attempts to produce an m3u8 file from the given audio file. An exception
    is raised if the process fails. This produces multiple files; additional
    files will be placed adjacent to the target filepath, including the other
    playlists (m3u8), and the parts for those playlists (ts).

    Args:
        local_filepath (str): The path to where the audio file to process is located.
        target_folder (str): The folder where the output should be stored; must already
            exist and be empty.
        ffmpeg (str): The path to the ffmpeg executable.

    Returns:
        str: The path to the primary m3u8 file that was produced.
    """
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)

    audio_bitrates = AUDIO_BITRATES

    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostats",
        "-i",
        local_filepath,
        "-vn",
        "-hls_time",
        "10",
        "-hls_list_size",
        "0",
        "-hls_playlist_type",
        "vod",
        "-master_pl_name",
        "playlist.m3u8",
        "-f",
        "hls",
        *itertools.chain.from_iterable(
            (
                f"-b:a:{i}",
                f"{bitrate}k",
            )
            for i, bitrate in enumerate(audio_bitrates)
        ),
        *itertools.chain.from_iterable(("-map", f"0:a") for _ in audio_bitrates),
        "-var_stream_map",
        " ".join(f"a:{i}" for i, _ in enumerate(audio_bitrates)),
        os.path.join(target_folder, "vs_%v", "out.m3u8"),
    ]
    logging.info(f"Running command: {json.dumps(cmd)}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise Exception(
            "ffmpeg failed\n\nstdout: ```\n"
            + result.stdout.decode("utf-8")
            + "\n```\n\nstderr: ```\n"
            + result.stderr.decode("utf-8")
            + "\n```"
        )

    return os.path.join(target_folder, "playlist.m3u8")


async def produce_m3u8_async(
    local_filepath: str,
    target_folder: str,
    *,
    ffmpeg: str,
    pool: multiprocessing.pool.Pool,
) -> str:
    """Async version of produce_m3u8, using the given multiprocessing pool to
    produce the m3u8 file.
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def _on_done(result):
        loop.call_soon_threadsafe(fut.set_result, result)

    def _on_error(err):
        loop.call_soon_threadsafe(fut.set_exception, err)

    pool.apply_async(
        produce_m3u8,
        args=(local_filepath, target_folder),
        kwds={"ffmpeg": ffmpeg},
        callback=_on_done,
        error_callback=_on_error,
    )
    return await fut


if __name__ == "__main__":
    os.makedirs(os.path.join("tmp", "audio_test", "out"), exist_ok=True)
    for bitrate in AUDIO_BITRATES:
        produce_mp4(
            os.path.join("tmp", "audio_test", "file_example_WAV_10MG.wav"),
            os.path.join("tmp", "audio_test", "out", f"out_{bitrate}.mp4"),
            ffmpeg=shutil.which("ffmpeg"),
            bitrate_kbps=bitrate,
        )
    print(
        produce_m3u8(
            os.path.join("tmp", "audio_test", "file_example_WAV_10MG.wav"),
            os.path.join("tmp", "audio_test", "out"),
            ffmpeg=shutil.which("ffmpeg"),
        )
    )
