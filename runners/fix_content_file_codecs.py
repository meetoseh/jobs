"""Determines the real codecs and format parameters for the given content file export"""

import json
import math
import multiprocessing
import os
import shutil
from typing import Any, Dict, List, Optional, Tuple, cast

import aiofiles
import yaml
from audio import BasicContentFilePartInfo
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
from dataclasses import dataclass
import logging
import logging.config
from lib.asyncutils.constrained_amap import constrained_amap

from temp_files import temp_dir
from videos import (
    VideoSegmentInfo,
    get_clean_video_segment_info_async,
    get_video_mp4_info,
)

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, content_file_export_uid: str):
    """Inspects the actual export parts for the given content file export to determine
    the correct codecs and format parameters. Also double checks the part durations
    and sizes are accurate.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        content_file_export_uid (str): the uid of the content file export to fix
    """

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.executeunified3(
        (
            (
                """
                SELECT
                    format,
                    format_parameters,
                    bandwidth,
                    target_duration,
                    quality_parameters
                FROM content_file_exports
                WHERE uid = ?
                """,
                (content_file_export_uid,),
            ),
            (
                """
                SELECT
                    content_file_export_parts.uid,
                    s3_files.key,
                    s3_files.file_size,
                    content_file_export_parts.position,
                    content_file_export_parts.duration_seconds
                FROM content_file_export_parts, content_file_exports, s3_files
                WHERE
                    content_file_exports.uid = ?
                    AND content_file_exports.id = content_file_export_parts.content_file_export_id
                    AND s3_files.id = content_file_export_parts.s3_file_id
                ORDER BY content_file_export_parts.position
                """,
                (content_file_export_uid,),
            ),
        )
    )

    if not response[0].results:
        raise ValueError(f"Content file export {content_file_export_uid} not found")

    if not response[1].results:
        raise ValueError(f"Content file export {content_file_export_uid} has no parts")

    original_format = cast(str, response[0].results[0][0])
    original_format_parameters = cast(
        Dict[str, Any], json.loads(cast(str, response[0].results[0][1]))
    )
    original_bandwidth = cast(int, response[0].results[0][2])
    original_target_duration = cast(int, response[0].results[0][3])
    original_quality_parameters = cast(
        Dict[str, Any], json.loads(cast(str, response[0].results[0][4]))
    )

    parts: List[_CFPart] = []
    for row in response[1].results:
        parts.append(_CFPart(*row))

    logging.info(
        f"Updating content file export {content_file_export_uid} with {len(parts)} parts; originally:\n"
        f"  format: {original_format}\n"
        f"  format_parameters: {original_format_parameters}\n"
        f"  bandwidth: {original_bandwidth}\n"
        f"  target_duration: {original_target_duration}\n"
        f"  quality_parameters: {original_quality_parameters}"
    )

    assert original_format in ("mp4", "m3u8"), original_format

    if original_format == "mp4":
        assert len(parts) == 1, parts

    ffprobe = shutil.which("ffprobe")
    assert ffprobe is not None, "ffprobe not found in PATH"

    files = await itgs.files()
    with temp_dir() as folder, multiprocessing.Pool(
        processes=max((os.cpu_count() or 2) // 2, 1), initializer=setup_logging
    ) as pool:

        async def download_part(
            part: _CFPart,
        ) -> Tuple[_CFPart, BasicContentFilePartInfo, Optional[VideoSegmentInfo]]:
            part_path = os.path.join(folder, str(part.position))
            async with aiofiles.open(part_path, "wb") as out:
                await files.download(
                    out, bucket=files.default_bucket, key=part.s3_key, sync=False
                )

            basic_info = get_video_mp4_info(part_path, ffprobe)
            if len(basic_info.codecs) == 1:
                assert basic_info.codecs[0].startswith("mp4a")
                return part, basic_info, None

            info = await get_clean_video_segment_info_async(part_path, ffprobe, pool)
            return part, basic_info, info

        total_real_file_size = 0
        total_real_duration = 0
        real_target_duration = 0
        real_max_bandwidth = 0
        audio_codec: Optional[str] = None
        video_codec: Optional[str] = None
        resolution: Optional[Tuple[int, int]] = None

        async for part, basic_info, info in constrained_amap(
            iter(parts), download_part, max_concurrency=5, individual_timeout=20
        ):
            logging.debug(
                f"For part {part.position} ({part.uid}), got info: {basic_info} (detailed: {info})"
            )
            part_path = os.path.join(folder, str(part.position))
            real_part_file_size = os.path.getsize(part_path)

            if part.file_size != real_part_file_size:
                logging.warning(
                    f"Part {part.position} ({part.uid}) has incorrect file size: "
                    f"expected {part.file_size}, got {real_part_file_size}"
                )

            if abs(part.duration_seconds - basic_info.duration) > 0.01:
                logging.warning(
                    f"Part {part.position} ({part.uid}) has incorrect duration: "
                    f"expected {part.duration_seconds}, got {basic_info.duration}"
                )

            if info is not None:
                if video_codec is None:
                    video_codec = info.video_codec
                else:
                    assert video_codec == info.video_codec, (
                        video_codec,
                        info.video_codec,
                    )

                if audio_codec is None:
                    audio_codec = info.audio_codec
                else:
                    assert audio_codec == info.audio_codec, (
                        audio_codec,
                        info.audio_codec,
                    )

                if resolution is None:
                    resolution = info.resolution
                else:
                    assert resolution == info.resolution, (resolution, info.resolution)
            else:
                assert len(basic_info.codecs) == 1
                if audio_codec is None:
                    audio_codec = basic_info.codecs[0]
                else:
                    assert audio_codec == basic_info.codecs[0], (
                        audio_codec,
                        basic_info.codecs[0],
                    )

            ceiled_duration = math.ceil(basic_info.duration)
            if real_target_duration < ceiled_duration:
                real_target_duration = ceiled_duration

            part_bandwidth = int((real_part_file_size * 8) / basic_info.duration)
            if real_max_bandwidth < part_bandwidth:
                real_max_bandwidth = part_bandwidth

            total_real_file_size += real_part_file_size
            total_real_duration += basic_info.duration

        assert audio_codec is not None

        average_bandwidth = int((total_real_file_size * 8) / total_real_duration)

        if resolution is not None:
            old_width: Optional[int] = original_quality_parameters.get("width")
            if not isinstance(old_width, (int, type(None))) or (
                old_width is not None and old_width != resolution[0]
            ):
                logging.warning(
                    f"Old width {old_width} does not match new width {resolution[0]}"
                )

            old_height: Optional[int] = original_quality_parameters.get("height")
            if not isinstance(old_height, (int, type(None))) or (
                old_height is not None and old_height != resolution[1]
            ):
                logging.warning(
                    f"Old height {old_height} does not match new height {resolution[1]}"
                )

        if real_max_bandwidth != original_bandwidth:
            logging.warning(
                f"Old bandwidth {original_bandwidth} does not match new bandwidth {real_max_bandwidth}"
            )

        if real_target_duration != original_target_duration:
            logging.warning(
                f"Old target duration {original_target_duration} does not match new target duration {real_target_duration}"
            )

        new_quality_parameters = dict(original_quality_parameters)
        new_quality_parameters.pop("width", None)
        new_quality_parameters.pop("height", None)

        await cursor.execute(
            """
            UPDATE content_file_exports
            SET
                format_parameters=?,
                bandwidth=?,
                codecs=?,
                target_duration=?,
                quality_parameters=?
            WHERE uid=?
            """,
            (
                json.dumps(
                    {
                        "average_bandwidth": average_bandwidth,
                        **(
                            {}
                            if resolution is None
                            else {
                                "width": resolution[0],
                                "height": resolution[1],
                            }
                        ),
                    },
                    sort_keys=True,
                ),
                real_max_bandwidth,
                ",".join(
                    sorted([c for c in (audio_codec, video_codec) if c is not None])
                ),
                real_target_duration,
                json.dumps(new_quality_parameters, sort_keys=True),
                content_file_export_uid,
            ),
        )


def setup_logging():
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)


@dataclass
class _CFPart:
    uid: str
    s3_key: str
    file_size: int
    position: int
    duration_seconds: int


if __name__ == "__main__":
    import asyncio

    async def main():
        content_file_export_uid = input("Content file export uid: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.fix_content_file_codecs",
                content_file_export_uid=content_file_export_uid,
            )

    asyncio.run(main())
