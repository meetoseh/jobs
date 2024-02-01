import asyncio
from fractions import Fraction
import json
import math
import subprocess
import time
from typing import List, Optional, Sequence, Tuple, cast
from content import (
    ContentFile,
    ContentFileExport,
    ContentFileExportPart,
    S3File,
    hash_content_sync,
)
from graceful_death import GracefulDeath
from images import determine_crop
from itgs import Itgs
from lib.codecs import determine_codecs_from_probe
from m3u8 import M3UPlaylist, M3UVod, M3UVodReference, parse_m3u_vod
from temp_files import temp_dir
from audio import (
    BasicContentFilePartInfo,
    _PreparedAudioContent,
    PreparedM3UPlaylist,
    PreparedM3UVodExport,
    PreparedM3UVodExportPart,
    PreparedMP4,
    _upload_all,
    _upsert_prepared,
)
from dataclasses import dataclass
import os
import shutil
import secrets
import logging
import logging.config
import yaml
import multiprocessing
import multiprocessing.pool


class ProcessVideoAbortedException(Exception):
    """Raised when a video processing job failed but it'll probably work if
    it's tried again on a fresh instance; typically this is from receiving
    a term signal
    """


@dataclass(frozen=True)
class VideoQuality:
    """Represents video quality settings that we attempt to encode at"""

    width: int
    """The width of the video, in pixels"""
    height: int
    """The height of the video, in pixels"""
    audio_bitrate: int
    """The audio bitrate, in kbps"""
    video_bitrate: int
    """The video bitrate, in kbps"""


INSTAGRAM_VERTICAL: Sequence[VideoQuality] = (
    VideoQuality(width=1080, height=1920, audio_bitrate=384, video_bitrate=10486),
)
"""Video qualities appropriate for instagram stories"""

DESKTOP_LANDSCAPE: Sequence[VideoQuality] = (
    VideoQuality(width=1920, height=1080, audio_bitrate=384, video_bitrate=10486),
    VideoQuality(width=1280, height=720, audio_bitrate=384, video_bitrate=3456),
)
"""Video qualities appropriate for landscape desktop viewing"""


async def process_video(
    local_filepath: str,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    max_file_size: int,
    name_hint: str,
    exports: Sequence[VideoQuality],
) -> ContentFile:
    """Performs our standard video processing on the video at the given
    local filepath, then uploads it to S3 and stores it in our database
    as a new content file, returning the new content file.

    If the given file has already been processed, as dictated by it's sha512
    hash, then any missing exports are produced and **only those newly produced
    exports are returned**. Use content.get_content_file to get the full
    content file.

    Args:
        local_filepath (str): The local filepath of the video file to process.
        itgs (Itgs): the integration to use for networked services
        gd (GracefulDeath): the signal tracker
        max_file_size (int): The maximum size of the content file, in bytes.
        name_hint (str): A hint for the name of the content file.
        exports (List[VideoQuality]): The qualities to encode at.

    Raises:
        ProcessVideoAbortedException: if the process was aborted but it'll
            probably work if it's retried on a fresh instance; typically this
            is from receiving a term signal
    """
    ffmpeg = shutil.which("ffmpeg")
    assert ffmpeg is not None, "ffmpeg not found"
    ffprobe = shutil.which("ffprobe")
    assert ffprobe is not None, "ffprobe not found"

    file_size = os.path.getsize(local_filepath)
    if file_size > max_file_size:
        raise ValueError(f"{file_size=} exceeds {max_file_size=}")

    name = name_hint[:64] + "-" + secrets.token_urlsafe(16)

    with temp_dir() as temp_folder:
        return await process_video_into(
            local_filepath,
            itgs=itgs,
            gd=gd,
            temp_folder=temp_folder,
            name=name,
            exports=exports,
            ffmpeg=ffmpeg,
            ffprobe=ffprobe,
        )


async def process_video_into(
    local_filepath: str,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    temp_folder: str,
    name: str,
    exports: Sequence[VideoQuality],
    ffmpeg: str,
    ffprobe: str,
) -> ContentFile:
    """Processes the video file at the given local filepath into the standard
    exports, using the specified temporary folder. The produced content file is
    given the specified name if it does not already exist. If the file has already
    been processed, as dictated by it's sha512 hash, then any missing exports are
    produced and the returned content file **ONLY HAS THE NEWLY PRODUCED EXPORTS**.

    PERF:
        This currently always produces all of the exports, even if they already
        exist, but will delete any that are not needed. This is just for simplicity
        and can be swapped out later if it becomes a performance issue.

    Args:
        local_filepath (str): The path to where the video file to process is located.
        itgs (Itgs): the integration to use for networked services
        gd (GracefulDeath): the signal tracker
        temp_folder (str): A folder to use for temporary files. This will not be
            cleaned up.
        name (str): The name of the content file to produce.
        exports (List[VideoQuality]): The qualities to encode at.
        ffmpeg (str): The path to the ffmpeg executable.
        ffprobe (str): The path to the ffprobe executable.
    """
    mp4_folder = os.path.join(temp_folder, "mp4")
    os.makedirs(mp4_folder, exist_ok=True)

    hls_folder = os.path.join(temp_folder, "hls")
    os.makedirs(hls_folder, exist_ok=True)

    original_sha512 = hash_content_sync(local_filepath)

    mp4s: List[Tuple[str, BasicContentFilePartInfo]] = []
    hls: List[Tuple[str, M3UVod]] = []
    for export in exports:
        iden = f"{export.width}x{export.height}-{export.video_bitrate}-{export.audio_bitrate}-{secrets.token_hex(8)}"

        mp4_path = os.path.join(mp4_folder, f"{iden}.mp4")
        mp4s.append(
            (mp4_path, _encode_video(local_filepath, mp4_path, export, ffmpeg, ffprobe))
        )
        if gd.received_term_signal:
            raise ProcessVideoAbortedException()

        export_hls_folder = os.path.join(hls_folder, iden)
        os.makedirs(export_hls_folder, exist_ok=True)
        export_hls_m3u8 = os.path.join(export_hls_folder, "playlist.m3u8")
        _encode_hls_video(mp4_path, export_hls_m3u8, ffmpeg, ffprobe)
        if gd.received_term_signal:
            raise ProcessVideoAbortedException()

        hls_vod = await parse_m3u_vod(export_hls_m3u8)
        if gd.received_term_signal:
            raise ProcessVideoAbortedException()

        hls.append((export_hls_m3u8, hls_vod))

    master_playlist_filepath = os.path.join(hls_folder, "master.m3u8")
    master_playlist = _create_master_playlist(hls, master_playlist_filepath, ffprobe)

    now = time.time()
    files = await itgs.files()
    content_file_uid = f"oseh_cf_{secrets.token_urlsafe(16)}"
    prepared = _PreparedAudioContent(
        content_file_uid=content_file_uid,
        original_filepath=local_filepath,
        original=S3File(
            uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
            bucket=files.default_bucket,
            key=f"s3_files/videos/originals/{content_file_uid}/{name}",
            file_size=os.path.getsize(local_filepath),
            content_type="application/octet-stream",
            created_at=now,
        ),
        original_sha512=original_sha512,
        name=name,
        duration_seconds=mp4s[0][1].duration,
        created_at=now,
        mp4s=[
            PreparedMP4(
                local_filepath=mp4_path,
                export=ContentFileExport(
                    uid=f"oseh_cfe_{secrets.token_urlsafe(16)}",
                    format="mp4",
                    format_parameters={
                        "width": settings.width,
                        "height": settings.height,
                    },
                    bandwidth=mp4.bit_rate,
                    codecs=mp4.codecs,
                    target_duration=math.ceil(mp4.duration),
                    quality_parameters={
                        "bitrate_bps": mp4.bit_rate,
                        "faststart": True,
                        "target_audio_bitrate_kbps": settings.audio_bitrate,
                        "target_video_bitrate_kbps": settings.video_bitrate,
                    },
                    created_at=now,
                    parts=[
                        ContentFileExportPart(
                            uid=f"oseh_cfep_{secrets.token_urlsafe(16)}",
                            s3_file=S3File(
                                uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
                                bucket=files.default_bucket,
                                key=f"s3_files/videos/{content_file_uid}/mp4/{mp4.bit_rate}/{secrets.token_urlsafe(8)}.mp4",
                                file_size=os.path.getsize(mp4_path),
                                content_type="video/mp4",
                                created_at=now,
                            ),
                            position=0,
                            duration_seconds=mp4.duration,
                            created_at=now,
                        )
                    ],
                ),
            )
            for settings, (mp4_path, mp4) in zip(exports, mp4s)
        ],
        hls=PreparedM3UPlaylist(
            master_file_path=master_playlist_filepath,
            playlist=master_playlist,
            vods=[
                PreparedM3UVodExport(
                    uid=f"oseh_cfe_{secrets.token_urlsafe(16)}",
                    format="m3u8",
                    format_parameters={
                        "average_bandwidth": vod.average_bandwidth,
                        "width": export.width,
                        "height": export.height,
                    },
                    bandwidth=vod.bandwidth,
                    codecs=vod.codecs,
                    target_duration=vod.vod.target_duration,
                    quality_parameters={
                        "target_audio_bitrate_kbps": export.audio_bitrate,
                        "target_video_bitrate_kbps": export.video_bitrate,
                    },
                    created_at=now,
                    parts=[
                        PreparedM3UVodExportPart(
                            uid=f"oseh_cfep_{secrets.token_urlsafe(16)}",
                            s3_file=S3File(
                                uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
                                bucket=files.default_bucket,
                                key=f"s3_files/videos/{content_file_uid}/hls/{secrets.token_urlsafe(8)}.ts",
                                file_size=os.path.getsize(
                                    os.path.join(
                                        os.path.dirname(master_playlist_filepath),
                                        os.path.dirname(vod.path),
                                        part.path,
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
                for (vod, export) in zip(master_playlist.vods, exports)
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


def _determine_video_filters_for_sizing(
    src: str, target_width: int, target_height: int, ffprobe: str
) -> List[str]:
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "v",
        "-show_entries",
        "stream=width,height",
        "-of",
        "json",
        src,
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

    result_json = json.loads(result.stdout.decode("utf-8"))
    width = result_json["streams"][0]["width"]
    height = result_json["streams"][0]["height"]

    assert isinstance(width, int)
    assert isinstance(height, int)

    if width < target_width or height < target_height:
        raise ValueError(
            f"Cannot encode {src=} at {target_width}x{target_height} because the source is {width}x{height}"
        )

    crops = determine_crop((width, height), (target_width, target_height), (0.5, 0.5))
    (top_crop, right_crop, bot_crop, left_crop) = crops
    width_after_crop = width - right_crop - left_crop
    height_after_crop = height - top_crop - bot_crop

    video_filters: List[str] = []
    if any(c != 0 for c in crops):
        video_filters.append(
            f"crop={width_after_crop}:{height_after_crop}:{left_crop}:{top_crop}"
        )

    if (width_after_crop, height_after_crop) != (target_width, target_height):
        video_filters.append(
            f"scale='w={target_width}:h={target_height}:sws_flags=lanczos'"
        )

    video_filters.append("setsar=1/1")
    return video_filters


def _encode_video(
    src: str, dest: str, quality: VideoQuality, ffmpeg: str, ffprobe: str
) -> BasicContentFilePartInfo:
    """Encodes the video at the given source path to the given destination path
    using the given quality settings, fetching the resulting video info using
    ffprobe.

    Args:
        src (str): The source path.
        dest (str): The destination path.
        quality (VideoQuality): The quality settings to encode at.
        ffmpeg (str): The path to the ffmpeg executable.
        ffprobe (str): The path to the ffprobe executable.

    Returns:
        _Mp4Info: The info of the encoded video.
    """
    video_filters = _determine_video_filters_for_sizing(
        src, quality.width, quality.height, ffprobe
    )

    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostats",
        "-i",
        src,
        "-codec:v",
        "h264",
        "-codec:a",
        "aac",
        "-b:v",
        f"{quality.video_bitrate}k",
        "-b:a",
        f"{quality.audio_bitrate}k",
        "-vf",
        ",".join(video_filters),
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-movflags",
        "+faststart",
        dest,
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

    return get_video_mp4_info(dest, ffprobe)


def get_video_mp4_info(path: str, ffprobe: str) -> BasicContentFilePartInfo:
    """Determines the mp4 info of the video at the given path using ffprobe."""
    cmd = [
        ffprobe,
        "-v",
        "warning",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        path,
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

    info = json.loads(result.stdout.decode("utf-8"))
    codecs = determine_codecs_from_probe(info)
    return BasicContentFilePartInfo(
        duration=float(info["format"]["duration"]),
        bit_rate=int(info["format"]["bit_rate"]),
        codecs=codecs,
    )


def _encode_hls_video(src: str, dest: str, ffmpeg: str, ffprobe: str) -> None:
    """Writes the video at the given source path into an HLS VOD file
    whose master is at the given destination and whose segments are
    stored in the same directory as the destination.

    This simply copies the video and audio codecs from the source file,
    and thus should be passed an already encoded video.
    """

    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostats",
        "-i",
        src,
        "-c:a",
        "copy",
        "-c:v",
        "copy",
        "-start_number",
        "0",
        "-hls_time",
        "5",
        "-hls_list_size",
        "0",
        "-f",
        "hls",
        dest,
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


@dataclass
class VideoSegmentInfo:
    bitrate: int
    size_bytes: int
    resolution: Tuple[int, int]
    duration: float
    video_codec: str
    audio_codec: str


def get_clean_video_segment_info(path: str, ffprobe: str) -> VideoSegmentInfo:
    """Determines the resolution of the given video file at the given path.
    Only works on clean video files, i.e., 2 streams, one of which is video
    and the other is audio.

    This is intended to work for mp4s and HLS ts segment files
    """
    cmd = [
        ffprobe,
        "-v",
        "warning",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        path,
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

    info = json.loads(result.stdout.decode("utf-8"))
    logging.debug(f"ffprobe info for segment at {path=}: {info}")

    assert len(info["streams"]) == 2, info
    video_stream = info["streams"][0]
    audio_stream = info["streams"][1]

    if video_stream["codec_type"] != "video":
        video_stream, audio_stream = audio_stream, video_stream

    assert video_stream["codec_type"] == "video"
    assert audio_stream["codec_type"] == "audio"

    bitrate = int(info["format"]["bit_rate"])
    size_bytes = int(info["format"]["size"])
    duration = float(info["format"]["duration"])
    resolution = (int(video_stream["width"]), int(video_stream["height"]))

    codecs = determine_codecs_from_probe(info)
    video_codec = codecs[0]
    audio_codec = codecs[1]

    if not audio_codec.startswith("mp4a"):
        video_codec, audio_codec = audio_codec, video_codec

    assert audio_codec.startswith("mp4a")
    assert video_codec.startswith("avc1")

    return VideoSegmentInfo(
        bitrate=bitrate,
        size_bytes=size_bytes,
        resolution=resolution,
        duration=duration,
        video_codec=video_codec,
        audio_codec=audio_codec,
    )


async def get_clean_video_segment_info_async(
    path: str, ffprobe: str, pool: multiprocessing.pool.Pool
) -> VideoSegmentInfo:
    """Executes get_clean_video_segment_info in a separate process to avoid
    blocking the event loop, and returns the result.
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def _on_done(result):
        loop.call_soon_threadsafe(fut.set_result, result)

    def _on_error(err):
        loop.call_soon_threadsafe(fut.set_exception, err)

    pool.apply_async(
        get_clean_video_segment_info,
        args=(path, ffprobe),
        callback=_on_done,
        error_callback=_on_error,
    )
    return await fut


def _create_master_playlist(
    vods: List[Tuple[str, M3UVod]], dest: str, ffprobe: str
) -> M3UPlaylist:
    """Creates a master playlist m3u8 file at the given destination by combining
    the vods specified as (filepath, info) tuples, and returns the playlist contained
    in that file.

    The file itself is mostly for debugging purposes, but it's pretty small so
    there isn't much overhead.
    """

    vod_references: List[M3UVodReference] = []
    for vod_m3u8_path, vod_info in vods:
        vod_m3u8_dir = os.path.dirname(vod_m3u8_path)
        highest_bitrate = 0
        total_size_bytes = 0
        total_duration_seconds = 0
        resolution: Optional[Tuple[int, int]] = None
        video_codec: Optional[str] = None
        audio_codec: Optional[str] = None
        for content in vod_info.content:
            content_path = os.path.join(vod_m3u8_dir, content.path)
            content_info = get_clean_video_segment_info(content_path, ffprobe)
            total_size_bytes += content_info.size_bytes
            total_duration_seconds += content_info.duration
            highest_bitrate = max(highest_bitrate, content_info.bitrate)

            if resolution is None:
                resolution = content_info.resolution
            else:
                assert (
                    resolution == content_info.resolution
                ), f"{resolution=} != {content_info.resolution=} for {content_path=}"

            if video_codec is None:
                video_codec = content_info.video_codec
            else:
                assert (
                    video_codec == content_info.video_codec
                ), f"{video_codec=} != {content_info.video_codec=} for {content_path=}"

            if audio_codec is None:
                audio_codec = content_info.audio_codec
            else:
                assert (
                    audio_codec == content_info.audio_codec
                ), f"{audio_codec=} != {content_info.audio_codec=} for {content_path=}"

        average_bitrate = round((total_size_bytes * 8) / total_duration_seconds)
        assert audio_codec is not None
        assert video_codec is not None
        vod_references.append(
            M3UVodReference(
                bandwidth=highest_bitrate,
                average_bandwidth=average_bitrate,
                resolution=resolution,
                codecs=[video_codec, audio_codec],
                claims=[],
                path=os.path.relpath(vod_m3u8_path, os.path.dirname(dest)),
                vod=vod_info,
            )
        )

    with open(dest, "w", newline="\n") as out:
        out.write(f"#EXTM3U\n")

        for vod_reference in vod_references:
            out.write(f"#EXT-X-STREAM-INF:BANDWIDTH={vod_reference.bandwidth}")
            if vod_reference.average_bandwidth is not None:
                out.write(f",AVERAGE-BANDWIDTH={vod_reference.average_bandwidth}")
            if vod_reference.resolution is not None:
                out.write(
                    f",RESOLUTION={vod_reference.resolution[0]}x{vod_reference.resolution[1]}"
                )
            out.write(f",CODECS=\"{','.join(vod_reference.codecs)}\"\n")
            out.write(f"{vod_reference.path.replace(os.path.sep, '/')}\n")

    return M3UPlaylist(
        claims=dict(),
        vods=vod_references,
    )


@dataclass
class VideoGenericInfo:
    framerate: Fraction
    """The framerate of the video, expressed as an exact fraction"""
    duration: float
    """The duration of the video in seconds, approximate"""
    width: int
    """The width of the video in pixels"""
    height: int
    """The height of the video in pixels"""
    bit_rate: int
    """The average bitrate of the video in bits per second"""
    n_frames: int
    """The number of frames in the video"""


def get_video_generic_info(path: str) -> VideoGenericInfo:
    ffprobe = shutil.which("ffprobe")
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "v",
        "-count_frames",
        "-show_entries",
        "stream=nb_read_frames,r_frame_rate,width,height",
        "-show_format",
        "-print_format",
        "json",
        path,
    ]
    logging.debug(f"Running command: {json.dumps(cmd)}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise Exception(
            "ffprobe failed\n\nstdout: ```\n"
            + result.stdout.decode("utf-8")
            + "\n```\n\nstderr: ```\n"
            + result.stderr.decode("utf-8")
            + "\n```"
        )

    result_json = json.loads(result.stdout.decode("utf-8"))
    stream = next(
        (
            stream
            for stream in result_json["streams"]
            if isinstance(stream["nb_read_frames"], int)
        ),
        result_json["streams"][0],
    )
    n_frames = stream["nb_read_frames"]

    if not isinstance(n_frames, int):
        try:
            n_frames = int(n_frames)
        except ValueError:
            raise ValueError(
                f"ffprobe returned a non-integer for {n_frames=} on {path=}: {result.stdout=}"
            )

    if n_frames <= 0:
        raise ValueError(
            f"ffprobe returned a non-positive for {n_frames=} on {path=}: {result.stdout=}"
        )

    duration = float(result_json["format"]["duration"])
    if duration <= 0:
        raise ValueError(
            f"ffprobe returned a non-positive for {duration=} on {path=}: {result.stdout=}"
        )

    width = int(stream["width"])
    if width <= 0:
        raise ValueError(
            f"ffprobe returned a non-positive for {width=} on {path=}: {result.stdout=}"
        )

    height = int(stream["height"])
    if height <= 0:
        raise ValueError(
            f"ffprobe returned a non-positive for {height=} on {path=}: {result.stdout=}"
        )

    bit_rate = int(result_json["format"]["bit_rate"])
    if bit_rate <= 0:
        raise ValueError(
            f"ffprobe returned a non-positive for {bit_rate=} on {path=}: {result.stdout=}"
        )

    framerate = Fraction(stream["r_frame_rate"])
    if framerate <= 0:
        raise ValueError(
            f"ffprobe returned a non-positive for {framerate=} on {path=}: {result.stdout=}"
        )

    result = VideoGenericInfo(
        framerate=framerate,
        duration=duration,
        width=width,
        height=height,
        bit_rate=bit_rate,
        n_frames=n_frames,
    )
    logging.debug(f"Video info for {path=}: {result=}")
    return result


if __name__ == "__main__":

    async def main():
        gd = GracefulDeath()

        with open("logging.yaml", "r") as f:
            logging_config = yaml.safe_load(f)

        logging.config.dictConfig(logging_config)

        async with Itgs() as itgs:
            with temp_dir() as temp_folder:
                await process_video_into(
                    local_filepath="tmp/test-large-video.mp4",
                    itgs=itgs,
                    gd=gd,
                    temp_folder=temp_folder,
                    name="test-video",
                    exports=[*INSTAGRAM_VERTICAL, *DESKTOP_LANDSCAPE],
                    ffmpeg=cast(str, shutil.which("ffmpeg")),
                    ffprobe=cast(str, shutil.which("ffprobe")),
                )

    asyncio.run(main())
