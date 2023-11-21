from fractions import Fraction
import json
import subprocess
import time
from typing import List, Sequence, Tuple
from content import (
    ContentFile,
    ContentFileExport,
    ContentFileExportPart,
    S3File,
    hash_content_sync,
)
from graceful_death import GracefulDeath
from itgs import Itgs
from temp_files import temp_dir
from audio import (
    _Mp4Info,
    _PreparedAudioContent,
    _PreparedMP4,
    _upload_all,
    _upsert_prepared,
)
from dataclasses import dataclass
import os
import shutil
import secrets
import logging


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
"""The bitrates we encode at"""


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

    original_sha512 = hash_content_sync(local_filepath)

    mp4s: List[Tuple[str, _Mp4Info]] = []
    for export in exports:
        mp4_path = os.path.join(
            mp4_folder, f"{export.width}x{export.height}-{secrets.token_hex(8)}.mp4"
        )
        mp4s.append(
            (mp4_path, _encode_video(local_filepath, mp4_path, export, ffmpeg, ffprobe))
        )
        if gd.received_term_signal:
            raise ProcessVideoAbortedException()

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
            _PreparedMP4(
                local_filepath=mp4_path,
                export=ContentFileExport(
                    uid=f"oseh_cfe_{secrets.token_urlsafe(16)}",
                    format="mp4",
                    bandwidth=mp4.bit_rate,
                    codecs=["aac"],
                    target_duration=int(mp4.duration),
                    quality_parameters={
                        "bitrate_bps": mp4.bit_rate,
                        "faststart": True,
                        "width": settings.width,
                        "height": settings.height,
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
        hls=None,
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


def _encode_video(
    src: str, dest: str, quality: VideoQuality, ffmpeg: str, ffprobe: str
) -> _Mp4Info:
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
        f"scale='w={quality.width}:h={quality.height}:sws_flags=lanczos',setsar=1/1",
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

    cmd = [
        "ffprobe",
        "-v",
        "warning",
        "-print_format",
        "json",
        "-show_format",
        dest,
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
    return _Mp4Info(
        duration=float(info["format"]["duration"]),
        bit_rate=int(info["format"]["bit_rate"]),
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
