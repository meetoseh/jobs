"""Adds the audio to the video"""
import logging
import shutil
import subprocess
import json
from typing import Optional

from shareables.journey_audio.settings import standard_audio_fade


def add_audio(
    video_path: str, audio_path: str, dest_path: str, duration: Optional[int]
) -> None:
    """Produces a new video with the audio added to it. If a duration is specified,
    the video will be clipped to that duration and a fade out will be applied.

    Args:
        video_path (str): Where the video file is located
        audio_path (str): Where the audio file is located
        dest_path (str): Where to write the video with the audio added
        duration (int, None): If specified, the video will be clipped to the
            specified duration. Otherwise the video remains the original duration

    Returns:
        None: The video is written to the destination path
    """
    logging.info(f"add_audio({video_path=}, {audio_path=}, {dest_path=})")
    ffmpeg = shutil.which("ffmpeg")

    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostats",
        *(["-t", str(duration)] if duration is not None else []),
        "-i",
        video_path,
        *(["-t", str(duration)] if duration is not None else []),
        "-i",
        audio_path,
        "-c:v",
        "copy",
        "-acodec",
        "aac",
        "-b:v",
        "2M",
        "-b:a",
        "1411k",
        *(
            ["-af", f"afade={standard_audio_fade(duration)}"]
            if duration is not None
            else []
        ),
        "-map_metadata",
        "-1",
        dest_path,
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

    logging.info(f"add_audio complete; {dest_path=} written")
