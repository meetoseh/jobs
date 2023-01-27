"""The first step in this pipeline where we crop the audio to the first 15 seconds,
downmix to a single audio channel, add a fade out, and export in wav LPCM format
using ffmpeg
"""
from typing import Optional
from shareables.journey_audio.settings import standard_audio_fade
from shareables.shareable_pipeline_exception import ShareablePipelineException
from dataclasses import dataclass
import subprocess
import logging
import shutil
import json
import numpy as np
import sys
import os
from temp_files import temp_file


class CropAndNormalizeError(ShareablePipelineException):
    def __init__(self, message: str):
        super().__init__(message, 1, "crop_and_normalize")


@dataclass
class CropAndNormalizeResult:
    """The result after cropping and normalizing an audio file for use in
    audio visualization.
    """

    path: str
    """The path to where the cropped and normalized audio file is located. This
    is always in a signed little-endian PCM format, though the bit depth may vary.
    """

    sample_rate: int
    """How many samples there are in a given second of audio. Typically, 44100
    """

    bits_per_sample: Optional[int]
    """How many bits were available per sample, if that's a sensible question from
    the original format (typically available as we want to use uncompressed wav files
    as the source)
    """

    stored_bit_depth: int
    """How many bits were used to store each sample."""

    dtype: np.dtype
    """The numpy datatype that can be used to load the audio file. This is typically
    platform-endian int32
    """


def crop_and_normalize(
    source_path: str, dest_path: str, duration: Optional[int] = None
) -> CropAndNormalizeResult:
    """Loads the audio file in the given source path, in any format that
    ffmpeg recognizes, crops it if requested, downmixes to a single channel,
    adds a fade out if cropping, and exports it to the given destination path in
    a simple PCM format.

    Args:
        source_path (str): Where the original audio file is located
        dest_path (str): Where to write the cropped, normalized audio file
            that will be used for audio visualization. Note that this audio
            file is not used directly as the audio in the final video.
        duration (int, None): If specified, the audio will be clipped to the
            specified duration. Otherwise the audio remains the original duration

    Returns:
        None: The audio is written to the destination path

    Raises:
        CropAndNormalizeException: If the source audio is not at least 15 seconds
            long in a format that ffmpeg recognizes
    """
    logging.info(f"crop_and_normalize({source_path=}, {dest_path=}, {duration=})")
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")

    cmd = [
        ffprobe,
        "-v",
        "warning",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=sample_rate,bits_per_sample",
        source_path,
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

    joined_output = result.stdout.decode("utf-8")
    output_lines = joined_output.split("\n")
    sample_rate: Optional[int] = None
    bits_per_sample: Optional[int] = None
    for line in output_lines:
        if not line or line[0] == "[":
            continue

        key, value = line.split("=")
        if key == "sample_rate":
            sample_rate = int(value)
        elif key == "bits_per_sample":
            bits_per_sample = int(value)

    if sample_rate is None:
        raise CropAndNormalizeError(
            f"Could not determine sample rate:\n{joined_output}"
        )

    initial_bit_depth = bits_per_sample or 24
    target_bit_depth = 32
    if bits_per_sample and bits_per_sample > 32:
        target_bit_depth = 64

    platform_is_big_endian = sys.byteorder == "big"
    endian_format_marker = "be" if platform_is_big_endian else "le"
    endian_np_marker = ">" if platform_is_big_endian else "<"
    initial_bytes_per_sample = initial_bit_depth // 8
    target_bytes_per_sample = target_bit_depth // 8

    format = f"u{initial_bit_depth}{endian_format_marker}"
    dtype = np.dtype(f"{endian_np_marker}u{target_bytes_per_sample}")

    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostats",
        *(["-t", str(duration)] if duration is not None else []),
        "-i",
        source_path,
        "-f",
        format,
        "-flags",
        "+bitexact",
        "-fflags",
        "+bitexact",
        "-map_metadata",
        "-1",
        "-vn",
        "-acodec",
        f"pcm_{format}",
        *(
            [
                "-af",
                f"afade={standard_audio_fade(duration)}",
            ]
            if duration is not None
            else []
        ),
        "-ac",
        "1",
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

    if initial_bit_depth != target_bit_depth:
        padding_bytes = target_bytes_per_sample - initial_bytes_per_sample
        assert (
            padding_bytes > 0
        ), f"cannot pad {initial_bytes_per_sample=} to {target_bytes_per_sample=}"

        logging.info(
            f"Converting to new bit depth by padding most significant bytes with 0s"
        )
        padding = b"\x00" * padding_bytes
        with temp_file() as intermediate_file:
            with open(dest_path, "rb") as source_file, open(
                intermediate_file, "wb"
            ) as dest_file:
                if platform_is_big_endian:
                    while True:
                        chunk = source_file.read(initial_bytes_per_sample)
                        if not chunk:
                            break
                        dest_file.write(padding)
                        dest_file.write(chunk)
                else:
                    while True:
                        chunk = source_file.read(initial_bytes_per_sample)
                        if not chunk:
                            break
                        dest_file.write(chunk)
                        dest_file.write(padding)

            os.unlink(dest_path)
            os.rename(intermediate_file, dest_path)

    logging.debug(f"crop_and_normalize() complete; {dest_path=} written")
    return CropAndNormalizeResult(
        path=dest_path,
        sample_rate=sample_rate,
        bits_per_sample=bits_per_sample,
        stored_bit_depth=target_bit_depth,
        dtype=dtype,
    )
