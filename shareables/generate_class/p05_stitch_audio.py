import json
import logging
import os
import secrets
from typing import List
from itgs import Itgs
from shareables.generate_class.p03_create_class_meta import GeneratedClassMeta
from shareables.generate_class.p04_generate_audio_parts import (
    GeneratedClassWithDisjointAudio,
)
from dataclasses import dataclass
import shutil
import subprocess


@dataclass
class GeneratedClassWithJustVoice:
    meta: GeneratedClassMeta
    """The metadata for the class"""
    voice_audio: str
    """The path to the voice audio for the class, which needs background sound"""


@dataclass
class AudioInfo:
    duration: float


def get_audio_info(path: str) -> AudioInfo:
    """Gets basic information about an audio file"""
    ffprobe = shutil.which("ffprobe")
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "a",
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
    duration = float(result_json["format"]["duration"])
    return AudioInfo(duration=duration)


def pad_start_with_silence(src: str, *, duration: float, out: str) -> None:
    """Pads the start of the given audio file with silence for the given duration
    and stores it in the given output file.
    """
    ffmpeg = shutil.which("ffmpeg")
    cmd = [ffmpeg, "-i", src, "-af", f"adelay={int(duration*1000)}", out]
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


def pad_end_with_silence(src: str, *, duration: float, out: str) -> None:
    """Pads the end of the given audio file with silence for the given duration
    and stores it in the given output file.
    """
    ffmpeg = shutil.which("ffmpeg")
    cmd = [
        ffmpeg,
        "-i",
        src,
        "-af",
        f"apad=pad_dur={duration:.3f}",
        out,
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


def concat_audio_files(paths: List[str], *, out: str, folder: str) -> None:
    """Concatenates the audio files at the given locations into a single audio
    file at the given output location. The files are concatenated in the order
    they are given, with no cross fade or other effects. This may produce
    additional files in the given folder, which should be cleaned up after
    this function is called.

    This has no special handling for a single audio file, raises an exception
    with no audio files, and produces a format based on the extension of the
    output file.

    Args:
        paths (list[str]): The paths to the audio files to concatenate
        out (str): The path to the output file
        folder (str): The folder to store temporary files in
    """
    if not paths:
        raise Exception("Cannot concatenate an empty list of audio files")

    concat_list = os.path.join(folder, secrets.token_urlsafe(16) + ".txt")
    with open(concat_list, "w") as f:
        for path in paths:
            f.write(f"file '{os.path.basename(path)}'\n")

    ffmpeg = shutil.which("ffmpeg")
    cmd = [
        ffmpeg,
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        concat_list,
        "-c",
        "copy",
        out,
    ]
    logging.debug(f"Running command: {json.dumps(cmd)}")
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
class PaddedClass:
    original: str
    expected_duration: float
    duration: float
    pad_before: float
    pad_after: float

    @property
    def padded_duration(self) -> float:
        return self.duration + self.pad_before + self.pad_after


async def stitch_audio(
    itgs: Itgs, *, folder: str, disjoint: GeneratedClassWithDisjointAudio
) -> GeneratedClassWithJustVoice:
    """Stitches the audio files together in the disjoint class, adding space between
    the parts to pad to either 0.5s (if it filled the whole slot), otherwise padding
    to fit the slot size.

    Always adds a 1s buffer at the begining and 7s at the end.
    """
    info = [get_audio_info(path) for path in disjoint.audio_parts]

    classes: List[PaddedClass] = [
        PaddedClass(
            original=path,
            expected_duration=phrase[0].get_width_in_seconds(),
            duration=audio_info.duration,
            pad_before=0,
            pad_after=0,
        )
        for path, phrase, audio_info in zip(
            disjoint.audio_parts, disjoint.meta.transcript.phrases, info
        )
    ]
    classes[0].pad_before = 1
    classes[-1].pad_after = 7

    # For any class which is shorter than its expected duration, pad the end
    # with silence
    for cls in classes:
        if cls.duration < cls.expected_duration:
            cls.pad_after += cls.expected_duration - cls.duration

    # For the last class, if we just padded it at the end beyond 7s, move that padding
    # to before
    if classes[-1].pad_after > 7:
        classes[-1].pad_before += classes[-1].pad_after - 7
        classes[-1].pad_after = 7

    # If there are any classes with no end padding, give them 0.5s end padding
    for cls in classes:
        cls.pad_after = max(cls.pad_after, 0.5)

    # Actually generate the padded files
    padded_files: List[str] = []
    for cls in classes:
        cls_out = os.path.join(folder, secrets.token_urlsafe(16) + ".wav")
        pad_start_with_silence(
            src=cls.original,
            duration=cls.pad_before,
            out=cls_out,
        )
        cls_out2 = os.path.join(folder, secrets.token_urlsafe(16) + ".wav")
        pad_end_with_silence(
            src=cls_out,
            duration=cls.pad_after,
            out=cls_out2,
        )
        padded_files.append(cls_out2)

    out = os.path.join(folder, f"stitched-voice-{secrets.token_urlsafe(16)}.wav")
    concat_audio_files(padded_files, out=out, folder=folder)

    return GeneratedClassWithJustVoice(
        meta=disjoint.meta,
        voice_audio=out,
    )
