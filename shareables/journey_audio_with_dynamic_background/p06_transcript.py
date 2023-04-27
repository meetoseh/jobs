"""The first step in this pipeline is to generate a transcript of
the relevant portion of the audio file. Note that this is labeled
as step 6, since the first 5 steps are the same as in journey_audio.
"""
from dataclasses import dataclass
import json
import subprocess
from typing import List, Optional, Tuple
import logging
import shutil
from shareables.shareable_pipeline_exception import ShareablePipelineException
from temp_files import temp_file
import os
import openai


class TranscriptError(ShareablePipelineException):
    def __init__(self, message: str):
        super().__init__(message, 6, "transcript")


@dataclass
class Timestamp:
    hours: int
    minutes: int
    seconds: int
    milliseconds: int

    def __str__(self) -> str:
        return f"{self.hours}:{self.minutes}:{self.seconds}.{self.milliseconds}"

    def in_seconds(self) -> float:
        return (
            self.hours * 60 * 60
            + self.minutes * 60
            + self.seconds
            + self.milliseconds / 1000
        )


def parse_vtt_timestamp(timestamp: str) -> Timestamp:
    """Parses a timestamp as specified in vtt files, e.g.,
    00:01:03.000 means 1 minute, 3 seconds.

    Args:
        timestamp (str): The timestamp to parse

    Returns:
        Timestamp: The parsed timestamp

    Raises:
        ValueError: If the timestamp is not in the expected format
    """
    if len(timestamp) != len("00:00:00.000"):
        raise ValueError(f"Invalid timestamp (bad length): {timestamp}")

    if timestamp[2] != ":":
        raise ValueError(f"Invalid timestamp (expected colon at index 2): {timestamp}")

    if timestamp[5] != ":":
        raise ValueError(f"Invalid timestamp (expected colon at index 5): {timestamp}")

    if timestamp[8] != ".":
        raise ValueError(f"Invalid timestamp (expected period at index 8): {timestamp}")

    try:
        hours = int(timestamp[0:2])
    except ValueError:
        raise ValueError(
            f"Invalid timestamp (expected hours at index 0-1): {timestamp}"
        )

    try:
        minutes = int(timestamp[3:5])
    except ValueError:
        raise ValueError(
            f"Invalid timestamp (expected minutes at index 3-4): {timestamp}"
        )

    try:
        seconds = int(timestamp[6:8])
    except ValueError:
        raise ValueError(
            f"Invalid timestamp (expected seconds at index 6-7): {timestamp}"
        )

    try:
        milliseconds = int(timestamp[9:12])
    except ValueError:
        raise ValueError(
            f"Invalid timestamp (expected milliseconds at index 9-11): {timestamp}"
        )

    return Timestamp(hours, minutes, seconds, milliseconds)


@dataclass
class TimeRange:
    start: Timestamp
    end: Timestamp

    def __str__(self) -> str:
        return f"{self.start} --> {self.end}"

    def get_width_in_seconds(self) -> float:
        return self.end.in_seconds() - self.start.in_seconds()


def parse_vtt_timerange(line: str) -> TimeRange:
    """Parses a timerange in vtt format, for example,

    00:00:33.000 --> 00:00:42.000

    Args:
        line (str): The line to parse

    Returns:
        TimeRange: The parsed timerange

    Raises:
        ValueError: If the timerange is not in the expected format
    """
    if len(line) != len("00:00:00.000 --> 00:00:00.000"):
        raise ValueError(f"Invalid timerange (bad length): {line}")

    if line[len("00:00:00.000") : len("00:00:00.000 --> ")] != " --> ":
        raise ValueError(f"Invalid timerange (expected ' --> ' at index 12-16): {line}")

    start = parse_vtt_timestamp(line[: len("00:00:00.000")])
    end = parse_vtt_timestamp(line[len("00:00:00.000 --> ") :])

    return TimeRange(start, end)


@dataclass
class Transcript:
    phrases: List[Tuple[TimeRange, str]]
    """The phrases in the transcript, as a list of (timerange, text) tuples, where
    the timerange is the time during which the given text was said. Generally
    one thought is said per timerange, but this is not guaranteed.
    """

    def __str__(self) -> str:
        return "\n".join(
            [
                f"{timerange}\n{text}\n"
                for (timerange, text) in self.phrases
                if len(text) > 0
            ]
        )


def parse_vtt_transcript(raw: str) -> Transcript:
    """Parses the VTT transcript already loaded as a string.

    A VTT file is very simple; it's a series of timeranges followed by text
    and then a blank line. For example:

    WEBVTT

    00:00:00.000 --> 00:00:20.000
    Hi, it's Anna. Welcome to your moment for gratitude. It's really easy in our day-to-day hustle and bustle and to-do lists to lose sight of what we have to be grateful for.

    00:00:20.000 --> 00:00:33.000
    Can you take a moment here and now, wherever you are, whether it be sitting, standing up, or lying down, to just remember three things.

    00:00:33.000 --> 00:00:42.000
    They could be big, they could be small. Just let them be your own. Three things that you're grateful for.

    00:00:42.000 --> 00:01:03.000
    A gentle reminder that every breath that we get to experience is a gift.

    00:01:03.000 --> 00:01:18.000
    Can you allow these thoughts, these notions, these simple gratitudes to ground you? Can you carry them with you through the rest of your day?

    00:01:18.000 --> 00:01:29.000
    Peace.

    Args:
        raw (str): The raw VTT transcript

    Returns:
        Transcript: The parsed transcript

    Raises:
        ValueError: If the transcript is not in the expected format
    """
    lines = raw.split("\n")

    if len(lines) == 0:
        raise TranscriptError("Empty transcript")

    if lines[0] != "WEBVTT":
        raise TranscriptError("Expected 'WEBVTT' as first line")

    phrases: List[Tuple[TimeRange, str]] = []

    next_index = 1
    while next_index < len(lines):
        if lines[next_index] == "":
            next_index += 1
            continue

        try:
            timerange = parse_vtt_timerange(lines[next_index])
        except ValueError as e:
            raise ValueError(f"Invalid timerange on line {next_index+1}: {e}")

        next_index += 1

        text = ""
        while next_index < len(lines) and lines[next_index] != "":
            if text != "":
                text += " "
            text += lines[next_index]
            next_index += 1

        phrases.append((timerange, text.strip()))

    return Transcript(phrases)


def create_transcript(
    source_audio_path: str,
    duration: Optional[int] = None,
    *,
    instructor: Optional[str] = None,
) -> Transcript:
    """Creates a transcript of the audio file at the given path. This should
    be passed the source audio file; it will be transcoded as necessary. If
    the duration is specified, only the first duration seconds of the audio
    will be transcribed.

    Args:
        source_audio_path (str): The path to the source audio file
        duration (Optional[int], optional): The duration of the audio to transcribe, in seconds. Defaults to None.
    """
    ffmpeg = shutil.which("ffmpeg")
    with temp_file(".mp4") as target_filepath:
        # attempts to mirror the settings that are actually used by the
        # whisper model, but we need some compression to send this over
        # the wire, hence mp4
        cmd = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-nostats",
            *(["-t", str(duration)] if duration is not None else []),
            "-i",
            source_audio_path,
            "-vn",
            "-acodec",
            "aac",
            "-ar",
            "16000",
            "-b:a",
            "128k",
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
            raise TranscriptError(
                "ffmpeg failed\n\nstdout: ```\n"
                + result.stdout.decode("utf-8")
                + "\n```\n\nstderr: ```\n"
                + result.stderr.decode("utf-8")
                + "\n```"
            )

        original_filesize = os.path.getsize(source_audio_path)
        target_filesize = os.path.getsize(target_filepath)
        logging.info(
            f"Transcoded audio to {target_filepath} ({original_filesize} bytes -> {target_filesize} bytes)"
        )

        logging.info(
            f"Producing transcript for {target_filepath} using OpenAI whisper-1"
        )
        openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]

        try:
            with open(target_filepath, "rb") as f:
                transcript: str = openai.Audio.translate(
                    "whisper-1",
                    f,
                    api_key=openai_api_key,
                    response_format="vtt",
                    language="en",
                    prompt=(
                        f"A transcript of a class with {instructor}."
                        if instructor
                        else None
                    ),
                )
        except Exception:
            raise TranscriptError("OpenAI Transcription via API failed")

        logging.info(f"VTT Transcript: {transcript}")
        try:
            return parse_vtt_transcript(transcript)
        except ValueError as e:
            raise TranscriptError(f"Error parsing VTT transcript from OpenAI: {e}")
