from typing import Optional
import openai
import openai._types
import shutil
import logging
import json
import subprocess
import os
from itgs import Itgs
from temp_files import temp_file
from lib.transcripts.model import TranscriptWithSource, parse_vtt_transcript
from lib.redis_api_limiter import ratelimit_using_redis


async def create_transcript(
    itgs: Itgs,
    source: str,
    *,
    duration: Optional[int] = None,
    prompt: Optional[str] = None,
) -> TranscriptWithSource:
    """Creates a transcript for the audio or video file at the given source
    path. The file will be first processed by ffmpeg into the ideal format
    for uploading, which means stripping video and typically reducing the
    sample rate.

    Args:
        itgs (Itgs): The integrations to (re)use
        source (str): The path to the source audio or video file
        duration (int, None): If specified, only the first duration seconds are
            transcribed
        prompt (str, None): If specified, used to assist the model in
            transcribing the file. Should include, for example, the name of the
            speaker if known.

    Returns:
        Transcript: The transcript of the file from openai's whisper-1 model
    """
    logging.info(f"lib.transcripts.get.create_transcript({source=}, {duration=})")
    await ratelimit_using_redis(
        itgs, key="external_apis:api_limiter:whisper-1", time_between_requests=5
    )
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
            source,
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
            raise ValueError(
                "ffmpeg failed\n\nstdout: ```\n"
                + result.stdout.decode("utf-8")
                + "\n```\n\nstderr: ```\n"
                + result.stderr.decode("utf-8")
                + "\n```"
            )

        original_filesize = os.path.getsize(source)
        target_filesize = os.path.getsize(target_filepath)
        logging.info(
            f"Transcoded audio to {target_filepath} ({original_filesize} bytes -> {target_filesize} bytes)"
        )
        logging.info(
            f"Producing transcript for {target_filepath} using OpenAI whisper-2"
        )
        openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
        openai_client = openai.OpenAI(api_key=openai_api_key)

        try:
            with open(target_filepath, "rb") as f:
                transcript_vtt = openai_client.audio.translations.create(
                    model="whisper-1",
                    file=f,
                    response_format="vtt",
                    prompt=prompt if prompt is not None else openai._types.NOT_GIVEN,
                )
        except Exception:
            raise ValueError("OpenAI Transcription via API failed")

        logging.info(
            f"Produced transcript for {target_filepath} using OpenAI whisper-1:\n\n{transcript_vtt}\n\n"
        )
        result = parse_vtt_transcript(transcript_vtt.text)
        logging.debug(f"Parsed transcript: {result=}")
        return TranscriptWithSource(
            transcript=result,
            source={
                "type": "ai",
                "model": "whisper-1",
                "version": "live",
            },
        )
