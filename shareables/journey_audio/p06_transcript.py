"""The first step in this pipeline is to generate a transcript of
the relevant portion of the audio file. Note that this is labeled
as step 6, since the first 5 steps are the same as in journey_audio.
"""

import logging
from typing import Optional, cast
from itgs import Itgs
from lib.transcripts.db import (
    store_transcript_for_content_file,
    fetch_transcript_for_content_file,
)
from lib.transcripts.gen import create_transcript as _create_transcript
from lib.transcripts.model import Transcript, parse_vtt_transcript
from shareables.shareable_pipeline_exception import ShareablePipelineException
from content import hash_content_sync


class TranscriptError(ShareablePipelineException):
    def __init__(self, message: str):
        super().__init__(message, 6, "transcript")


async def create_transcript(
    itgs: Itgs,
    source_audio_path: str,
    duration: Optional[int] = None,
    *,
    instructor: Optional[str] = None,
    source_audio_hash: Optional[str] = None,
) -> Transcript:
    """Creates a transcript of the audio file at the given path. This should
    be passed the source audio file; it will be transcoded as necessary. If
    the duration is specified, only the first duration seconds of the audio
    will be transcribed.

    Note that this will attempt to load the transcript from the database if
    it's already available, and will store it in the database if it's forced
    to generate it, similar to create_journey_transcript. It looks up the
    content file based on the hash of the source audio file.

    Args:
        itgs (Itgs): The integrations to (re)use
        source_audio_path (str): The path to the source audio file
        duration (int, None): The duration of the audio to transcribe, in seconds. Defaults to None.
        instructor (str, None): The name of the instructor, if known. Defaults to None.
            Improves the transcript, in particular if the instructor introduces themself.
        source_audio_hash (str, None): The hash of the source audio file, or None to have it
            computed.
    """
    if source_audio_hash is None:
        source_audio_hash = hash_content_sync(source_audio_path)

    conn = await itgs.conn()
    cursor = conn.cursor()

    response = await cursor.execute(
        """
        SELECT
            content_files.uid
        FROM content_files
        WHERE
            content_files.original_sha512 = ?
        """,
        [source_audio_hash],
    )
    if not response.results:
        logging.info(
            f"Since there is no content file for this audio ({source_audio_hash=}), "
            "cannot cache transcript permanently. Using diskcache instead."
        )
        cache = await itgs.local_cache()
        cache_key = f"transcripts:{source_audio_hash}".encode("ascii")
        raw_transcript = cast(Optional[bytes], cache.get(cache_key))
        if raw_transcript is not None:
            try:
                return parse_vtt_transcript(str(raw_transcript, "utf-8"))
            except Exception:
                logging.warning(
                    f"Failed to parse cached transcript:\n\n{raw_transcript}",
                    exc_info=True,
                )
                cache.delete(cache_key)

        transcript = await _create_transcript(
            itgs,
            source_audio_path,
            duration=duration,
            prompt=f"A class from {instructor}." if instructor is not None else None,
        )
        cache.set(
            cache_key,
            f"WEBVTT\n\n{transcript.transcript}".encode("utf-8"),
            expire=60 * 60 * 24 * 7,
        )
        return transcript.transcript

    content_file_uid: str = response.results[0][0]
    transcript = await fetch_transcript_for_content_file(itgs, content_file_uid)
    if transcript is not None:
        return transcript

    transcript = await _create_transcript(
        itgs,
        source_audio_path,
        duration=duration,
        prompt=f"A class from {instructor}." if instructor is not None else None,
    )
    await store_transcript_for_content_file(
        itgs,
        content_file_uid=content_file_uid,
        transcript=transcript.transcript,
        source=transcript.source,
    )
    return transcript.transcript
