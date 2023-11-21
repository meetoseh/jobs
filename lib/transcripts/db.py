import secrets
from typing import Literal, Optional, List, Tuple, cast
from itgs import Itgs
from lib.transcripts.model import (
    Transcript,
    TranscriptSource,
    load_transcript_from_phrases,
)
import time
import json


async def fetch_transcript_for_content_file(
    itgs: Itgs,
    content_file_uid: str,
    *,
    consistency: Literal["none", "weak", "strong"] = "none",
) -> Optional[Transcript]:
    """Fetches the transcript associated with the content file with the given
    uid.

    Args:
        itgs (Itgs): The integrations to (re)use
        content_file_uid (str): The uid of the content file
        consistency ('none', 'weak', 'strong'): The consistency level to read
            from the database with

    Returns:
        Transcript or None: The latest transcript for that content file, if it
            has one, otherwise None
    """
    conn = await itgs.conn()
    cursor = conn.cursor(consistency)

    response = await cursor.execute(
        """
        SELECT
            starts_at, ends_at, phrase
        FROM transcript_phrases
        WHERE
            EXISTS (
                SELECT 1 FROM content_files, content_file_transcripts
                WHERE
                    content_files.uid = ?
                    AND content_file_transcripts.content_file_id = content_files.id
                    AND content_file_transcripts.transcript_id = transcript_phrases.transcript_id
                    AND NOT EXISTS (
                        SELECT 1 FROM content_file_transcripts AS cft2
                        WHERE
                            cft2.content_file_id = content_files.id
                            AND (
                                cft2.created_at > content_file_transcripts.created_at
                                OR (
                                    cft2.created_at = content_file_transcripts.created_at
                                    AND cft2.uid > content_file_transcripts.uid
                                )
                            )
                    )
            )
        ORDER BY starts_at, ends_at, uid
        """,
        (content_file_uid,),
    )

    if not response.results:
        return None

    return load_transcript_from_phrases(
        cast(List[Tuple[float, float, str]], response.results)
    )


async def store_transcript_for_content_file(
    itgs: Itgs,
    *,
    content_file_uid: str,
    transcript: Transcript,
    source: TranscriptSource,
    generated_at: Optional[float] = None,
):
    """Stores a new transcript for the content file with the given uid.

    Args:
        itgs (Itgs): The integrations to (re)use
        content_file_uid (str): The uid of the content file
        transcript (Transcript): The transcript to store
        generated_at (float): The timestamp of when the transcript was generated, or
            None to use the current time

    Raises:
        ValueError: if no content file with the given uid exists
    """
    if generated_at is None:
        generated_at = time.time()

    conn = await itgs.conn()
    cursor = conn.cursor()

    transcript_uid = f"oseh_t_{secrets.token_urlsafe(16)}"
    content_transcript_uid = f"oseh_cft_{secrets.token_urlsafe(16)}"
    phrase_qmark_list = ", ".join(["(?,?,?,?)"] * len(transcript.phrases))
    phrase_insert_qargs = []
    for phrase in transcript.phrases:
        phrase_insert_qargs.extend(
            (
                f"oseh_tp_{secrets.token_urlsafe(16)}",
                phrase[0].start.in_seconds(),
                phrase[0].end.in_seconds(),
                phrase[1],
            )
        )

    response = await cursor.executemany3(
        (
            (
                """
                INSERT INTO transcripts (
                    uid, source, created_at
                ) 
                SELECT ?, ?, ?
                WHERE
                    EXISTS (
                        SELECT 1 FROM content_files
                        WHERE content_files.uid = ?
                    )
                """,
                (transcript_uid, json.dumps(source), generated_at, content_file_uid),
            ),
            (
                f"""
                WITH phrases_to_insert(uid, starts_at, ends_at, phrase) AS (VALUES {phrase_qmark_list})
                INSERT INTO transcript_phrases (
                    uid,
                    transcript_id,
                    starts_at,
                    ends_at,
                    phrase
                )
                SELECT
                    phrases_to_insert.uid,
                    transcripts.id,
                    phrases_to_insert.starts_at,
                    phrases_to_insert.ends_at,
                    phrases_to_insert.phrase
                FROM phrases_to_insert, transcripts
                WHERE transcripts.uid = ?
                """,
                (*phrase_insert_qargs, transcript_uid),
            ),
            (
                """
                INSERT INTO content_file_transcripts (
                    uid, content_file_id, transcript_id, created_at
                )
                SELECT
                    ?, content_files.id, transcripts.id, ?
                FROM content_files, transcripts
                WHERE
                    content_files.uid = ?
                    AND transcripts.uid = ?
                """,
                (
                    content_transcript_uid,
                    generated_at,
                    content_file_uid,
                    transcript_uid,
                ),
            ),
        )
    )

    if response[0].rows_affected != 1:
        raise ValueError(
            f"Failed to create transcript (content file {content_file_uid=} does not exist)"
        )

    if response[1].rows_affected != len(transcript.phrases):
        raise ValueError(
            f"Failed to store all {len(transcript.phrases)=} phrases (only stored {response[2].rows_affected=}): this should not happen"
        )

    if response[2].rows_affected != 1:
        raise ValueError(
            f"Failed to associate {content_file_uid=} with {transcript_uid=}: this should not happen"
        )
