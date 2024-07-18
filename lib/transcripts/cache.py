import io
from typing import Awaitable, List, Optional, cast
from pydantic import BaseModel, Field
from itgs import Itgs
from lib.transcripts.model import TimeRange, Timestamp, Transcript
import perpetual_pub_sub as pps


class CachedTranscriptPhrase(BaseModel):
    """A single phrase within a transcript. Phrases are non-overlapping, but may
    not partition the content due to periods of silence.

    Only simple, single-speaker transcripts are supported at this time.
    """

    starts_at: float = Field(
        description="When this phrase begins, in seconds from the start of the recording"
    )
    ends_at: float = Field(
        description="When this phrase ends, in seconds from the start of the recording"
    )
    phrase: str = Field(description="The text of the phrase")


class CachedTranscript(BaseModel):
    """A transcript of a recording"""

    # NOTE: this is referenced in redis/keys.md

    uid: str = Field(
        description="The primary stable external identifier for this transcript"
    )
    phrases: List[CachedTranscriptPhrase] = Field(
        description="The phrases in this transcript, in ascending order of start time"
    )

    def to_internal(self) -> Transcript:
        """Converts this to the jobs internal representation, which is close to the VTT
        format (and trivially convertible to it)
        """
        return Transcript(
            phrases=[
                (
                    TimeRange(
                        start=Timestamp.from_seconds(phrase.starts_at),
                        end=Timestamp.from_seconds(phrase.ends_at),
                    ),
                    phrase.phrase,
                )
                for phrase in self.phrases
            ]
        )


async def get_transcript(itgs: Itgs, uid: str) -> Optional[CachedTranscript]:
    """Fetches the transcript with the given uid from the nearest source,
    if it exists, otherwise returns None

    Args:
        itgs (Itgs): the integrations to (re)use
        uid (str): the uid of the transcript to fetch

    Returns:
        Transcript, None: the transcript, if one with the given
            uid exists, otherwise None. This operates at less-than-none
            consistency, meaning it has to exist for the whole function call and
            at least 5 minutes before (default cursor freshness) to be
            guarranteed to be found, and may be stale if changed (though
            transcripts should not change, rather, a new one should be created)
    """
    result = await get_transcript_from_local_cache(itgs, uid)
    if result is not None:
        return CachedTranscript.model_validate_json(result)

    result = await get_transcript_from_network_cache(itgs, uid)
    if result is not None:
        await write_transcript_to_local_cache(itgs, uid, result)
        return CachedTranscript.model_validate_json(result)

    result = await get_transcript_from_source(itgs, uid)
    if result is None:
        return None
    encoded_result = result.__pydantic_serializer__.to_json(result)
    await write_transcript_to_local_cache(itgs, uid, content=encoded_result)
    await write_transcript_to_network_cache(itgs, uid, content=encoded_result)
    await push_transcript_to_all_instances_local_cache(
        itgs, uid, content=encoded_result
    )
    return result


async def get_transcript_from_local_cache(itgs: Itgs, uid: str) -> Optional[bytes]:
    """Attempts to get the transcript with the given uid from
    the local cache, if it exists, otherwise returns None.

    Args:
        itgs (Itgs): the integrations to (re)use
        uid (str): the uid of the transcript to fetch

    Returns:
        bytes, None: The transcript within a response, if it's available,
            otherwise None.
    """
    cache = await itgs.local_cache()
    raw = cast(
        Optional[bytes],
        cache.get(f"transcripts:byuid:{uid}".encode("utf-8")),
    )
    if raw is None:
        return None
    return raw[8:]


async def write_transcript_to_local_cache(itgs: Itgs, uid: str, content: bytes) -> None:
    """Writes the given serialized transcript to the local cache.

    Args:
        itgs (Itgs): the integrations to (re)use
        uid (str): the uid of the transcript to write
        content (bytes): the serialized transcript to write, as if from
            `transcript.model_dump_json().encode('utf-8')`
    """
    encoded_length = len(content).to_bytes(8, "big", signed=False)
    total_to_write = encoded_length + content

    cache = await itgs.local_cache()
    cache.set(f"transcripts:byuid:{uid}".encode("utf-8"), total_to_write)


async def get_transcript_from_network_cache(itgs: Itgs, uid: str) -> Optional[bytes]:
    """Attempts to get the transcript with the given uid from
    the network cache, if it exists, otherwise returns None.

    Args:
        itgs (Itgs): the integrations to (re)use
        uid (str): the uid of the transcript to fetch

    Returns:
        bytes, None: The transcript, if it's available,
            otherwise None.
    """
    redis = await itgs.redis()
    return await cast(
        Awaitable[Optional[bytes]], redis.get(f"transcripts:{uid}".encode("utf-8"))
    )


async def write_transcript_to_network_cache(
    itgs: Itgs, uid: str, content: bytes
) -> None:
    """Writes the given serialized transcript to the network cache.

    Args:
        itgs (Itgs): the integrations to (re)use
        uid (str): the uid of the transcript to write
        content (bytes): the serialized transcript to write, as if from
            `transcript.model_dump_json().encode('utf-8')`
    """
    redis = await itgs.redis()
    await redis.set(f"transcripts:{uid}".encode("utf-8"), content)


async def push_transcript_to_all_instances_local_cache(
    itgs: Itgs, uid: str, content: bytes
) -> None:
    """Actively pushes the given serialized transcript to the local cache
    of all instances.

    Args:
        itgs (Itgs): the integrations to (re)use
        uid (str): the uid of the transcript to write
        content (bytes): the serialized transcript to write, as if from
            `transcript.model_dump_json().encode('utf-8')`
    """
    encoded_uid = uid.encode("utf-8")
    message = (
        len(encoded_uid).to_bytes(4, "big", signed=False)
        + encoded_uid
        + len(content).to_bytes(8, "big", signed=False)
        + content
    )

    redis = await itgs.redis()
    await redis.publish(b"ps:transcripts", message)


async def get_transcript_from_source(
    itgs: Itgs, uid: str
) -> Optional[CachedTranscript]:
    """Attempts to fetch the transcript with the given uid from
    the database, if it exists, otherwise returns None.

    Args:
        itgs (Itgs): the integrations to (re)use
        uid (str): the uid of the transcript to fetch

    Returns:
        Transcript, None: The transcript, if it's available,
            otherwise None.
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            transcript_phrases.starts_at,
            transcript_phrases.ends_at,
            transcript_phrases.phrase
        FROM transcripts, transcript_phrases
        WHERE
            transcripts.uid = ?
            AND transcript_phrases.transcript_id = transcripts.id
        ORDER BY
            transcript_phrases.starts_at ASC,
            transcript_phrases.ends_at ASC,
            transcript_phrases.uid ASC
        """,
        (uid,),
    )
    if not response.results:
        return None

    phrases: List[CachedTranscriptPhrase] = []
    for row in response.results:
        phrases.append(
            CachedTranscriptPhrase(
                starts_at=row[0],
                ends_at=row[1],
                phrase=row[2],
            )
        )

    return CachedTranscript(
        uid=uid,
        phrases=phrases,
    )


async def actively_sync_local_cache():
    assert pps.instance is not None

    async with pps.PPSSubscription(pps.instance, "ps:transcripts", "taslc") as sub:
        async for raw_message in sub:
            message = io.BytesIO(raw_message)
            uid_length = int.from_bytes(message.read(4), "big", signed=False)
            uid = message.read(uid_length).decode("utf-8")
            content_length = int.from_bytes(message.read(8), "big", signed=False)
            content = message.read(content_length)
            async with Itgs() as itgs:
                await write_transcript_to_local_cache(itgs, uid, content)
