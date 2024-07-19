"""Generates a new journey embeddings file, without considering if we have a good
one or not. Typically, it makes sense to call `runners.ensure_journey_embeddings`
instead of invoking this directly, which will first verify if we have a good
journey embeddings file already.
"""

import hashlib
import io
import json
import os
import secrets
import time
from typing import List, Optional, cast
from itgs import Itgs
from graceful_death import GracefulDeath
from dataclasses import dataclass

from jobs import JobCategory
from journal_chat_jobs.lib.journey_embeddings import JourneyEmbeddingMetadata
from lib.transcripts.cache import CachedTranscript, get_transcript
import logging
import openai
import temp_files
import struct
import socket

category = JobCategory.HIGH_RESOURCE_COST

MODEL = "text-embedding-3-large"
TECHNIQUE = "metadata-and-transcript:v1.0.0"


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Clears the `journey_embeddings_needs_refresh` redis key and generates a new journey
    embeddings file, storing it in the database. If it fails, tries to set the
    `journey_embeddings_needs_refresh` key back to
    `{"reason": "last-job-failed", "at": time.time()}`. If it succeeds, overwrites
    the redis key `journey_embeddings` with the metadata of the new embeddings file
    and publishes a message to `ps:journey_embeddings`

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    redis = await itgs.redis()
    await redis.delete(b"journey_embeddings_needs_refresh")

    async def _bounce():
        await redis.set(
            b"journey_embeddings_needs_refresh",
            json.dumps({"reason": "last-job-interrupted", "at": time.time()}).encode(
                "utf-8"
            ),
        )
        jobs = await itgs.jobs()
        await jobs.enqueue("runners.generate_journey_embeddings")

    try:
        conn = await itgs.conn()
        cursor = conn.cursor("none")

        last_uid: Optional[str] = None
        journeys: List[JourneyForEmbedding] = []
        while True:
            response = await cursor.execute(
                """
SELECT
    journeys.uid,
    journeys.title,
    journeys.description,
    instructors.name,
    transcripts.uid
FROM journeys, instructors, transcripts
WHERE
    journeys.deleted_at IS NULL
    AND journeys.special_category IS NULL
    AND (? IS NULL OR journeys.uid > ?)
    AND instructors.id = journeys.instructor_id
    AND transcripts.id = (
        SELECT content_file_transcripts.transcript_id
        FROM content_file_transcripts
        WHERE
            content_file_transcripts.content_file_id = journeys.audio_content_file_id
        ORDER BY content_file_transcripts.created_at DESC, content_file_transcripts.uid ASC
    )
ORDER BY journeys.uid ASC
LIMIT 100
                """,
                (last_uid, last_uid),
            )

            if not response.results:
                break

            for row in response.results:
                if gd.received_term_signal:
                    await _bounce()
                    return

                row_uid = cast(str, row[0])
                row_title = cast(str, row[1])
                row_description = cast(str, row[2])
                row_instructor_name = cast(str, row[3])
                row_transcript_uid = cast(str, row[4])

                row_transcript = await get_transcript(itgs, uid=row_transcript_uid)
                if row_transcript is None:
                    continue

                journeys.append(
                    JourneyForEmbedding(
                        uid=row_uid,
                        title=row_title,
                        description=row_description,
                        instructor_name=row_instructor_name,
                        transcript=row_transcript,
                    )
                )

            last_uid = journeys[-1].uid

        logging.info(
            f"Collected {len(journeys)} journeys, starting embedding, 10 per request"
        )

        journey_uid_longest_length_bytes = max(
            len(journey.uid.encode("utf-8")) for journey in journeys
        )

        # the length we store the journey uids with. Always a multiple of 4.
        # for example, 29 -> 32, 32 -> 32, 33 -> 36
        journey_uid_length = ((journey_uid_longest_length_bytes + 3) // 4) * 4
        embedding_length_count = 3072
        embedding_length_bytes = embedding_length_count * 8

        openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
        client = openai.Client(api_key=openai_api_key)

        block = bytearray(journey_uid_length + embedding_length_bytes)

        offsets: List[int] = []
        with temp_files.temp_file() as embeddings_file:
            with open(embeddings_file, "wb") as embeddings_out:
                hasher = hashlib.sha512()
                for start_idx in range(0, len(journeys), 10):
                    if gd.received_term_signal:
                        await _bounce()
                        return
                    end_idx = min(start_idx + 10, len(journeys))

                    embeddings = client.embeddings.create(
                        model=MODEL,
                        encoding_format="float",
                        input=[
                            f"""{journeys[idx].title}
    {journeys[idx].instructor_name}
    {journeys[idx].description}
    {str(journeys[idx].transcript.to_internal())}
                            """
                            for idx in range(start_idx, end_idx)
                        ],
                    )
                    logging.debug(f"Embedded {start_idx}-{end_idx}")

                    for idx, embedding in zip(
                        range(start_idx, end_idx), embeddings.data
                    ):
                        journey = journeys[idx]
                        journey_uid = journey.uid.encode("utf-8")
                        initial_pad = journey_uid_length - len(journey_uid)
                        block[:initial_pad] = b"\0" * initial_pad
                        block[initial_pad : initial_pad + len(journey_uid)] = (
                            journey_uid
                        )

                        for subidx, f in enumerate(embedding.embedding):
                            struct.pack_into(
                                ">d", block, journey_uid_length + subidx * 8, f
                            )

                        offsets.append(embeddings_out.tell())
                        embeddings_out.write(block)
                        hasher.update(block)

            integrity = hasher.hexdigest()
            file_size = os.path.getsize(embeddings_file)
            logging.info(
                f"Finished writing embedding (size: {file_size}, integrity: {integrity})"
            )

            journey_embedding_uid = f"oseh_jemb_{secrets.token_urlsafe(16)}"

            files = await itgs.files()
            s3_file_uid = f"oseh_s3f_{secrets.token_urlsafe(16)}"
            s3_file_key = f"s3_files/journey_embeddings/{journey_embedding_uid}"
            s3_file_bucket = files.default_bucket

            purgatory_key = json.dumps(
                {
                    "key": s3_file_key,
                    "bucket": s3_file_bucket,
                    "hint": "jobs/runners/generate_journey_embeddings.py",
                    "expected": False,
                },
                sort_keys=True,
            )
            soon = time.time() + 60 * 60
            await redis.zadd("files:purgatory", mapping={purgatory_key: soon})
            with open(embeddings_file, "rb") as embeddings_in:
                await files.upload(
                    embeddings_in,
                    bucket=files.default_bucket,
                    key=s3_file_key,
                    sync=True,
                )

            created_at = time.time()
            await cursor.executemany3(
                (
                    (
                        """
INSERT INTO s3_files (
    uid, key, file_size, content_type, created_at
)
VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            s3_file_uid,
                            s3_file_key,
                            file_size,
                            "application/octet-stream",
                            created_at,
                        ),
                    ),
                    (
                        """
INSERT INTO journey_embeddings ( 
    uid, model, technique, journey_uid_byte_length, embedding_byte_length, s3_file_id, sha512, created_at
)
SELECT
    ?, ?, ?, ?, ?, s3_files.id, ?, ?
FROM s3_files WHERE s3_files.uid = ?
                        """,
                        (
                            journey_embedding_uid,
                            MODEL,
                            TECHNIQUE,
                            journey_uid_length,
                            embedding_length_bytes,
                            integrity,
                            created_at,
                            s3_file_uid,
                        ),
                    ),
                ),
            )

            for start_idx in range(0, len(journeys), 100):
                end_idx = min(start_idx + 100, len(journeys))

                query = io.StringIO()
                query.write("WITH batch(uid, journey_uid, offset) AS (VALUES (?, ?, ?)")
                qargs = []
                for i in range(start_idx, end_idx):
                    if i != start_idx:
                        query.write(", (?, ?, ?)")
                    qargs.append(f"oseh_jemi_{secrets.token_urlsafe(16)}")
                    qargs.append(journeys[i].uid)
                    qargs.append(offsets[i])
                query.write(
                    ") INSERT INTO journey_embedding_items ("
                    " uid, journey_embedding_id, journey_id, offset"
                    ") SELECT"
                    " batch.uid,"
                    " (SELECT journey_embeddings.id FROM journey_embeddings WHERE journey_embeddings.uid = ?),"
                    " journeys.id,"
                    " batch.offset "
                    "FROM batch, journeys "
                    "WHERE journeys.uid = batch.journey_uid"
                )
                qargs.append(journey_embedding_uid)
                response = await cursor.execute(query.getvalue(), qargs)
                if response.rows_affected != end_idx - start_idx:
                    # will be cleaned up when the s3 file is deleted
                    raise Exception(
                        f"Expected {end_idx - start_idx} rows affected, got {response.rows_affected}"
                    )

            await redis.zrem("files:purgatory", purgatory_key)
            await redis.set(
                b"journey_embeddings",
                JourneyEmbeddingMetadata.__pydantic_serializer__.to_json(
                    JourneyEmbeddingMetadata(
                        uid=journey_embedding_uid,
                        s3_file_key=s3_file_key,
                        s3_file_bucket=s3_file_bucket,
                        journey_uid_byte_length=journey_uid_length,
                        embedding_byte_length=embedding_length_bytes,
                        model=MODEL,
                        sha512=integrity,
                    )
                ),
            )

            slack = await itgs.slack()
            await slack.send_ops_message(
                f"`{socket.gethostname()}` successfully updated journey embeddings\n\n"
                f"- uid: `{journey_embedding_uid}`\n"
                f"- integrity: `{integrity}`\n"
                f"- number of journeys: {len(journeys)}\n"
                f"- file size: {file_size} bytes"
            )
    except BaseException:
        await redis.set(
            b"journey_embeddings_needs_refresh",
            json.dumps({"reason": "last-job-failed", "at": time.time()}).encode(
                "utf-8"
            ),
        )
        raise


@dataclass
class JourneyForEmbedding:
    uid: str
    title: str
    description: str
    instructor_name: str
    transcript: CachedTranscript


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.generate_journey_embeddings")

    asyncio.run(main())
