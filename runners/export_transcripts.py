"""Produces a giant TXT file containing all the transcripts of undeleted classes,
for manual analysis/searching to find classes
"""
from typing import List, Optional, Tuple, cast
from itgs import Itgs
from graceful_death import GracefulDeath
from lib.transcripts.model import load_transcript_from_phrases
from temp_files import temp_file
from jobs import JobCategory
import socket
import zipfile
import os

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath, *, s3_key: str):
    """Produces a giant TXT file containing all the transcripts of undeleted classes,
    for manual analysis/searching to find classes, and uploads it to S3
    in the default bucket under the given key.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        s3_key (str): the key to upload the file to
    """
    slack = await itgs.slack()
    await slack.send_ops_message(
        f"{socket.gethostname()} managing transcript export..."
    )

    with temp_file() as local_filepath, temp_file() as local_filepath_zip:
        conn = await itgs.conn()
        cursor = conn.cursor("none")

        last_uid: Optional[str] = None

        with open(local_filepath, "w") as out:
            while True:
                if gd.received_term_signal:
                    return
                response = await cursor.execute(
                    """
                    SELECT
                        journeys.uid,
                        journeys.title,
                        journeys.description,
                        instructors.name,
                        audio_content_files.duration_seconds,
                        transcripts.uid
                    FROM journeys, instructors, content_files AS audio_content_files, content_file_transcripts, transcripts
                    WHERE
                        journeys.deleted_at IS NULL
                        AND journeys.special_category IS NULL
                        AND (
                            journeys.variation_of_journey_id IS NULL 
                            OR (
                                NOT EXISTS (
                                    SELECT 1 FROM journeys AS variations
                                    WHERE
                                        variations.id = journeys.variation_of_journey_id
                                        AND variations.deleted_at IS NULL
                                        AND variations.special_category IS NULL
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM journeys AS variations
                                    WHERE
                                        variations.variation_of_journey_id = journeys.variation_of_journey_id
                                        AND variations.deleted_at IS NULL
                                        AND variations.special_category IS NULL
                                        AND variations.uid < journeys.uid
                                )
                            )
                        )
                        AND journeys.instructor_id = instructors.id
                        AND journeys.audio_content_file_id = audio_content_files.id
                        AND journeys.audio_content_file_id = content_file_transcripts.content_file_id
                        AND NOT EXISTS (
                            SELECT 1 FROM content_file_transcripts AS cft
                            WHERE
                                cft.content_file_id = journeys.audio_content_file_id
                                AND (
                                    cft.created_at > content_file_transcripts.created_at
                                    OR (
                                        cft.created_at = content_file_transcripts.created_at
                                        AND cft.uid < content_file_transcripts.uid
                                    )
                                )
                        )
                        AND content_file_transcripts.transcript_id = transcripts.id
                        AND (? IS NULL OR journeys.uid > ?)
                    ORDER BY journeys.uid ASC
                    LIMIT 10
                    """,
                    (last_uid, last_uid),
                )

                if not response.results:
                    break

                for (
                    row_uid,
                    row_title,
                    row_description,
                    row_instructor_name,
                    row_duration_seconds,
                    row_transcript_uid,
                ) in response.results:
                    last_uid = row_uid

                    out.write("Title: ")
                    out.write(row_title)
                    out.write("\nInstructor: ")
                    out.write(row_instructor_name)
                    out.write("\nDescription: ")
                    out.write(row_description)
                    out.write(
                        f"\nDuration: {int(row_duration_seconds)}s"
                        "\nTranscript:\n---\n"
                    )

                    inner_response = await cursor.execute(
                        """
                        SELECT
                            starts_at, ends_at, phrase
                        FROM transcript_phrases
                        WHERE
                            EXISTS (
                                SELECT 1 FROM transcripts
                                WHERE
                                    transcripts.uid = ?
                                    AND transcripts.id = transcript_phrases.transcript_id
                            )
                        ORDER BY starts_at, ends_at, uid
                        """,
                        (row_transcript_uid,),
                    )
                    row_transcript = load_transcript_from_phrases(
                        cast(
                            List[Tuple[float, float, str]], inner_response.results or []
                        )
                    )
                    for timerange, phrase in row_transcript.phrases:
                        out.write(f"{timerange.start} - {timerange.end}: {phrase}\n")

                    out.write("\n---\n\n")

        with zipfile.ZipFile(local_filepath_zip, "w") as zf:
            zf.write(
                local_filepath,
                arcname="transcripts.txt",
                compress_type=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            )

        files = await itgs.files()
        with open(local_filepath_zip, "rb") as f:
            await files.upload(f, bucket=files.default_bucket, key=s3_key, sync=True)

        compressed_size_bytes = os.path.getsize(local_filepath_zip)
        uncompressed_size_bytes = os.path.getsize(local_filepath)
        await slack.send_ops_message(
            f"{socket.gethostname()} Done managing transcript export @ {s3_key}. "
            f"Compressed size: {compressed_size_bytes} bytes. "
            f"Decompressed size: {uncompressed_size_bytes} bytes."
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        s3_key = input("S3 Key: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.export_transcripts", s3_key=s3_key)

    asyncio.run(main())
