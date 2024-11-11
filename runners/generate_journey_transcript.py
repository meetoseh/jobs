"""Generates a transcript for the audio file associated with a particular journey.
Note that the journey is useful information compared to just the content file itself,
as the name of the instructor can be used to assist the model in transcribing the file.
"""

from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from lib.transcripts.gen import create_transcript
from lib.transcripts.db import store_transcript_for_content_file
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, journey_uid: str):
    """Generates a transcript for the audio file associated with the journey with
    the given uid, if it doesn't already have one and we're not already generating
    one.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_uid (str): the uid of the journey to generate a transcript for
    """
    async with basic_redis_lock(
        itgs, f"jobs:generate_transcript:{journey_uid}:lock", gd=gd, spin=True
    ):
        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        response = await cursor.execute(
            """
            SELECT 1 FROM journeys, content_files, content_file_transcripts
            WHERE
                journeys.uid = ?
                AND journeys.audio_content_file_id = content_files.id
                AND content_files.id = content_file_transcripts.content_file_id
            LIMIT 1
            """,
            [journey_uid],
        )
        if response.results:
            logging.info(
                f"Since {journey_uid=} already has a transcript, not generating a new one"
            )
            return

        response = await cursor.execute(
            """
            SELECT 
                instructors.name, content_files.uid, s3_files.key 
            FROM journeys, instructors, content_files, s3_files
            WHERE
                journeys.uid = ?
                AND journeys.instructor_id = instructors.id
                AND journeys.audio_content_file_id = content_files.id
                AND s3_files.id = content_files.original_s3_file_id
            """,
            [journey_uid],
        )
        if not response.results:
            logging.error(f"Could not find a journey with {journey_uid=}")
            return

        instructor_name: str = response.results[0][0]
        content_file_uid: str = response.results[0][1]
        s3_key: Optional[str] = response.results[0][2]

        if s3_key is None:
            raise NotImplementedError(
                "TODO: handle content files whose original was lost"
            )

        files = await itgs.files()
        with temp_file() as source:
            with open(source, "wb") as f:
                await files.download(
                    f, bucket=files.default_bucket, key=s3_key, sync=True
                )

            result = await create_transcript(
                itgs, source, prompt=f"A class with {instructor_name}."
            )
            await store_transcript_for_content_file(
                itgs,
                content_file_uid=content_file_uid,
                transcript=result.transcript,
                source=result.source,
            )


if __name__ == "__main__":
    import asyncio

    async def main():
        journey_uid = input("Journey uid: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.generate_journey_transcript", journey_uid=journey_uid
            )

    asyncio.run(main())
