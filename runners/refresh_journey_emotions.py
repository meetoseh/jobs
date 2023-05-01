from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from lib.transcripts.db import fetch_transcript_for_content_file
from runners.repopulate_emotions import assign_emotions_for_journey
from runners.generate_journey_transcript import execute as generate_journey_transcript
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, journey_uid: str):
    """Refreshes the emotions for the journey with the given uid. If the journey does
    not have a transcript, one is generated.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_uid (str): The UID of the journey to refresh the emotions for
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            content_files.uid,
            journeys.title,
            journeys.description, 
            instructors.name,
            EXISTS (
                SELECT 1 FROM content_file_transcripts
                WHERE content_file_transcripts.content_file_id = journeys.audio_content_file_id
            ) AS b1
        FROM journeys, content_files, instructors
        WHERE
            journeys.uid = ?
            AND content_files.id = journeys.audio_content_file_id
            AND instructors.id = journeys.instructor_id
        """,
        (journey_uid,),
    )
    if not response.results:
        logging.warning(
            f"There is no journey with {journey_uid=} to refresh the emotions for"
        )
        return

    content_file_uid: str = response.results[0][0]
    journey_title: str = response.results[0][1]
    journey_description: str = response.results[0][2]
    instructor_name: str = response.results[0][3]
    has_transcript: bool = response.results[0][4]

    if not has_transcript:
        await generate_journey_transcript(itgs, gd, journey_uid=journey_uid)
        if gd.received_term_signal:
            logging.debug("Received term signal, bouncing job")
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.refresh_journey_emotions", journey_uid=journey_uid
            )
            return

        transcript = await fetch_transcript_for_content_file(
            itgs, content_file_uid, consistency="weak"
        )
    else:
        transcript = None

    await assign_emotions_for_journey(
        itgs,
        journey_uid=journey_uid,
        content_file_uid=content_file_uid,
        title=journey_title,
        description=journey_description,
        instructor=instructor_name,
        transcript=transcript,
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        journey_uid = input("Journey UID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.refresh_journey_emotions", journey_uid=journey_uid
            )

    asyncio.run(main())
