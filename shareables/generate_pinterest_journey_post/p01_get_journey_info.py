from itgs import Itgs
from graceful_death import GracefulDeath
from dataclasses import dataclass
from lib.transcripts.model import Transcript
from lib.transcripts.db import fetch_transcript_for_content_file
from runners.generate_journey_transcript import execute as generate_transcript
from shareables.shareable_pipeline_exception import ShareablePipelineException


class GetJourneyInfoError(ShareablePipelineException):
    def __init__(self, msg: str):
        super().__init__(msg, step_number=1, step_name="get_journey_info")


@dataclass
class PinterestJourneyForPost:
    uid: str
    """The uid of the journey"""

    title: str
    """The journey title"""

    description: str
    """The journey description"""

    instructor: str
    """The name of the instructor for the journey"""

    category: str
    """The internal name of the category of the journey"""

    transcript: Transcript
    """The full transcript of the journey"""

    @classmethod
    def from_dict(c, raw: dict):
        return c(
            uid=raw["uid"],
            title=raw["title"],
            description=raw["description"],
            instructor=raw["instructor"],
            category=raw["category"],
            transcript=Transcript.from_dict(raw["transcript"]),
        )


async def get_journey_info(
    itgs: Itgs, *, gd: GracefulDeath, journey_uid: str
) -> PinterestJourneyForPost:
    """Gets the information about the journey to use for the pin.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): the graceful death context
        journey_uid (str): the uid of the journey to use for the pin

    Returns:
        PinterestJourneyForPost: the information about the journey to use
            for the pin
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            journeys.title,
            journeys.description,
            instructors.name,
            content_files.uid,
            journey_subcategories.internal_name
        FROM journeys, instructors, content_files, journey_subcategories
        WHERE
            journeys.uid = ?
            AND journeys.instructor_id = instructors.id
            AND journeys.audio_content_file_id = content_files.id
            AND journeys.journey_subcategory_id = journey_subcategories.id
        """,
        (journey_uid,),
    )
    if not response.results:
        raise GetJourneyInfoError(f"no journey found with uid {journey_uid}")

    journey_title: str = response.results[0][0]
    journey_description: str = response.results[0][1]
    instructor_name: str = response.results[0][2]
    content_file_uid: str = response.results[0][3]
    category: str = response.results[0][4]

    transcript = await fetch_transcript_for_content_file(
        itgs, content_file_uid, consistency="none"
    )
    if transcript is None:
        await generate_transcript(itgs, gd, journey_uid=journey_uid)
        transcript = await fetch_transcript_for_content_file(
            itgs, content_file_uid, consistency="weak"
        )
        if transcript is None:
            raise GetJourneyInfoError(
                f"no transcript found for content file {content_file_uid}, journey {journey_uid}"
            )

    return PinterestJourneyForPost(
        uid=journey_uid,
        title=journey_title,
        description=journey_description,
        instructor=instructor_name,
        category=category,
        transcript=transcript,
    )
