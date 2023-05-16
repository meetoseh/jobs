from dataclasses import dataclass
from typing import List
from graceful_death import GracefulDeath
from itgs import Itgs
from shareables.generate_class.p01_select_classes import InputJourney, SelectedJourneys
from shareables.journey_audio_with_dynamic_background.p06_transcript import (
    create_transcript,
)
from lib.transcripts.model import Transcript


@dataclass
class InputJourneyWithTranscript:
    journey: InputJourney
    transcript: Transcript


@dataclass
class SelectedJourneysWithTranscripts:
    emotion: str
    category: str
    journeys_with_transcripts: List[InputJourneyWithTranscript]


async def generate_transcripts(
    itgs: Itgs, *, gd: GracefulDeath, journeys: SelectedJourneys
) -> SelectedJourneysWithTranscripts:
    """Fetches or creates transcripts for each journey in the given list,
    and returns the augmented list.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): Used for signalling when to stop early
        journeys (list[InputJourney]): the journeys to augment

    Returns:
        list[InputJourneyWithTranscript]: the augmented journeys
    """

    result: List[InputJourneyWithTranscript] = []
    for journey in journeys.journeys:
        if gd.received_term_signal:
            raise Exception("recieved term signal")
        transcript = await create_transcript(
            itgs, journey.audio_path, instructor=journey.instructor
        )
        result.append(
            InputJourneyWithTranscript(journey=journey, transcript=transcript)
        )
    return SelectedJourneysWithTranscripts(
        emotion=journeys.emotion,
        category=journeys.category,
        journeys_with_transcripts=result,
    )
