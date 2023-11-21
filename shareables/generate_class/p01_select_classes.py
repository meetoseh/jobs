from dataclasses import dataclass
import json
import logging
import os
import secrets
from typing import Dict, List, Literal, Optional, Set, Tuple, Union, cast
from graceful_death import GracefulDeath
from itgs import Itgs
import random


@dataclass
class InputJourneyPromptWord:
    style: Literal["word"]
    text: str
    options: List[str]


@dataclass
class InputJourneyPromptNumeric:
    style: Literal["numeric"]
    text: str
    min: int
    max: int
    step: Literal[1]


@dataclass
class InputJourneyPromptColor:
    style: Literal["color"]
    text: str
    colors: List[str]


InputJourneyPrompt = Union[
    InputJourneyPromptWord, InputJourneyPromptNumeric, InputJourneyPromptColor
]


@dataclass
class InputJourney:
    title: str
    """The title of the journey"""

    description: str
    """A description of the journey"""

    instructor: str
    """The name of the instructor for the journey"""

    prompt: InputJourneyPrompt
    """The prompt for the journey"""

    audio_path: str
    """The file location where the original audio for the journey can be found"""


@dataclass
class SelectedJourneys:
    category: str
    emotion: str
    journeys: List[InputJourney]


def prompt_from_dict(prompt_obj: Dict[str, str]) -> InputJourneyPrompt:
    """Parses the prompt from the given dictionary, which should be as if it
    came from dataclasses.asdict(prompt)
    """
    if prompt_obj["style"] == "word":
        return InputJourneyPromptWord(**prompt_obj)  # type: ignore
    elif prompt_obj["style"] == "color":
        return InputJourneyPromptColor(**prompt_obj)  # type: ignore
    elif prompt_obj["style"] == "numeric":
        return InputJourneyPromptNumeric(**prompt_obj)  # type: ignore
    else:
        raise ValueError(f"Unknown prompt style: {prompt_obj['style']}")


async def select_classes(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    folder: str,
    category: Optional[str] = None,
    emotion: Optional[str] = None,
    exclude_categories: Optional[Set[str]] = None,
    exclude_emotions: Optional[Set[str]] = None,
) -> SelectedJourneys:
    """Selects which classes will be used as the examples in the prompt to the
    language model in order to generate the new class.

    This generates a list of 2-3 classes which all share a category and one
    emotion. The category and emotion is selected randomly (with rejection)
    unless specified.

    Args:
        itgs (Itgs): The integrations to (re)use
        gd (GracefulDeath): Used for signalling when to stop early
        folder (str): The path to the already existing folder where the source
            audio files can be downloaded to. The callee is responsible for
            cleaning up this folder when they are done with the journeys.
        category (str, None): If specified, the category that all the
            selected classes will share
        emotion (str, None): If specified, the emotion that all the selected
            classes will share
        exclude_categories (Set[str], None): If specified, when selecting
            the category, this set of categories will be excluded. Ignored if
            category is not None
        exclude_emotions (Set[str], None): If specified, when selecting
            the emotion, this set of emotions will be excluded. Ignored if
            emotion is not None

    Returns:
        List[InputJourneyPrompt]: 2-5 classes which all share a category and
            one emotion
    """
    can_reroll_category = category is None
    exclude_categories = exclude_categories or set()
    exclude_emotions = exclude_emotions or set()

    conn = await itgs.conn()
    cursor = conn.cursor("none")
    if category is None:
        response = await cursor.execute(
            """
            SELECT
                internal_name,
                COUNT(*)
            FROM journey_subcategories, journeys, content_files
            WHERE
                journeys.journey_subcategory_id = journey_subcategories.id
                AND journeys.deleted_at IS NULL
                AND journeys.special_category IS NULL
                AND journeys.audio_content_file_id = content_files.id
                AND content_files.original_s3_file_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM journeys AS j2
                    WHERE j2.audio_content_file_id = journeys.audio_content_file_id
                      AND j2.uid > journeys.uid
                      AND j2.deleted_at IS NULL
                )
            GROUP BY journey_subcategories.id
            """
        )
        subcategories = [
            cast(str, row[0])
            for row in response.results or []
            if row[1] > 2 and row[0] not in exclude_categories
        ]
        if not subcategories:
            raise ValueError(
                f"No subcategories found with more than 2 journeys; {exclude_categories=}"
            )
        category = random.choice(subcategories)

    if emotion is None:
        response = await cursor.execute(
            """
            SELECT
                emotions.word,
                COUNT(*)
            FROM emotions, journey_emotions, journeys, journey_subcategories, content_files
            WHERE
                journey_emotions.emotion_id = emotions.id
                AND journey_emotions.journey_id = journeys.id
                AND journeys.special_category IS NULL
                AND journeys.deleted_at IS NULL
                AND journey_subcategories.id = journeys.journey_subcategory_id
                AND journey_subcategories.internal_name = ?
                AND content_files.id = journeys.audio_content_file_id
                AND content_files.original_s3_file_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM journeys AS j2
                    WHERE j2.audio_content_file_id = journeys.audio_content_file_id
                      AND j2.uid > journeys.uid
                      AND j2.special_category IS NULL
                      AND j2.deleted_at IS NULL
                )
            GROUP BY emotions.id
            """,
            (category,),
        )
        emotions = [
            cast(str, row[0])
            for row in (response.results or [])
            if row[1] >= 2 and row[0] not in exclude_emotions
        ]
        if not emotions:
            if can_reroll_category:
                return await select_classes(
                    itgs,
                    gd=gd,
                    folder=folder,
                    exclude_categories=exclude_categories.union([category]),
                    exclude_emotions=exclude_emotions,
                )
            raise ValueError(
                f"No emotions found with more than 2 journeys in {category}: {exclude_emotions=}"
            )
        emotion = random.choice(emotions)

    batch_size = 50
    last_uid: Optional[str] = None

    chosen: List[Tuple[str, str, str, str, str, str]] = []
    num_seen = 0
    target_number = 3

    logging.debug(
        f"selecting up to {target_number} classes for {emotion} within {category} at random..."
    )

    # we don't want to force the database to do the random ordering so that
    # we can spread the load out across instances

    while True:
        if gd.received_term_signal:
            raise Exception("recieved term signal")
        response = await cursor.execute(
            """
            SELECT
                journeys.uid,
                journeys.title,
                journeys.description,
                instructors.name,
                interactive_prompts.prompt,
                s3_files.key
            FROM journeys, journey_subcategories, journey_emotions, emotions, interactive_prompts, content_files, s3_files, instructors
            WHERE
                journeys.journey_subcategory_id = journey_subcategories.id
                AND journey_subcategories.internal_name = ?
                AND journey_emotions.journey_id = journeys.id
                AND journey_emotions.emotion_id = emotions.id
                AND emotions.word = ?
                AND interactive_prompts.id = journeys.interactive_prompt_id
                AND content_files.id = journeys.audio_content_file_id
                AND s3_files.id = content_files.original_s3_file_id
                AND instructors.id = journeys.instructor_id
                AND journeys.special_category IS NULL
                AND journeys.deleted_at IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM journeys AS j2
                    WHERE j2.audio_content_file_id = journeys.audio_content_file_id
                        AND j2.uid > journeys.uid
                        AND j2.special_category IS NULL
                        AND j2.deleted_at IS NULL
                )
                AND (? IS NULL OR journeys.uid > ?)
            ORDER BY journeys.uid ASC
            LIMIT ?
            """,
            (category, emotion, last_uid, last_uid, batch_size),
        )

        if not response.results:
            break

        for row_uncasted in response.results:
            row = cast(Tuple[str, str, str, str, str, str], row_uncasted)
            if len(chosen) < target_number:
                chosen.append(row)
            elif random.randint(0, num_seen) < target_number:
                chosen[random.randint(0, target_number - 1)] = row
            num_seen += 1

        last_uid = response.results[-1][0]

        if len(response.results) < batch_size:
            break

    logging.debug(f"selected {len(chosen)} classes for {emotion} within {category}")
    result: List[InputJourney] = []

    files = await itgs.files()
    for uid, title, description, instructor, prompt_json, key in chosen:
        logging.debug(f"Downloading {title} ({uid})...")

        audio_path = os.path.join(folder, f"{secrets.token_urlsafe(16)}")
        with open(audio_path, "wb") as f:
            await files.download(f, bucket=files.default_bucket, key=key, sync=True)

        prompt_obj = json.loads(prompt_json)
        prompt = prompt_from_dict(prompt_obj)

        result.append(
            InputJourney(
                title=title,
                description=description,
                instructor=instructor,
                prompt=prompt,
                audio_path=audio_path,
            )
        )

    return SelectedJourneys(category=category, emotion=emotion, journeys=result)
