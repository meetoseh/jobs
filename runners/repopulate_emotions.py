"""Repopulates the emotions table with a fixed list of emotions and then uses
chatgpt-turbo-3.5 to assign emotions to each journey. Works best with no more
than 16 emotions, but theoretically can work as long as there's enough token
space to fit the prompt and response.

This requires a transcript for each journey that is tagged; journeys without
transcripts are skipped.
"""
import datetime
import os
import re
import secrets
import string
from typing import AsyncIterator, FrozenSet, List, Literal, Optional, Set, Tuple
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from lib.emotions.emotion_content import purge_emotion_content_statistics_everywhere
from lib.redis_api_limiter import ratelimit_using_redis
from lib.transcripts.db import fetch_transcript_for_content_file
from lib.transcripts.model import Transcript
from lib.chatgpt.model import ChatCompletionMessage
from runners.backup_database import execute as backup_database
import openai
import json
import time

from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST

EMOTIONS: List[Tuple[str, str, Set[str]]] = [
    ("calm", "calm down", {"calm", "chill", "chilled", "calming"}),
    ("compassionate", "be compassionate", {"compassion", "compassionate"}),
    ("chill", "chill out", {"chill", "chilled", "calm", "calming"}),
    (
        "relaxed",
        "relax",
        {"relaxed", "relaxation", "relax", "peaceful", "peace", "at peace"},
    ),
    ("hopeful", "be hopeful", {"hope", "hopeful", "hopefulness"}),
    ("positive", "be positive", {"positive", "positivity", "uplifted", "renewed"}),
    ("focused", "be focused", {"focus", "focused", "reflective", "clear"}),
    ("energized", "be energized", {"energized", "energize", "energetic"}),
    (
        "inspired",
        "be inspired",
        {
            "inspired",
            "inspiration",
            "grateful",
            "curious",
            "renewed",
            "empowered",
            "motivated",
        },
    ),
    ("creative", "be creative", {"creative", "creativity", "playful"}),
    (
        "connected",
        "be connected",
        {"connected", "connection", "connect", "intimate", "connectedness"},
    ),
    ("open", "be open", {"open", "openness", "spiritual", "growth"}),
    (
        "grounded",
        "be grounded",
        {
            "grounded",
            "grounding",
            "grateful",
            "humble",
            "humility",
            "present",
            "groundedness",
        },
    ),
    ("loved", "feel loved", {"loved", "love", "loving", "self-love"}),
    ("balanced", "be balanced", {"balanced", "balance", "balanceful", "centered"}),
    ("content", "be content", {"contentful", "content"}),
    (
        "supported",
        "be supported",
        {"supported", "nurtured", "nurture", "support", "empowered"},
    ),
    ("valued", "feel valued", {"value", "valued", "worthy", "worthiness"}),
    (
        "safe",
        "be safe",
        {
            "safe",
            "secure",
            "secured",
            "security",
            "vulnerable",
            "inner strength",
            "strong",
            "steady",
        },
    ),
    ("confident", "be confident", {"confident", "confidence", "assertive"}),
    ("sleepy", "feel sleepy", {"sleepy", "sleep", "sleepiness"}),
]
PROMPT_VERSION = "1.0.3"
# semver for create_prompt_for_journey


def create_prompt_for_journey(
    title: str, description: str, instructor: str, transcript: Transcript
) -> List[ChatCompletionMessage]:
    """Creates the prompt that should be fed to chatgpt in order to generate
    the tags on the given journey.

    Args:
        title (str): The title of the journey
        description (str): The description of the journey
        instructor (str): The instructor of the journey
        transcript (Transcript): The transcript of the journey
    """
    numbered_tags_lines = []
    for i, (emotion, _, _) in enumerate(EMOTIONS):
        numbered_tags_lines.append(f"{i + 1}. {emotion}")
    numbered_tags = "\n".join(numbered_tags_lines)

    joined_transcript = " ".join([txt for (_, txt) in transcript.phrases])

    return [
        {
            "role": "system",
            "content": f"""Your role in this task is to determine what feelings
the class evokes. You must output a numbered list containing at least 2
emotions. You must only choose amongst the specified emotions. The classes are
designed such that there are always at least three matching emotions, so you must
select at least three emotions to get full credit.

The feelings that classes can resolve are:

{numbered_tags}

===

The following is an example of a valid response:

This class asks you to connect with your inner child, which is generally associated
with feelings of being playful, happy, and care-free. Hence, the class is designed
to evoke the following feelings:

1. valued
2. loved
3. creative
""",
        },
        {
            "role": "user",
            # In the dev environment there aren't useful descriptions so it mainly just causes
            # confusion, but in the prod environment the descriptions are meaningful and thus
            # can greatly help the model.
            "content": joined_transcript
            if os.environ["ENVIRONMENT"] == "dev"
            else (
                f"{title} by {instructor}: {description}\n\n===\n\n{joined_transcript}"
            ),
        },
    ]


def parse_emotions(
    completion: str, *, dropped: Optional[Set[str]] = None
) -> FrozenSet[str]:
    """Parses the emotions from the completion provided by the
    completions api.

    Args:
        completion (str): the completion to parse
        dropped (set[str], None): If specified, any dropped emotions are
            included in this set, which can be useful for identifying emotions
            we should have in our list but don't.

    Returns:
        List[str]: the selected emotion tags

    Raises:
        ValueError: if the completion does not contain a numbered list
    """
    state: Literal["before_list", "in_list"] = "before_list"
    current_item: Optional[str] = None
    raw_emotions: List[str] = []

    item_regex = re.compile(r"^\s*(\d+)\.(.+)$")

    for line in completion.splitlines():
        item_match = item_regex.match(line)
        if state == "before_list":
            if item_match:
                if int(item_match.group(1)) != 1:
                    raise ValueError(
                        f"Invalid completion: {completion} (expected start of list, got {line})"
                    )
                state = "in_list"
                current_item = item_match.group(2)
        elif state == "in_list":
            if item_match:
                raw_emotions.append(current_item)
                if int(item_match.group(1)) != len(raw_emotions) + 1:
                    raise ValueError(
                        f"Invalid completion: {completion} (expected item {len(raw_emotions) + 1}, got {line})"
                    )
                current_item = item_match.group(2)
            else:
                current_item += " " + line

    if current_item:
        raw_emotions.append(current_item)

    if not raw_emotions:
        raise ValueError(f"Invalid completion: {completion} (no emotions selected)")

    valid_emotions = set()
    for suggested_emotion in raw_emotions:
        suggested_emotion = suggested_emotion.lower().strip(
            string.whitespace + string.punctuation
        )
        found = False
        for base, _, aliases in EMOTIONS:
            if suggested_emotion == base or suggested_emotion in aliases:
                valid_emotions.add(base)
                found = True

        if not found:
            logging.debug(
                f"Dropping completed emotion {suggested_emotion=}: not a valid tag"
            )
            if dropped is not None:
                dropped.add(suggested_emotion)

    if len(valid_emotions) < 2:
        raise ValueError(
            f"Invalid completion: {completion} (only {len(valid_emotions)} valid emotions selected)"
        )

    return frozenset(valid_emotions)


def _temperature_for_attempt(attempt: int) -> float:
    """Gets the temperature used in the openai completion model for the given
    attempt, where attempt 0 is the first attempt.
    """
    return 1.0 + (attempt * 0.1) * (-1 if attempt % 2 == 0 else 1)


async def get_emotions_for_journey(
    itgs: Itgs,
    *,
    uid: str,
    title: str,
    description: str,
    instructor: str,
    transcript: Transcript,
    max_completion_retries: int = 5,
    dropped: Optional[Set[str]] = None,
) -> FrozenSet[str]:
    """Uses chatgpt to generate the emotions for the given journey.

    Args:
        itgs (Itgs): The integrations to (re)use
        uid (str): The uid of the journey, for logging purposes
        title (str): The title of the journey
        description (str): The description of the journey
        instructor (str): The instructor of the journey
        transcript (Transcript): The transcript of the journey
        max_completion_retries (int): The maximum number of times to
            retry the completion if it fails.
        dropped (set[str], None): If specified, any dropped emotions are
            included in this set, which can be useful for identifying emotions
            we should have in our list but don't.


    Returns:
        FrozenSet[str]: The emotions that were generated
    """

    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
    messages = create_prompt_for_journey(title, description, instructor, transcript)
    logging.info(
        f"Creating tags for {title} by {instructor} using the following prompt:\n\n{json.dumps(messages, indent=2)}"
    )
    attempt = 0
    while True:
        try:
            await ratelimit_using_redis(
                itgs,
                key="external_apis:api_limiter:chatgpt",
                time_between_requests=3,
            )
            completion = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=_temperature_for_attempt(attempt),
                api_key=openai_api_key,
            )
            logging.info(f"Got completion:\n\n{completion}")
            emotions = parse_emotions(
                completion.choices[0].message.content, dropped=dropped
            )
            logging.info(f"Parsed emotions: {json.dumps(list(emotions))}")
            return emotions
        except Exception:
            attempt += 1
            if attempt >= max_completion_retries:
                raise ValueError(
                    f"Unable to create emotions for {title} by {instructor} ({uid=})"
                )
            logging.info("Failed to parse completion, retrying...")


async def store_emotions_for_journey(
    itgs: Itgs, *, uid: str, title: str, instructor: str, emotions: List[str]
) -> None:
    """Associates the journey with the given uid with the given emotions, deleting
    any previous associations.

    Args:
        itgs (Itgs): The integrations to (re)use
        uid (str): The uid of the journey
        title (str): The title of the journey, for logging
        instructor (str): The instructor of the journey, for logging
        emotions (List[str]): The emotions to associate with the journey

    Raises:
        ValueError: If the journey does not exist or cannot be associated with
            the given emotions
    """
    conn = await itgs.conn()
    cursor = conn.cursor()

    journey_emotion_uids = list(
        f"oseh_je_{secrets.token_urlsafe(16)}" for _ in emotions
    )
    now = time.time()

    qargs = []
    for je_uid, emotion in zip(journey_emotion_uids, emotions):
        qargs.extend((je_uid, emotion))
    qargs.append(
        json.dumps(
            {"type": "ai", "model": "gpt-3.5-turbo", "prompt_version": PROMPT_VERSION}
        )
    )
    qargs.append(now)
    qargs.append(uid)

    values_list = ", ".join(list("(?, ?)" for _ in emotions))
    response = await cursor.executemany3(
        (
            (
                """
                DELETE FROM journey_emotions
                WHERE
                    EXISTS (
                        SELECT 1 FROM journeys
                        WHERE journeys.id = journey_emotions.journey_id
                          AND journeys.uid = ?
                    )
                """,
                (uid,),
            ),
            (
                f"""
                WITH new_emotions(uid, emotion) AS (VALUES {values_list})
                INSERT INTO journey_emotions (
                    uid, 
                    journey_id, 
                    emotion_id, 
                    creation_hint, 
                    created_at
                )
                SELECT
                    new_emotions.uid,
                    journeys.id,
                    emotions.id,
                    ?,
                    ?
                FROM new_emotions, journeys, emotions
                WHERE
                    journeys.uid = ?
                    AND emotions.word = new_emotions.emotion
                """,
                qargs,
            ),
        )
    )
    if response[1].rows_affected is None or response[1].rows_affected != len(emotions):
        raise ValueError(
            f"Unable to associate all emotions with journey {uid=}: only {response[1].rows_affected=} stored"
        )

    await purge_emotion_content_statistics_everywhere(itgs)
    logging.info(
        f"Stored {len(emotions)} emotions for journey {title} by {instructor} ({uid=}): {emotions}"
    )


async def iter_journeys_for_emotions(
    itgs: Itgs, *, batch_size: int = 50
) -> AsyncIterator[Tuple[str, str, str, str]]:
    """Iterates journeys which are not deleted and are not in a course,
    for which a transcript does exist.

    Yields (uid, content_file_uid, title, description, instructor) for each journey.

    Args:
        itgs (Itgs): The integrations to (re)use
        batch_size (int): The maximum number of journeys to keep in memory
            at once. A higher value reduces the overall number of queries
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    last_uid: Optional[str] = None

    while True:
        response = await cursor.execute(
            """
            SELECT
                journeys.uid,
                content_files.uid,
                journeys.title,
                journeys.description,
                instructors.name
            FROM journeys, content_files, instructors
            WHERE
                journeys.instructor_id = instructors.id
                AND journeys.deleted_at IS NULL
                AND content_files.id = journeys.audio_content_file_id
                AND EXISTS (
                    SELECT 1 FROM content_file_transcripts
                    WHERE content_file_transcripts.content_file_id = journeys.audio_content_file_id
                )
                AND (
                    ? IS NULL
                    OR journeys.uid > ?
                )
            ORDER BY journeys.uid
            LIMIT ?
            """,
            (last_uid, last_uid, batch_size),
        )
        if not response.results:
            return

        for row in response.results:
            yield row

        if len(response.results) < batch_size:
            return

        last_uid = response.results[-1][0]


async def assign_emotions_for_journey(
    itgs: Itgs,
    *,
    journey_uid: str,
    content_file_uid: str,
    title: str,
    description: str,
    instructor: str,
    transcript: Optional[Transcript] = None,
    dropped: Optional[Set[str]] = None,
) -> None:
    """Assigns emotions to the given journey and stores them in the database. If the
    journey already has emotions, they are deleted before the new emotions are assigned.

    If the journey doesn't have a transcript or we fail to select emotions to
    assign, this does nothing.

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The uid of the journey
        content_file_uid (str): The uid of the content file for the journey
        title (str): The title of the journey
        description (str): The description of the journey
        instructor (str): The instructor of the journey
        transcript (Transcript, None): The transcript, or None to have it fetched
            from the database
        dropped (Set[str], None): If specified, whenever we find tags that might
            be appropriate for content but for which there is no corresponding emotion,
            we add them to this set
    """
    if transcript is None:
        transcript = await fetch_transcript_for_content_file(
            itgs, content_file_uid=content_file_uid, consistency="none"
        )
    if transcript is None:
        logging.warning(
            f"Skipping journey {title} by {instructor} ({journey_uid=}): no transcript? (should have been filtered out)"
        )
        return

    try:
        emotions = await get_emotions_for_journey(
            itgs,
            uid=journey_uid,
            title=title,
            description=description,
            instructor=instructor,
            transcript=transcript,
            dropped=dropped,
        )
    except ValueError:
        logging.error(
            f"ValueError while assigning emotions for {title} by {instructor} ({journey_uid=})",
            exc_info=True,
        )
        slack = await itgs.slack()
        await slack.send_web_error_message(
            f"Failed to assign emotions for {title} by {instructor} (`{journey_uid}`) using transcript:\n\n```\n{transcript}\n```",
            preview=f"Failed to assign emotions for {title}",
        )
        return

    await store_emotions_for_journey(
        itgs,
        uid=journey_uid,
        title=title,
        instructor=instructor,
        emotions=list(emotions),
    )


async def execute(itgs: Itgs, gd: GracefulDeath):
    async def _bounce():
        logging.debug("Received term signal, bouncing")
        jobs = await itgs.jobs()
        await jobs.enqueue("runners.repopulate_emotions")

    async with basic_redis_lock(itgs, "jobs:repopulate_emotions:lock"):
        if gd.received_term_signal:
            await _bounce()
            return
        logging.info("Repopulating emotions. Starting with database backup...")
        await backup_database(itgs, gd, hint="-repopulate_emotions")
        if gd.received_term_signal:
            await _bounce()
            return

        logging.debug("Removing old emotions...")
        conn = await itgs.conn()
        cursor = conn.cursor("weak")
        await cursor.execute("DELETE FROM emotions")
        logging.info(f"Removed {cursor.rows_affected or 0} old emotions")

        logging.debug("Uploading new emotions...")
        values_list = ", ".join(["(?, ?)"] * len(EMOTIONS))
        response = await cursor.execute(
            f"INSERT INTO emotions (word, antonym) VALUES {values_list}",
            [a for e in EMOTIONS for a in e[:2]],
        )
        if response.rows_affected != len(EMOTIONS):
            raise ValueError(
                f"Unable to insert all emotions: {response.rows_affected=} of {len(EMOTIONS)=}"
            )
        logging.info(f"Uploaded {len(EMOTIONS)} new emotions")

        logging.debug("Assigning emotions to journeys...")
        dropped: Set[str] = set()
        async for journey_uid, content_file_uid, title, description, instructor in iter_journeys_for_emotions(
            itgs
        ):
            if gd.received_term_signal:
                await _bounce()
                return

            logging.info(f"Assigning emotions for {journey_uid=}")
            await assign_emotions_for_journey(
                itgs,
                journey_uid=journey_uid,
                content_file_uid=content_file_uid,
                title=title,
                description=description,
                instructor=instructor,
                dropped=dropped,
            )

        logging.info("Done repopulating emotions, fetching stats...")

        response = await cursor.execute(
            """
            SELECT
                emotions.word,
                COUNT(*)
            FROM emotions, journey_emotions
            WHERE
                journey_emotions.emotion_id = emotions.id
            GROUP BY emotions.id
            """
        )
        emotion_counts = {row[0]: row[1] for row in response.results}

        for emotion, _, _ in EMOTIONS:
            if emotion not in emotion_counts:
                emotion_counts[emotion] = 0

        emotion_counts = {
            k: v
            for k, v in sorted(
                emotion_counts.items(), key=lambda item: item[1], reverse=True
            )
        }

        logging.info(f"Emotion counts: {json.dumps(emotion_counts, indent=2)}")
        logging.info(f"Dropped emotions: {json.dumps(sorted(dropped), indent=2)}")

        if os.environ["ENVIRONMENT"] != "dev":
            slack = await itgs.slack()
            await slack.send_ops_message(
                f"Emotion counts:\n\n```\n{json.dumps(emotion_counts, indent=2)}\n```\n",
                preview="Emotions repopulated successfully",
            )

            if len(dropped) > 20:
                key = f"s3_files/jobs/repopulate_emotions/dropped_{datetime.datetime.now().isoformat()}.txt"
                with temp_file() as path:
                    with open(path, "w") as f:
                        for tag in sorted(dropped):
                            print(tag, file=f)

                    files = await itgs.files()
                    with open(path, "rb") as f:
                        await files.upload(
                            f, bucket=files.default_bucket, key=key, sync=True
                        )

                    await slack.send_ops_message(
                        f"Tags that were dropped were stored at {key=} ({len(dropped)} emotions)"
                    )
            else:
                await slack.send_ops_message(
                    f"Dropped tags:\n\n```\n{json.dumps(sorted(dropped), indent=2)}\n```\n"
                )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.repopulate_emotions")

    asyncio.run(main())
