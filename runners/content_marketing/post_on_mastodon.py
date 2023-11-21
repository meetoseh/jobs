import json
import secrets
import time
from typing import List, Optional
import openai

import pytz
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import os
import textwrap
import string
from lib.redis_api_limiter import ratelimit_using_redis
from lib.transcripts.db import fetch_transcript_for_content_file
from lib.transcripts.model import Transcript
from dataclasses import dataclass
import requests
from openai.types.chat import ChatCompletionMessageParam
import unix_dates
import asyncio


category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, dry_run: Optional[bool] = None):
    """Makes a new post on /r/oseh using an AI video, if there is one that hasn't been
    posted yet.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        dry_run (bool, None): True to not actually post anything, false to post,
            None to decide based on the current environment (i.e., post only in prod)
    """
    og_dry_run = dry_run

    async def _bounce():
        logging.info("Received term signal, bouncing post on mastodon job")
        jobs = await itgs.jobs()
        await jobs.enqueue("content_marketing.post_on_mastodon", dry_run=og_dry_run)

    if dry_run is None:
        dry_run = os.environ["ENVIRONMENT"] != "production"

    mastodon_access_token = os.environ["OSEH_MASTODON_ACCESS_TOKEN"]

    logging.info("Selecting which journey to post on mastodon...")
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            journeys.title,
            journeys.description,
            journeys.uid,
            content_files.uid,
            journey_subcategories.internal_name,
            instructors.name
        FROM journeys, content_files, journey_subcategories, instructors
        WHERE
            journeys.special_category = 'ai'
            AND journeys.deleted_at IS NULL
            AND journeys.sample_content_file_id IS NOT NULL
            AND journeys.video_content_file_id IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM journey_mastodon_posts, journey_public_links
                WHERE
                    journey_public_links.id = journey_mastodon_posts.journey_public_link_id
                    AND journey_public_links.journey_id = journeys.id
            )
            AND EXISTS (
                SELECT 1 FROM content_file_transcripts
                WHERE content_file_transcripts.content_file_id = journeys.audio_content_file_id
            )
            AND content_files.id = journeys.audio_content_file_id
            AND journey_subcategories.id = journeys.journey_subcategory_id
            AND instructors.id = journeys.instructor_id
        ORDER BY journeys.created_at DESC
        LIMIT 1
        """
    )
    if not response.results:
        await handle_contextless_error(
            extra_info="Cannot make a mastodon post: no AI journeys found that are ready and haven't been posted yet",
        )
        return

    journey_title: str = response.results[0][0]
    journey_description: str = response.results[0][1]
    journey_uid: str = response.results[0][2]
    content_file_uid: str = response.results[0][3]
    category: str = response.results[0][4]
    instructor_name: str = response.results[0][5]

    transcript = await fetch_transcript_for_content_file(
        itgs, content_file_uid, consistency="weak"
    )
    if transcript is None:
        await handle_contextless_error(
            extra_info=f"Cannot make a mastodon post: failed to load transcript for journey {journey_uid}"
        )
        return

    response = await cursor.execute(
        """
        SELECT
            emotions.word
        FROM emotions
        WHERE
            EXISTS (
                SELECT 1 FROM journey_emotions, journeys
                WHERE
                    journey_emotions.journey_id = journeys.id
                    AND journeys.uid = ?
                    AND journey_emotions.emotion_id = emotions.id
            )
        """,
        (journey_uid,),
    )
    emotions: List[str] = [row[0] for row in response.results or []]

    status = textwrap.shorten(f"{journey_title}: {journey_description}", width=300)
    logging.info("Attempting to come up with a creative status text...")
    for attempt in range(5):
        if gd.received_term_signal:
            await _bounce()
            return
        try:
            post_info = await create_post_info(
                itgs,
                journey_title=journey_title,
                journey_description=journey_description,
                category=category,
                instructor_name=instructor_name,
                emotions=emotions,
                transcript=transcript,
                attempt=attempt,
            )
            status = post_info.text
            break
        except Exception:
            if attempt == 4:
                raise
            logging.exception(
                f"Failed to come up with a creative status (attempt {attempt+1/5}), trying again.."
            )
            await asyncio.sleep(5)

    jpl_uid = f"oseh_jpl_{secrets.token_urlsafe(16)}"
    jpl_code = make_code(journey_title)
    jpl_url = os.environ["ROOT_FRONTEND_URL"] + "/jpl?code=" + jpl_code

    status = f"{status}\n\n{jpl_url}"

    now = time.time()
    response = await cursor.execute(
        """
        INSERT INTO journey_public_links (
            uid, code, journey_id, created_at, deleted_at
        )
        SELECT
            ?, ?, journeys.id, ?, NULL
        FROM journeys
        WHERE
            journeys.uid = ?
        """,
        (jpl_uid, jpl_code, now, journey_uid),
    )
    if response.rows_affected is None or response.rows_affected < 1:
        await handle_contextless_error(
            extra_info="Cannot make a mastodon post: failed to insert the new journey public link",
        )
        return

    logging.info(f"Posting on mastodon:\n{status}")
    if dry_run:
        logging.debug("Dry run, not actually posting")
        return

    idempotency_key = secrets.token_urlsafe(16)
    submission_id = None
    submission_uri = None
    submission_author = None
    for attempt in range(5):
        try:
            response = requests.post(
                "https://mastodon.social/api/v1/statuses",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {mastodon_access_token}",
                    "Idempotency-Key": idempotency_key,
                },
                json={
                    "status": status,
                    "visibility": "public",
                    "language": "en",
                },
            )
            response.raise_for_status()

            response_json = response.json()
            submission_id = response_json["id"]
            submission_uri = response_json["uri"]
            submission_author = response_json["account"]["url"]
            break
        except Exception:
            if attempt == 4:
                raise
            logging.exception(
                f"Failed to post on mastodon (attempt {attempt+1}/5), trying again.."
            )
            await asyncio.sleep(5)

    jmp_uid = f"oseh_jmp_{secrets.token_urlsafe(16)}"
    assert submission_id is not None
    assert submission_uri is not None
    assert submission_author is not None
    response = await cursor.execute(
        """
        INSERT INTO journey_mastodon_posts (
            uid, journey_public_link_id, status_id, permalink,
            status, author, created_at
        )
        SELECT
            ?, journey_public_links.id, ?, ?, ?, ?, ?
        FROM journey_public_links
        WHERE
            journey_public_links.uid = ?
        """,
        (
            jmp_uid,
            submission_id,
            submission_uri,
            status,
            submission_author,
            now,
            jpl_uid,
        ),
    )
    if response.rows_affected is None or response.rows_affected < 1:
        await handle_contextless_error(
            extra_info=f"Failed to store the mastodon post in the database: {submission_uri}"
        )

    logging.info(f"Successfully posted on mastodon: {submission_uri}")

    slack = await itgs.slack()
    await slack.send_ops_message(f"Posted on mastodon: {submission_uri}")


@dataclass
class MastodonPostInfo:
    text: str


def create_prompt_for_llm(
    *,
    journey_title: str,
    journey_description: str,
    instructor_name: str,
    category: str,
    emotions: List[str],
    transcript: Transcript,
) -> List[ChatCompletionMessageParam]:
    """Creates the prompt for chatgpt to create a status for the given journey."""
    tz = pytz.timezone("America/Los_Angeles")
    unix_day_today = unix_dates.unix_date_today(tz=tz)
    date_today = unix_dates.unix_date_to_date(unix_day_today)

    date_str = date_today.strftime("%A, %B %d")
    transcript_str = " ".join([p[1] for p in transcript.phrases])
    emotions_info = "" if not emotions else f"Emotions: {', '.join(emotions)}"

    return [
        {
            "role": "system",
            "content": f"""As a social media manager on mastodon, you come up with statuses for new content
that was just generated on Oseh. Mastodon statuses are like tweets, but they are
up to 500 characters. You do not need to include a link, since it will be
automatically included by the system.

Oseh is a daily meditation platform where you pick an emotion and receive
content designed to evoke that emotion: soundbaths, meditations, mindful talks,
and more. When we link content from social media, we link directly to a class
that was ai-generated, but users can sign up to the platform to receive content
that was created by people. The primary goal of these statuses is to get people
to signup, with the secondary goal to follow the mastodon account.

You should return mastodon posts in the following format:

===

{{"text": "This is my mastodon post text"}}

===

Good posts are succinct, avoid repetition, only lightly describe the content,
have a hook to get people interested, and use relevant hashtags to improve
discoverability. Follow the style of the following high-quality mastodon posts:

===

{{"text": "This meditation was created completely by AI! It's designed to practice a 3-part breath. Try it out, then you can signup to see the ones created by people and compare. \\n\\n#meditation #wellness #selfcare #lovingkindness"}}
{{"text": "This is almost scary - practice these affirmations that a computer thought up.\\n\\n#affirmations #wellness #mirrortime #AffirmationFriday #gratitude #GratefulFriday"}}

===

Today is {date_str}
""",
        },
        {
            "role": "user",
            "content": f"""Generate a mastodon post for the following ai-generated {category} journey:
            
Title: {journey_title}: 
Description: {journey_description}
Instructor: {instructor_name}
{emotions_info}
Transcript: {transcript_str}
""",
        },
    ]


def parse_completion(completion: str) -> MastodonPostInfo:
    """Parses the post that was included in the given completion from the api,
    or raises an exception if the completion is invalid.
    """
    json_starts_at = completion.index("{")
    json_ends_at = completion.rindex("}")

    json_str = completion[json_starts_at : json_ends_at + 1]
    json_obj = json.loads(json_str)
    assert isinstance(json_obj, dict), completion
    assert isinstance(json_obj.get("text"), str), completion
    text = json_obj["text"]
    assert len(text) > 30 and len(text) < 450, text
    return MastodonPostInfo(text=text)


def _temperature_for_attempt(attempt: int) -> float:
    """Gets the temperature used in the openai completion model for the given
    attempt, where attempt 0 is the first attempt.
    """
    return 1.0 + (((attempt + 1) // 2) * 0.05) * (-1 if attempt % 2 == 0 else 1)


async def create_post_info(
    itgs: Itgs,
    *,
    journey_title: str,
    journey_description: str,
    category: str,
    instructor_name: str,
    emotions: List[str],
    transcript: Transcript,
    attempt: int,
) -> MastodonPostInfo:
    """Attempts to come up with a tailored mastodon post for the given journey.
    This does not retry on failure and hence should be used in a loop that
    retries at least a few times.
    """
    prompt = create_prompt_for_llm(
        journey_title=journey_title,
        journey_description=journey_description,
        category=category,
        instructor_name=instructor_name,
        emotions=emotions,
        transcript=transcript,
    )
    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
    openai_client = openai.Client(api_key=openai_api_key)
    await ratelimit_using_redis(
        itgs,
        key="external_apis:api_limiter:chatgpt",
        time_between_requests=3,
    )
    completion = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=prompt,
        temperature=_temperature_for_attempt(attempt),
    )
    logging.info(f"Got completion:\n\n{completion}")
    best_choice = completion.choices[0]
    best_choice_message = best_choice.message
    best_content = best_choice_message.content
    assert best_content is not None, completion
    return parse_completion(best_content)


def make_code(text: str) -> str:
    """Makes a url-safe code from the given text. All non-ascii characters are
    removed, then the text is shortened to at most 16 characters, and all spaces
    are replaced with hyphens, and all other non-alphanumeric characters are
    removed, then a short random suffix is added
    """
    text = "".join(
        (c if c != " " else "-") for c in text if c in string.ascii_letters or c == " "
    )
    text = text[:16]
    text = text.rstrip("-")
    text += "-" + secrets.token_urlsafe(4)
    return text


if __name__ == "__main__":
    import argparse

    async def main():
        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--no-dry-run", action="store_true")
        args = parser.parse_args()

        dry_run = None
        if args.dry_run:
            dry_run = True
        if args.no_dry_run:
            dry_run = False

        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.content_marketing.post_on_mastodon", dry_run=dry_run
            )

    asyncio.run(main())
