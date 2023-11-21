"""Provides insight into what emotions users are selecting within the app, and
how that relates to what content we have, then posts that information to slack
"""
import os
from typing import Dict, List, Set, Tuple, cast
from error_middleware import handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import pytz
from lib.redis_api_limiter import ratelimit_using_redis
import unix_dates
import asyncio
import openai

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Calculates emotion selections, all time and recent, and relates them to how much
    content is eligible for each emotion, then posts that information to slack

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            emotions.word,
            COUNT(*)
        FROM journeys, journey_emotions, emotions
        WHERE
            journeys.id = journey_emotions.journey_id
            AND journey_emotions.emotion_id = emotions.id
            AND journeys.deleted_at IS NULL
            AND NOT EXISTS (
                SELECT 1 FROM course_journeys
                WHERE course_journeys.journey_id = journeys.id
            )
        GROUP BY emotions.id
        UNION ALL
        SELECT
            emotions.word,
            0
        FROM emotions
        WHERE
            NOT EXISTS (
                SELECT 1 FROM journey_emotions, journeys
                WHERE
                    journey_emotions.emotion_id = emotions.id
                    AND journey_emotions.journey_id = journeys.id
                    AND journeys.deleted_at IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM course_journeys
                        WHERE course_journeys.journey_id = journeys.id
                    )
            )
        """
    )

    emotion_word_to_content_count: List[Tuple[str, int]] = [
        cast(Tuple[str, int], tuple(row)) for row in (response.results or [])
    ]
    emotion_word_to_content_count_dict = dict(emotion_word_to_content_count)
    if not emotion_word_to_content_count:
        logging.info("No emotions found, skipping slack emotion selections")
        return

    all_emotion_words: Set[str] = set(row[0] for row in emotion_word_to_content_count)
    assert len(all_emotion_words) == len(
        emotion_word_to_content_count
    ), "Duplicate emotion words found"

    response = await cursor.execute(
        """
        SELECT
            COUNT(*)
        FROM journeys
        WHERE
            journeys.deleted_at IS NULL
            AND NOT EXISTS (
                SELECT 1 FROM course_journeys
                WHERE course_journeys.journey_id = journeys.id
            )
        """
    )
    assert response.results, response
    total_num_eligible_content = response.results[0][0]

    emotion_word_to_content_count.sort(key=lambda x: x[1], reverse=True)

    content_distribution = "\n".join(
        f" •  {word}: {count} ({count / total_num_eligible_content:.2%})"
        for word, count in emotion_word_to_content_count
    )
    logging.info(f"Content distribution:\n\n{content_distribution}")

    response = await cursor.execute(
        """
        SELECT
            emotions.word,
            COUNT(*)
        FROM emotions, emotion_users
        WHERE
            emotions.id = emotion_users.emotion_id
            AND json_extract(emotion_users.status, '$.type') = 'joined'
        GROUP BY emotions.id
        """
    )

    all_time_emotion_word_to_user_count_dict = cast(
        Dict[str, int], dict(response.results or [])
    )
    for word in all_emotion_words:
        if word not in all_time_emotion_word_to_user_count_dict:
            all_time_emotion_word_to_user_count_dict[word] = 0

    all_time_emotion_word_to_user_count: List[Tuple[str, int]] = list(
        all_time_emotion_word_to_user_count_dict.items()
    )
    all_time_emotion_word_to_user_count.sort(key=lambda x: x[1], reverse=True)

    total_num_emotion_word_users = max(
        sum(row[1] for row in all_time_emotion_word_to_user_count), 1
    )

    all_time_selection_distribution = "\n".join(
        f" •  {word}: {count} ({count / total_num_emotion_word_users:.2%})"
        for word, count in all_time_emotion_word_to_user_count
    )

    logging.info(
        f"All time selection distribution:\n\n{all_time_selection_distribution}"
    )

    tz = pytz.timezone("America/Los_Angeles")
    unix_date_today = unix_dates.unix_date_today(tz=tz)
    unix_date_week_ago = unix_date_today - 7
    unix_time_midnight_am_week_ago = unix_dates.unix_date_to_timestamp(
        unix_date_week_ago, tz=tz
    )

    response = await cursor.execute(
        """
        SELECT
            emotions.word,
            COUNT(*)
        FROM emotions, emotion_users
        WHERE
            emotions.id = emotion_users.emotion_id
            AND json_extract(emotion_users.status, '$.type') = 'joined'
            AND emotion_users.created_at > ?
        GROUP BY emotions.id
        """,
        (unix_time_midnight_am_week_ago,),
    )

    recent_emotion_word_to_user_count_dict = cast(
        Dict[str, int], dict(response.results or [])
    )
    for word in all_emotion_words:
        if word not in recent_emotion_word_to_user_count_dict:
            recent_emotion_word_to_user_count_dict[word] = 0

    recent_emotion_word_to_user_count: List[Tuple[str, int]] = list(
        recent_emotion_word_to_user_count_dict.items()
    )
    recent_emotion_word_to_user_count.sort(key=lambda x: x[1], reverse=True)

    total_num_recent_emotion_word_users = max(
        sum(row[1] for row in recent_emotion_word_to_user_count), 1
    )

    recent_selection_distribution = "\n".join(
        f" •  {word}: {count} ({count / total_num_recent_emotion_word_users:.2%})"
        for word, count in recent_emotion_word_to_user_count
    )

    logging.info(f"Recent selection distribution:\n\n{recent_selection_distribution}")

    all_time_word_to_absolute_inconsistency: List[Tuple[str, float]] = [
        (
            word,
            abs(
                word_num_emotion_users / total_num_emotion_word_users
                - emotion_word_to_content_count_dict[word] / total_num_eligible_content
            ),
        )
        for word, word_num_emotion_users in all_time_emotion_word_to_user_count
    ]
    all_time_word_to_absolute_inconsistency.sort(key=lambda x: x[1], reverse=True)

    all_time_abs_diff_selection_distribution = "\n".join(
        f" •  {word}: {abs_diff:.2%} ([{all_time_emotion_word_to_user_count_dict[word]}/{total_num_emotion_word_users} selections to {emotion_word_to_content_count_dict[word]}/{total_num_eligible_content} pieces of content]"
        for word, abs_diff in all_time_word_to_absolute_inconsistency
    )

    logging.info(
        f"All-time absolute difference selection distribution:\n\n{all_time_abs_diff_selection_distribution}"
    )

    recent_word_to_absolute_inconsistency: List[Tuple[str, float]] = [
        (
            word,
            abs(
                word_num_emotion_users / total_num_recent_emotion_word_users
                - emotion_word_to_content_count_dict[word] / total_num_eligible_content
            ),
        )
        for word, word_num_emotion_users in recent_emotion_word_to_user_count
    ]
    recent_word_to_absolute_inconsistency.sort(key=lambda x: x[1], reverse=True)

    recent_abs_diff_selection_distribution = "\n".join(
        f" •  {word}: {abs_diff:.2%} ([{recent_emotion_word_to_user_count_dict[word]}/{total_num_recent_emotion_word_users} selections to {emotion_word_to_content_count_dict[word]}/{total_num_eligible_content} pieces of content]"
        for word, abs_diff in recent_word_to_absolute_inconsistency
    )

    logging.info(
        f"Recent absolute difference selection distribution:\n\n{recent_abs_diff_selection_distribution}"
    )

    messages = [
        "Emotion Selections Report",
        f"Content distribution:\n{content_distribution}",
        f"All-time selection distribution:\n{all_time_selection_distribution}",
        f"Recent selection distribution:\n{recent_selection_distribution}",
        f"All-time absolute difference selection distribution:\n{all_time_abs_diff_selection_distribution}",
        f"Recent absolute difference selection distribution:\n{recent_abs_diff_selection_distribution}",
    ]

    try:
        openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
        openai_client = openai.Client(api_key=openai_api_key)
        await ratelimit_using_redis(
            itgs,
            key="external_apis:api_limiter:chatgpt",
            time_between_requests=3,
        )
        summary_response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "As a business analyst, your job is to analyze the weekly "
                        "emotion selections report and summarize the most important "
                        "findings.\n\n===\n\n"
                        "The emotion selections report is a report that provides "
                        "information on an online web-app, where-in users visit the "
                        "website, select an emotion, and are provided a short piece of "
                        "audio content related to that emotion.\n\n"
                        "The report consists of 5 sections: content distribution, "
                        "all-time selection distribution, recent selection "
                        "distribution, all-time absolute difference selection. These contain "
                        "the following information:\n"
                        "- The content distribution section describes how many pieces of "
                        "content are available for each emotion. This is a proxy for how much "
                        "money has been invested into evoking that emotion.\n"
                        "- The all-time selection distribution section describes how many "
                        "times users have selected each emotion throughout all time.\n"
                        "- The recent selection distribution section describes how many "
                        "times users have selected each emotion in the past week.\n"
                        "- The all-time absolute difference selection distribution section "
                        "describes the absolute difference between the percent of content "
                        "available for each emotion and the percent of times users have "
                        "selected each emotion throughout all time.\n"
                        "- The recent absolute difference selection distribution section "
                        "describes the absolute difference between the percent of content "
                        "available for each emotion and the percent of times users have "
                        "selected each emotion in the past week."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        "Summarize the following emotion selections report:\n\n"
                        + "\n\n===\n\n".join(messages[1:])
                    ),
                },
            ],
        )
        summary = summary_response.choices[0].message.content
    except Exception as e:
        await handle_error(
            e, extra_info="while producing a summary of the emotion selections report"
        )
        summary = "Not available (error occurred while producing summary)"

    messages.append(f"Summary:\n{summary}")

    if os.environ["ENVIRONMENT"] == "dev":
        logging.debug("Report:\n\n" + "\n\n===\n\n".join(messages))
        return

    slack = await itgs.slack()
    for message in messages:
        await slack.send_oseh_bot_message(message)
        await asyncio.sleep(1)


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.slack_stats.emotion_selections")

    asyncio.run(main())
