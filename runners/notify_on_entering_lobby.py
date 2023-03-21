from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.image_files.get_biggest_export import get_biggest_export_url
from lib.durations import format_duration
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, user_sub: str, journey_uid: str):
    """Notifies slack that the user with the given sub has just been provided a ref to
    the journey with the given uid.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): The sub of the user entering the lobby
        journey_uid (str): The uid of the journey the user is entering the lobby for
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            users.email,
            users.phone_number,
            users.given_name,
            users.family_name,
            users.created_at,
            profile_image_files.uid,
            attributed_utms.canonical_query_param
        FROM users
        LEFT OUTER JOIN image_files AS profile_image_files ON profile_image_files.id = users.picture_image_file_id
        LEFT OUTER JOIN utms AS attributed_utms ON (
            EXISTS (
                SELECT 1 FROM visitor_utms AS last_visitor_utms
                WHERE
                    last_visitor_utms.utm_id = attributed_utms.id
                    AND last_visitor_utms.clicked_at <= users.created_at
                    AND EXISTS (
                        SELECT 1 FROM visitor_users
                        WHERE 
                            visitor_users.visitor_id = last_visitor_utms.visitor_id
                            AND visitor_users.user_id = users.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM visitor_utms AS other_visitor_utms
                        WHERE
                            other_visitor_utms.clicked_at <= users.created_at
                            AND EXISTS (
                                SELECT 1 FROM visitor_users
                                WHERE visitor_users.visitor_id = other_visitor_utms.visitor_id
                                AND visitor_users.user_id = users.id
                            )
                            AND (
                                other_visitor_utms.clicked_at > last_visitor_utms.clicked_at
                                OR (
                                    other_visitor_utms.clicked_at = last_visitor_utms.clicked_at
                                    AND other_visitor_utms.uid > last_visitor_utms.uid
                                )
                            )
                    )
            )
        )
        WHERE users.sub = ?
        """,
        (user_sub,),
    )

    if not response.results:
        logging.warning(f"No user found with sub {user_sub}")
        return

    email: str = response.results[0][0]
    phone_number: Optional[str] = response.results[0][1]
    given_name: Optional[str] = response.results[0][2]
    family_name: Optional[str] = response.results[0][3]
    created_at: float = response.results[0][4]
    profile_image_file_uid: Optional[str] = response.results[0][5]
    attributed_source: Optional[str] = response.results[0][6]

    response = await cursor.execute(
        """
        SELECT
            journeys.title,
            instructors.name,
            background_image_files.uid,
            content_files.duration_seconds
        FROM journeys
        JOIN instructors ON instructors.id = journeys.instructor_id
        JOIN image_files AS background_image_files ON background_image_files.id = journeys.background_image_file_id
        JOIN content_files ON content_files.id = journeys.audio_content_file_id
        WHERE journeys.uid = ?
        """,
        (journey_uid,),
    )

    if not response.results:
        logging.warning(f"No journey found with uid {journey_uid}")
        return

    journey_title: str = response.results[0][0]
    instructor_name: str = response.results[0][1]
    background_image_file_uid: str = response.results[0][2]
    journey_duration_seconds: int = response.results[0][3]

    one_week = 60 * 60 * 24 * 7
    profile_image_url = (
        await get_biggest_export_url(
            itgs,
            uid=profile_image_file_uid,
            duration=one_week,
            max_width=400,
            max_height=400,
        )
        if profile_image_file_uid
        else None
    )
    background_image_url = await get_biggest_export_url(
        itgs,
        uid=background_image_file_uid,
        duration=one_week,
        max_width=500,
        max_height=500,
    )

    name = (
        f"{given_name} {family_name}"
        if given_name and family_name
        else (given_name or family_name or "Anonymous")
    )
    pretty_joined = unix_dates.unix_timestamp_to_datetime(
        created_at, tz=pytz.timezone("America/Los_Angeles")
    ).strftime("%B %d, %Y")
    user_details = "\n •  " + "\n •  ".join(
        [
            *([f"phone: `{phone_number}`"] if phone_number else []),
            f"email: `{email}`",
            f"joined: {pretty_joined}",
            *([f"source: `{attributed_source}`"] if attributed_source else []),
        ]
    )

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{name} is entering a lobby!*{user_details}",
            },
            **(
                {
                    "accessory": {
                        "type": "image",
                        "image_url": profile_image_url,
                        "alt_text": "profile image",
                    }
                }
                if profile_image_url
                else {}
            ),
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{journey_title}* ({format_duration(journey_duration_seconds)})\nby {instructor_name}",
            },
            "accessory": {
                "type": "image",
                "image_url": background_image_url,
                "alt_text": "background image",
            },
        },
    ]

    logging.debug(f"Posting to slack: {blocks}")

    slack = await itgs.slack()
    await slack.send_oseh_classes_blocks(blocks, preview=f"{name} is entering a lobby!")
