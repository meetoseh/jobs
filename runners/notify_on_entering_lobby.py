from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.durations import format_duration
from lib.shared.describe_user import create_slack_image_url_from_source, describe_user
import os

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    user_sub: str,
    journey_uid: str,
    action: str = "entering a lobby",
):
    """Notifies slack that the user with the given sub has just been provided a ref to
    the journey with the given uid.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): The sub of the user entering the lobby
        journey_uid (str): The uid of the journey the user is entering the lobby for
        action (str): The action message to include in the slack message
    """

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    user = await describe_user(itgs, user_sub)

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

    background_image_url = await create_slack_image_url_from_source(
        itgs, background_image_file_uid
    )

    blocks = [
        (
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"USER NOT FOUND is {action}",
                },
            }
            if user is None
            else await user.make_slack_block(itgs, f"{{name}} is {action}")
        ),
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{journey_title}* ({format_duration(journey_duration_seconds)})\nby {instructor_name}",
            },
            **(
                {
                    "accessory": {
                        "type": "image",
                        "image_url": background_image_url,
                        "alt_text": "background image",
                    }
                }
                if background_image_url is not None
                else {}
            ),
        },
    ]

    logging.debug(f"Posting to slack: {blocks}")

    if os.environ["ENVIRONMENT"] == "dev":
        return

    slack = await itgs.slack()
    await slack.send_oseh_classes_blocks(
        blocks,
        preview=f"{user.name_equivalent if user is not None else 'USER NOT FOUND'} is {action}!",
    )
