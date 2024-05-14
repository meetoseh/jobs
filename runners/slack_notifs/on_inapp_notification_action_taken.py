"""Invoked when storing an action within an inapp notification session;
sends a slack message if appropriate
"""

import json
from typing import Literal, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import asyncio
from jobs import JobCategory
from lib.shared.clean_for_slack import clean_for_slack
from lib.shared.describe_user import enqueue_send_described_user_slack_message
import logging
import os

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    user_action_uid: str,
):
    """
    ## DEPRECATED

    This runner SHOULD NOT be depended on. It is intended to maintain support
    for older versions of the app.

    `inapp_notifications`, and the corresponding stack-based client navigation paradigm,
    have been replaced with `client_flows`.

    ## HISTORICAL

    Expected to be called after storing an action within an inapp notification
    session; sends a slack message if appropriate

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_action_uid (str): the uid of the user action stored
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        "SELECT"
        " inapp_notification_actions.slack_message,"
        " users.sub "
        "FROM"
        " inapp_notification_user_actions,"
        " inapp_notification_actions,"
        " inapp_notification_users,"
        " users "
        "WHERE"
        " inapp_notification_user_actions.uid = ?"
        " AND inapp_notification_user_actions.inapp_notification_action_id = inapp_notification_actions.id"
        " AND inapp_notification_user_actions.inapp_notification_user_id = inapp_notification_users.id"
        " AND inapp_notification_users.user_id = users.id",
        (user_action_uid,),
    )
    if not response.results:
        await handle_warning(
            f"{__name__}:no_inapp_notification_user_action",
            f"bad `user_action_uid={clean_for_slack(user_action_uid)}` @ weak",
        )
        return

    slack_message_raw: Optional[str] = response.results[0][0]
    user_sub: str = response.results[0][1]
    if slack_message_raw is None:
        return

    slack_message = json.loads(slack_message_raw)
    channel: Literal["web_error", "ops", "oseh_bot", "oseh_classes"] = slack_message[
        "channel"
    ]
    assert isinstance(channel, str)
    assert channel in (
        "web_error",
        "ops",
        "oseh_bot",
        "oseh_classes",
    ), f"{channel=}, {user_action_uid=}"
    message: str = slack_message["message"]
    assert isinstance(message, str)

    if os.environ["ENVIRONMENT"] != "dev":
        await enqueue_send_described_user_slack_message(
            itgs, message=message, channel=channel, sub=user_sub
        )
    else:
        logging.info(
            f"eating slack {message=} for {user_sub=} on {channel=} because ENVIRONMENT=dev"
        )


if __name__ == "__main__":

    async def main():
        user_action_uid = input("user_action_uid: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.slack_notifs.on_inapp_notification_action_taken",
                user_action_uid=user_action_uid,
            )

    asyncio.run(main())
