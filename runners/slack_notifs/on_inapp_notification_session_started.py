"""Invoked when a user starts an inapp notification session; checks if we
are supposed to send a slack message, if so does so
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
    inapp_notification_uid: str,
    user_sub: str,
    session_uid: str,
):
    """
    ## DEPRECATED

    This runner SHOULD NOT be depended on. It is intended to maintain support
    for older versions of the app.

    `inapp_notifications`, and the corresponding stack-based client navigation paradigm,
    have been replaced with `client_flows`.

    ## HISTORICAL

    Expected to be called after a user starts an inapp notification session;
    sends a slack message if appropriate

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        inapp_notification_uid (str): the uid of the inapp notification started
        user_sub (str): the user who started the session
        session_uid (str): the session uid that was assigned
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        "SELECT slack_message FROM inapp_notifications WHERE uid=?",
        (inapp_notification_uid,),
    )
    if not response.results:
        await handle_warning(
            f"{__name__}:no_inapp_notification",
            f"bad `inapp_notification_uid={clean_for_slack(inapp_notification_uid)}`",
        )
        return

    slack_message_raw: Optional[str] = response.results[0][0]
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
    ), f"{channel=}, {inapp_notification_uid=}"
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
        inapp_notification_uid = input("inapp_notification_uid: ")
        user_sub = input("user_sub: ")
        session_uid = input("session_uid: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.slack_notifs.on_inapp_notification_session_started",
                inapp_notification_uid=inapp_notification_uid,
                user_sub=user_sub,
                session_uid=session_uid,
            )

    asyncio.run(main())
