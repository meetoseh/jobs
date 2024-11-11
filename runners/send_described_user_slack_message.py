"""Sends a slack message to the channel identified, augmenting the message with
data about the user specified.
"""

import json
from typing import Literal
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.shared.clean_for_slack import clean_for_preview
from lib.shared.describe_user import describe_user

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    message: str,
    sub: str,
    channel: Literal["web_error", "ops", "oseh_bot", "oseh_classes"],
):
    """Sends a message to the slack channel identified by the channel argument,
    augmenting the message with data about the user specified by the sub
    argument.

    The message can contain the following placeholders via curly brackets:
    - `name`: the user's name or nearest equivalent

    ex: "Hello, {name}!"

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        message (str): the message to send
        sub (str): the user to describe
        channel (Literal["web_error", "ops", "oseh_bot", "oseh_classes"]): the
            channel to send the message to
    """
    user = await describe_user(itgs, sub)

    if user is None:
        preview = message.format(name="USER NOT FOUND")
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message.format(name="USER NOT FOUND"),
                },
            }
        ]
    else:
        try:
            preview = message.format(name=user.name_equivalent)
            blocks = [await user.make_slack_block(itgs, message)]
        except KeyError as e:
            await handle_warning(
                f"{__name__}:malformed_message",
                f"Failed to format slack message preview for {message} for user {user.sub}",
                exc=e,
            )
            return

    preview = clean_for_preview(preview.splitlines()[0])
    if len(preview) > 64:
        preview = preview[:61] + "..."

    logging.info(
        f"Sending slack message to {channel}: {preview}\n\n{json.dumps(blocks)}"
    )
    slack = await itgs.slack()
    if not hasattr(slack, f"send_{channel}_blocks"):
        logging.warning(
            f"Channel {channel} specified for slack message is unknown, using oseh_bot instead"
        )
        channel = "oseh_bot"

    await getattr(slack, f"send_{channel}_blocks")(blocks, preview)


if __name__ == "__main__":
    import asyncio

    async def main():
        message = input("Message: ")
        sub = input("User Sub: ")
        channel = input("Channel: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.send_described_user_slack_message",
                message=message,
                sub=sub,
                channel=channel,
            )

    asyncio.run(main())
