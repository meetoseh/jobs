"""Sends a test notification through the push notification system. This tests the lowest
level, which does not include tracking to whom or why the notification was sent, nor does
it keep a history
"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.push.message_attempt_info import JobCallback, MessageContents
from lib.push.send import send_push

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    push_token: str,
    title: str,
    body: str,
    channel_id: str,
):
    """Sends a test notification through the push notification system. This tests the
    lowest level, which does not include tracking to whom or why the notification was
    sent, nor does it keep a history.

    It accepts the actual push token that you want to send to, meaning it can only send
    to a particular device, not to a user.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        push_token (str): the push token to send to
        title (str): the title of the notification
        body (str): the body of the notification
        channel_id (str): the channel ID for the notification on android
    """
    uid = await send_push(
        itgs,
        push_token=push_token,
        contents=MessageContents(
            title=title,
            body=body,
            channel_id=channel_id,
        ),
        success_job=JobCallback(name="runners.push.test_success_handler", kwargs={}),
        failure_job=JobCallback(name="runners.push.test_failure_handler", kwargs={}),
    )

    logging.info(f"Sent test push notification {uid} to {push_token}")


if __name__ == "__main__":
    import asyncio

    async def main():
        push_token = input("Push token: ")
        title = input("Title: ")
        body = input("Body: ")
        channel_id = input("Channel ID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.push.send_test",
                push_token=push_token,
                title=title,
                body=body,
                channel_id=channel_id,
            )

    asyncio.run(main())
