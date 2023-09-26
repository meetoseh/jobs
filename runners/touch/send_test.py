"""Sends a test touch. There is no reason to use this typically, since you
can just append to the test:to_send queue directly and the dashboard will
be more responsive (since you need to incremented queued at the same time).

Primarily this is for fast prototyping or running manually.
"""
import json
from typing import Literal
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.shared.job_callback import JobCallback
from lib.touch.send import send_touch

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    user_sub: str,
    channel: Literal["sms", "email", "push"],
    event_slug: str,
    event_parameters: dict,
):
    """Sends a touch through the touch system, which will result in a user touch,
    updates to the user touch state, and various user touch debug log entries.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): the sub of the user to contact
        channel (str): the channel to use
        event_slug (str): the slug for the event to use
        event_parameters (dict): the parameters for the event
    """
    uid = await send_touch(
        itgs,
        user_sub=user_sub,
        touch_point_event_slug=event_slug,
        channel=channel,
        event_parameters=event_parameters,
    )

    logging.info(f"Sent test touch {uid} to {user_sub}")


if __name__ == "__main__":
    import asyncio

    async def main():
        user_sub = input("User sub: ")

        if user_sub == "preset1":
            user_sub = "oseh_u_e5ZBKh0cvB154XtU9dFmHg"
            channel = "sms"
            event_slug = "daily_reminder"
            event_parameters = {"url": "oseh.io/l/foobar"}
        elif user_sub == "preset2":
            user_sub = "oseh_u_e5ZBKh0cvB154XtU9dFmHg"
            channel = "push"
            event_slug = "daily_reminder"
            event_parameters = {}
        else:
            channel = input("Channel (sms, email, push): ")
            event_slug = input("Event slug: ")
            event_parameters = json.loads(input("Event parameters (json): "))
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.touch.send_test",
                user_sub=user_sub,
                channel=channel,
                event_slug=event_slug,
                event_parameters=event_parameters,
            )

    asyncio.run(main())
