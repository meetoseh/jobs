"""Sends a test SMS. This tests the lowest level, which does not include tracking
to whom or why the SMS was sent, nor does it keep a history
"""

from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.shared.job_callback import JobCallback
from lib.sms.send import send_sms

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    phone_number: str,
    body: str,
):
    """Sends a test SMS through the SMS send queue. This tests the
    lowest level, which does not include tracking to whom or why the
    sms was sent, nor does it keep a history.

    It accepts the actual phone number that you want to send to, meaning it can only send
    to a particular device, not to a user.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        phone_number (str): the phone number to send to
        body (str): the body of the SMS
    """
    uid = await send_sms(
        itgs,
        phone_number=phone_number,
        body=body,
        success_job=JobCallback(name="runners.sms.test_success_handler", kwargs={}),
        failure_job=JobCallback(name="runners.sms.test_failure_handler", kwargs={}),
    )

    logging.info(f"Sent test SMS {uid} to {phone_number}")


if __name__ == "__main__":
    import asyncio

    async def main():
        phone_number = input("Phone number (E.164): ")
        body = input("Body: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.sms.send_test", phone_number=phone_number, body=body
            )

    asyncio.run(main())
