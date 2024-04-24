"""Sends a test notification through the email notification system. This tests the lowest
level, which does not include tracking to whom or why the notification was sent, nor does
it keep a history
"""

import json
from typing import Any, Dict
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.emails.send import send_email
from lib.shared.job_callback import JobCallback
import asyncio

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    email: str,
    subject: str,
    template: str,
    template_parameters: Dict[str, Any],
):
    """Sends a test notification through the email notification system. This tests the
    lowest level, which does not include tracking to whom or why the email was
    sent, nor does it keep a history.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        email (str): the email to send to
        subject (str): the subject of the email
        template (str): the template to use for the email
        template_parameters (Dict[str, Any]): the parameters to pass to the template
    """
    uid = await send_email(
        itgs,
        email=email,
        subject=subject,
        template=template,
        template_parameters=template_parameters,
        success_job=JobCallback(name="runners.emails.test_success_handler", kwargs={}),
        failure_job=JobCallback(name="runners.emails.test_failure_handler", kwargs={}),
    )

    logging.info(f"Sent test email {uid} to {email}")


if __name__ == "__main__":

    async def main():
        email = input("Email: ")
        subject = input("Subject: ")
        template = input("Template: ")
        template_parameters_raw = input("Template parameters (json): ")
        template_parameters = json.loads(template_parameters_raw)

        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.emails.send_test",
                email=email,
                subject=subject,
                template=template,
                template_parameters=template_parameters,
            )

    asyncio.run(main())
