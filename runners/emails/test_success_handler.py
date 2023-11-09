from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.emails.email_info import decode_data_for_success_job

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str):
    """A barebones success handler for emails.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded data from the emails system
    """
    email, info = decode_data_for_success_job(data_raw)
    logging.info(
        f"Email {email.message_id} ({email.subject}) was successfully delivered to {email.email} "
        f"using {email.template}:\n{email=}, {info=}"
    )
