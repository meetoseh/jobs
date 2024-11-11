"""A barebones failure handler for sms"""

from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.sms.sms_info import decode_data_for_failure_job
from lib.sms.handler import (
    maybe_suppress_phone_due_to_failure,
    retry_or_abandon_standard,
)

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str):
    """Handles failure in a test sms. This is extremely barebones, and
    does not result in any permanent storage that the notification occurred.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded failure information
    """
    attempt, failure_info = decode_data_for_failure_job(data_raw)
    result = await retry_or_abandon_standard(itgs, attempt, failure_info)
    if result.should_ignore:
        return

    logging.info(
        f"Test sms {attempt.uid} failed during {failure_info.action}: {failure_info}"
    )
    await maybe_suppress_phone_due_to_failure(itgs, attempt, failure_info)
