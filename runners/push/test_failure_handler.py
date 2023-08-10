"""A barebones failure handler for push notifications"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.push.message_attempt_info import (
    decode_data_for_failure_job,
)
from lib.push.handler import retry_or_abandon_standard

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str):
    """Handles failure in a test push notification. This is extremely barebones, and
    does not result in any permanent storage that the notification occurred.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded failure information
    """
    attempt, failure_info = decode_data_for_failure_job(data_raw)
    logging.info(
        f"Test push notification {attempt.uid} failed during {failure_info.action}: {failure_info}"
    )
    await retry_or_abandon_standard(itgs, attempt, failure_info)
