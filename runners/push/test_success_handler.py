"""A barebones success handler for a push notification"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.push.message_attempt_info import decode_data_for_success_job
import time

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str):
    """A barebones success handler for a push notification, which simply logs that
    the message attempt was accepted by the final notification service (FCMs or APNs)

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded message attempt and success information
    """
    attempt, success_info = decode_data_for_success_job(data_raw)
    now = time.time()
    logging.info(
        f"Test push notification {attempt.uid} confirmed successful "
        f"after {now - attempt.initially_queued_at:.2f}s:\n"
        f"  {attempt=}\n"
        f"  {success_info=}"
    )
