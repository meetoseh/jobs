"""A barebones success handler for sms"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.sms.sms_info import decode_data_for_success_job
import time

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str):
    """A barebones success handler for sms, which simply logs the result

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded sms and success information
    """
    sms, success_info = decode_data_for_success_job(data_raw)
    now = time.time()
    logging.info(
        f"SMS {sms.uid} confirmed successful "
        f"after {now - sms.initially_queued_at:.2f}s:\n"
        f"  {sms=}\n"
        f"  {success_info=}"
    )
