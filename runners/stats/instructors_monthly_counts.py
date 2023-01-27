"""Deletes no longer needed instructor monthly counts from redis to save memory"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import unix_dates
import time
import pytz
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Deletes no longer needed instructor monthly counts from redis to save memory.
    This is maintained in redis for the admin dashboard, and once the admin dashboard
    no longer needs it, it can be pulled (slower) using the database directly.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    now = time.time()

    curr_unix_month = unix_dates.unix_timestamp_to_unix_month(
        now, tz=pytz.timezone("America/Los_Angeles")
    )

    redis = await itgs.redis()

    earliest_stored = await redis.get("stats:instructors:monthly:earliest")
    if earliest_stored is None:
        return

    earliest_unix_month = int(earliest_stored)
    for unix_month in range(earliest_unix_month, curr_unix_month):
        key = f"stats:instructors:monthly:{unix_month}:count"
        logging.debug(f"redis delete {key=}")
        await redis.delete(key)

    await redis.set("stats:instructors:monthly:earliest", curr_unix_month)
