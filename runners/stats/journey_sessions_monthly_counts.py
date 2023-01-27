"""Handles deleting old monthly journey session statistics"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import unix_dates
import time
import pytz
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Deletes unused monthly journey session statistics from redis. This information
    is used in the admin dashboard, which uses a live redis count to quickly retrieve
    the number of sessions for the current month. This data can be retrieved from other
    locations, it's just slower to do so.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    redis = await itgs.redis()

    now = time.time()
    curr_unix_month = unix_dates.unix_timestamp_to_unix_month(
        now, tz=pytz.timezone("America/Los_Angeles")
    )

    earliest_key = "stats:journey_sessions:monthly:earliest"
    earliest_stored_month = await redis.get(earliest_key)
    if earliest_stored_month is None:
        return

    earliest_stored_month = int(earliest_stored_month)

    async with redis.pipeline() as pipe:
        pipe.multi()
        for month in range(earliest_stored_month, curr_unix_month, 1):
            key = f"stats:journey_sessions:monthly:{month}:count"
            logging.debug(f"deleting redis {key=}")
            await pipe.delete(key)

        logging.debug(f"updating {earliest_key=} to {curr_unix_month=}")
        await pipe.set(earliest_key, curr_unix_month)
        await pipe.execute()
