"""Rotates the completed daily by-subcategory totals into the total by-subcategory totals"""
from itgs import Itgs
from graceful_death import GracefulDeath
import unix_dates
import time
import pytz
import logging
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates the completed daily by-subcategory totals into the total by-subcategory totals,
    i.e., `stats:journey_sessions:bysubcat:totals:{unix_date}` into
    `stats:journey_sessions:bysubcat:totals`. The values in
    ``stats:journey_sessions:bysubcat:totals` aren't incremented directly by the
    backend as it would make caching the totals infeasible.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    redis = await itgs.redis()

    now = time.time()
    curr_unix_date = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )

    earliest_key = "stats:journey_sessions:bysubcat:totals:earliest"
    earliest_raw = await redis.get(earliest_key)
    if earliest_raw is None:
        return

    earliest_unix_date = int(earliest_raw)
    overall_totals_key = "stats:journey_sessions:bysubcat:totals"

    for unix_date in range(earliest_unix_date, curr_unix_date):
        if gd.received_term_signal:
            return

        totals_key = f"stats:journey_sessions:bysubcat:totals:{unix_date}"
        totals = await redis.hgetall(totals_key)
        if not totals:
            continue

        async with redis.pipeline() as pipe:
            pipe.multi()

            for key, value in totals.items():
                logging.debug(f"redis hincrby {overall_totals_key=} {key=} {value=}")
                await pipe.hincrby(overall_totals_key, key, value)

            logging.debug(f"redis delete {totals_key=}")
            await pipe.delete(totals_key)

            logging.debug(f"redis set {earliest_key=} {unix_date + 1=}")
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()
