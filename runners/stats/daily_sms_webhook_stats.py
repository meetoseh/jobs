"""Deletes old SMS webhook stats"""

from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import pytz
import unix_dates

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Deletes old SMS webhook stats from redis

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    curr_unix_date = unix_dates.unix_date_today(tz=tz)
    redis = await itgs.redis()
    earliest_key = b"stats:sms_webhooks:daily:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info("SMS webhook stats have not been initialized yet")
        return

    earliest_stored_date = int(earliest_stored_date)

    for unix_date in range(earliest_stored_date, curr_unix_date - 1):
        if gd.received_term_signal:
            logging.info(
                f"Not deleting further dates (term signal received) (next date: {unix_dates.unix_date_to_date(unix_date).isoformat()})"
            )
            return
        logging.debug(
            f"Deleting SMS webhook stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
        )

        stored_stats = await redis.hgetall(
            f"stats:sms_webhooks:daily:{unix_date}".encode("utf-8")  # type: ignore
        )
        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.expire(
                f"stats:sms_webhooks:daily:{unix_date}".encode("utf-8"), 604800
            )
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()

        logging.info(
            f"Set {unix_dates.unix_date_to_date(unix_date).isoformat()} to expire in 1 week; contains {stored_stats=}"
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_sms_webhook_stats")

    asyncio.run(main())
