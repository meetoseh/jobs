"""Rotates touch stale stats to the database"""

import time
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.process_redis_hgetall import process_redis_hgetall_ints
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue touch stale statistics from redis to the database. Specifically,
    this rotates data from before yesterday in the America/Los_Angeles timezone.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    curr_unix_date = unix_dates.unix_date_today(tz=tz)
    redis = await itgs.redis()
    earliest_key = b"stats:touch_stale:daily:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info("Touch stale stats have not been initialized yet")
        return

    earliest_stored_date = int(earliest_stored_date)

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    for unix_date in range(earliest_stored_date, curr_unix_date - 1):
        if gd.received_term_signal:
            logging.info(
                f"Not rotating further dates (term signal received) (next date: {unix_dates.unix_date_to_date(unix_date).isoformat()})"
            )
            return
        logging.debug(
            f"Rotating touch stale stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
        )

        result = await redis.hgetall(key_for_date(unix_date))  # type: ignore
        overall = process_redis_hgetall_ints(result)

        logging.debug(
            f"successfully fetched daily touch stale stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}: "
            f"{overall=}"
        )

        response = await cursor.execute(
            """
            INSERT INTO touch_stale_stats (
                retrieved_for, retrieved_at, stale
            )
            SELECT
                ?, ?, ?
            WHERE
                NOT EXISTS (
                    SELECT 1 FROM touch_stale_stats WHERE retrieved_for = ?
                )
            """,
            (
                unix_dates.unix_date_to_date(unix_date).isoformat(),
                time.time(),
                overall.get("stale", 0),
                unix_dates.unix_date_to_date(unix_date).isoformat(),
            ),
        )

        if response.rows_affected != 1:
            logging.warning(
                f"Failed to insert touch stale stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )
            existing_row = await cursor.execute(
                "SELECT * FROM touch_stale_stats WHERE retrieved_for = ?",
                (unix_dates.unix_date_to_date(unix_date).isoformat(),),
            )
            if not existing_row.results:
                raise Exception(
                    f"Failed to insert touch stale stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} "
                    "and no existing row was found"
                )
            slack = await itgs.slack()
            msg = (
                f"Failed to rotate daily touch stale stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}; "
                f"wanted to insert:\n\n"
                f"- {overall=}\n\n"
                f"but found existing data ({existing_row.results[0]})"
                "\n\ngoing to destroy the data in redis and keep the existing data in the database"
            )
            logging.warning(msg)
            await slack.send_ops_message(
                msg, preview="Daily Touch Stale Stats Rotation Failure"
            )
        else:
            logging.info(
                f"Successfully inserted daily touch stale stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )

        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(key_for_date(unix_date))
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()


def key_for_date(unix_date: int) -> bytes:
    return f"stats:touch_stale:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:touch_stale:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_touch_stale_stats")

    asyncio.run(main())
