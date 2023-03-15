"""Rolls over daily active users to rqlite"""
import asyncio

import pytz
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import unix_dates
import time
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Transfers immutable daily active user statistics from redis to rqlite.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    curr_unix_date = unix_dates.unix_timestamp_to_unix_date(
        time.time(), tz=pytz.timezone("America/Los_Angeles")
    )
    redis = await itgs.redis()
    earliest_key = "stats:daily_active_users:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        return

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    earliest_stored_date = int(earliest_stored_date)
    for unix_date in range(earliest_stored_date, curr_unix_date):
        if gd.received_term_signal:
            return

        key = f"stats:daily_active_users:{unix_date}"
        retrieved_at = time.time()
        retrieved_for = unix_dates.unix_date_to_date(unix_date)
        total = await redis.scard(key)

        response = await cursor.execute(
            """
            INSERT INTO daily_active_user_stats (
                retrieved_for, retrieved_at, total
            )
            SELECT
                ?, ?, ?
            WHERE
                NOT EXISTS (
                    SELECT 1 FROM daily_active_user_stats
                    WHERE retrieved_for = ?
                )
            """,
            (retrieved_for.isoformat(), retrieved_at, total, retrieved_for.isoformat()),
        )

        if response.rows_affected is None or response.rows_affected < 1:
            asyncio.ensure_future(
                handle_warning(
                    f"{__name__}:duplicate",
                    f"already had daily active user stats for {retrieved_for=}, so dropping {total=} {retrieved_at=}",
                )
            )

        async with redis.pipeline() as pipe:
            pipe.multi()

            logging.debug(f"redis delete {key=}")
            await pipe.delete(key)

            logging.debug(f"redis set {earliest_key=} {unix_date + 1}")
            await pipe.set(earliest_key, bytes(str(unix_date + 1), "ascii"))

            await pipe.execute()
