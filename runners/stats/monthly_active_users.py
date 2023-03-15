"""Rolls over monthly active users to rqlite"""
import asyncio
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import unix_dates
import time
import pytz
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Transfers immutable monthly active user statistics from redis to rqlite.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    curr_unix_month = unix_dates.unix_timestamp_to_unix_month(
        time.time(), tz=pytz.timezone("America/Los_Angeles")
    )
    redis = await itgs.redis()
    earliest_key = "stats:monthly_active_users:earliest"
    earliest_stored_month = await redis.get(earliest_key)
    if earliest_stored_month is None:
        return

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    earliest_stored_month = int(earliest_stored_month)
    for unix_month in range(earliest_stored_month, curr_unix_month):
        if gd.received_term_signal:
            return

        key = f"stats:monthly_active_users:{unix_month}"
        retrieved_at = time.time()
        retrieved_for = unix_dates.unix_month_to_date_of_first(unix_month)
        fmted_retrieved_for = f"{retrieved_for.year}-{retrieved_for.month:02}"
        total = await redis.scard(key)

        response = await cursor.execute(
            """
            INSERT INTO monthly_active_user_stats (
                retrieved_for, retrieved_at, total
            )
            SELECT
                ?, ?, ?
            WHERE
                NOT EXISTS (
                    SELECT 1 FROM monthly_active_user_stats
                    WHERE retrieved_for = ?
                )
            """,
            (fmted_retrieved_for, retrieved_at, total, fmted_retrieved_for),
        )

        if response.rows_affected is None or response.rows_affected < 1:
            asyncio.ensure_future(
                handle_warning(
                    f"{__name__}:duplicate",
                    f"already had monthly active user stats for {retrieved_for=}, so dropping {total=} {retrieved_at=}",
                )
            )

        logging.debug(f"redis delete {key=}")
        await redis.delete(key)
        logging.debug(f"redis set {earliest_key=} {unix_month + 1=}")
        await redis.set(earliest_key, unix_month + 1)
