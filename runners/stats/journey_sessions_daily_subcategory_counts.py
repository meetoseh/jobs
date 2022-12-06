"""Handles rolling daily journey session subcategory counts into rqlite"""
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import unix_dates
import time
import pytz
import asyncio


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Moves completed unique users who participated in a journey session data from
    redis to rqlite.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    redis = await itgs.redis()

    now = time.time()
    curr_unix_date = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )

    earliest_key = "stats:journey_sessions:bysubcat:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        return

    if earliest_stored_date >= curr_unix_date:
        return

    subcats_key = "stats:journey_sessions:bysubcat:subcategories"
    subcategories = await redis.smembers(subcats_key)
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    never_seen_subcategories = set(subcategories)
    for unix_date in range(int(earliest_stored_date), curr_unix_date):
        for subcategory in subcategories:
            if gd.received_term_signal:
                return

            subs_key = f"stats:journey_sessions:{subcategory}:{unix_date}:subs"
            unique_users = await redis.scard(subs_key)
            if unique_users == 0:
                continue

            never_seen_subcategories.remove(subcategory)
            response = await cursor.execute(
                """
                INSERT INTO journey_subcategory_view_stats (
                    subcategory, retrieved_for, retrieved_at, total
                )
                SELECT
                    ?, ?, ?, ?
                WHERE
                    NOT EXISTS (
                        SELECT 1 FROM journey_subcategory_view_stats AS jsvs
                        WHERE
                            jsvs.subcategory = ?
                            AND jsvs.retrieved_for = ?
                    )
                """,
                (
                    subcategory,
                    unix_dates.unix_date_to_date(unix_date).isoformat(),
                    time.time(),
                    unique_users,
                    subcategory,
                    unix_dates.unix_date_to_date(unix_date).isoformat(),
                ),
            )
            if response.rows_affected is None or response.rows_affected < 1:
                asyncio.ensure_future(
                    handle_warning(
                        f"{__name__}:duplicate",
                        f"for {subcategory=} and {unix_date=}, found data in redis that was already in rqlite: {unique_users=}",
                    )
                )

            logging.debug(f"deleting redis {subs_key=} with {unique_users=}")
            await redis.delete(subs_key)

        logging.debug(f"setting {earliest_key=} to {(unix_date + 1)=}")
        await redis.set(earliest_key, unix_date + 1)

    # this technically races
    for subcategory in never_seen_subcategories:
        exists_today = await redis.exists(
            f"stats:journey_sessions:{subcategory}:{curr_unix_date}:subs"
        )
        if not exists_today:
            logging.debug(f"srem {subcats_key=} {subcategory=}")
            await redis.srem(subcats_key, subcategory)
