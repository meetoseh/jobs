"""Hanldes moving user retention info from redis to rqlite once it's immutable"""
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


RETENTION_PERIOD_DAYS = [0, 1, 7, 30, 90]


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Moves immutable user retention data from redis to rqlite to free memory

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    redis = await itgs.redis()

    curr_unix_date = unix_dates.unix_timestamp_to_unix_date(
        time.time(), tz=pytz.timezone("America/Los_Angeles")
    )

    for period in RETENTION_PERIOD_DAYS:
        if gd.received_term_signal:
            return
        earliest_keys = [
            f"stats:retention:{period}day:{retained_str}:earliest"
            for retained_str in ("true", "false")
        ]

        earliest_stored_dates = await redis.mget(earliest_keys)
        earliest_stored_dates = [d for d in earliest_stored_dates if d is not None]
        if not earliest_stored_dates:
            continue

        earliest_stored_date = min(int(d) for d in earliest_stored_dates)
        for date_to_rollover in range(earliest_stored_date, curr_unix_date - 182):
            if gd.received_term_signal:
                return
            await rollover_to_rqlite(itgs, period, date_to_rollover)
            async with redis.pipeline() as pipe:
                pipe.multi()
                for earliest_key in earliest_keys:
                    logging.debug(
                        f"redis set {earliest_key=} {(date_to_rollover + 1)=}"
                    )
                    await pipe.set(earliest_key, date_to_rollover + 1)
                await pipe.execute()


async def rollover_to_rqlite(itgs: Itgs, period_days: int, unix_date: int) -> None:
    """Rolls over the specific date, specified as the number of days since
    the unix epoch, to rqlite. The unix date should be at least 182 days
    before the current date, since otherwise we still need to keep it in
    redis for the retention calculation.

    This does not manage the :earliest keys in redis, that is the caller's
    responsibility, so that this can be called in parallel if desired.

    Args:
        itgs (Itgs): the integrations to use
        period_days (int): the number of days in the retention period
        unix_date (int): the date to roll over
    """

    redis = await itgs.redis()

    retrieved_at = time.time()
    retrieved_for = unix_dates.unix_date_to_date(unix_date)
    async with redis.pipeline(transaction=False) as pipe:
        for retained_str in ("true", "false"):
            key = f"stats:retention:{period_days}day:{retained_str}:{unix_date}"
            await pipe.scard(key)
        retained, unretained = await pipe.execute()

    retained = int(retained)
    unretained = int(unretained)

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        INSERT INTO retention_stats (
            period_days, retrieved_for, retrieved_at, retained, unretained
        )
        SELECT
            ?, ?, ?, ?, ?
        WHERE
            NOT EXISTS (
                SELECT 1 FROM retention_stats
                WHERE
                    period_days = ?
                    AND retrieved_for = ?
            )
        """,
        (
            period_days,
            retrieved_for.isoformat(),
            retrieved_at,
            retained,
            unretained,
            period_days,
            retrieved_for.isoformat(),
        ),
    )

    if response.rows_affected is None or response.rows_affected < 1:
        asyncio.ensure_future(
            handle_warning(
                f"{__name__}:duplicate",
                f"Already had retention stats for {period_days=}, {retrieved_for=}; not inserting {retained=}, {unretained=}",
            )
        )

    async with redis.pipeline() as pipe:
        pipe.multi()
        for retained_str in ("true", "false"):
            key = f"stats:retention:{period_days}day:{retained_str}:{unix_date}"
            logging.debug(f"redis delete {key=}")
            await pipe.delete(key)
        await pipe.execute()
