"""This module provides functions required for keeping user statistics
accurate. This does not include functions for rolling data from redis to
rqlite, since that is done by the jobs repo.

This is not an exhausitive list of callbacks: see also interactive_prompts/lib/stats.py
"""

from itgs import Itgs
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
import pytz
import unix_dates


RETENTION_PERIOD_DAYS = [0, 1, 7, 30, 90]
STATS_TIMEZONE = pytz.timezone("America/Los_Angeles")


async def on_user_created(itgs: Itgs, sub: str, created_at: float) -> None:
    """Performs the necessary calls for when a user with the given sub
    is created in our database for the first time, typically right after
    they login.

    This impacts the following keys, which are described in docs/redis/keys.md

    - `stats:users:count`
    - `stats:users:monthly:{unix_month}:count`
    - `stats:users:monthly:earliest`
    - `stats:daily_new_users:{unix_date}`
    - `stats:daily_new_users:earliest`
    - `stats:retention:{period}:{retained}:{unix_date}
    - `stats:retention:{period}:{retained}:earliest`

    Args:
        itgs (Itgs): The integrations for networked services
        sub (str): The sub of the user that was created
        created_at (float): The time the user was created
    """
    redis = await itgs.redis()

    unix_date = unix_dates.unix_timestamp_to_unix_date(created_at, tz=STATS_TIMEZONE)
    unix_month = unix_dates.unix_timestamp_to_unix_month(created_at, tz=STATS_TIMEZONE)

    await ensure_set_if_lower_script_exists(redis)

    async with redis.pipeline() as pipe:
        pipe.multi()
        await set_if_lower(pipe, "stats:users:monthly:earliest", unix_month)
        await pipe.incr("stats:users:count")
        await pipe.incr(f"stats:users:monthly:{unix_month}:count")
        await pipe.incr(f"stats:daily_new_users:{unix_date}")
        await set_if_lower(pipe, "stats:daily_new_users:earliest", unix_date)
        for period_days in RETENTION_PERIOD_DAYS:
            await pipe.sadd(f"stats:retention:{period_days}day:false:{unix_date}", sub)  # type: ignore
            await set_if_lower(
                pipe, f"stats:retention:{period_days}day:false:earliest", unix_date
            )

        await pipe.execute()


async def on_user_is_active(
    itgs: Itgs, sub: str, *, user_created_at: float, started_at: float
) -> None:
    """Updates user-related statistics as a result of a user with the given
    sub starting an interactive prompt session at the given time, which is assumed to be
    near the current clock time.

    This impacts the following keys, which are described in docs/redis/keys.md

    - `stats:daily_active_users:{unix_date}`
    - `stats:daily_active_users:earliest`
    - `stats:monthly_active_users:{unix_month}`
    - `stats:monthly_active_users:earliest`
    - `stats:retention:{period}:{retained}:{unix_date}`
    - `stats:retention:{period}:{retained}:earliest`

    This function does not handle all the necessary updates for when a
    interactive prompt session starts, see e.g. interactive_prompts/lib/stats.py

    Args:
        itgs (Itgs): The integrations for networked services
        sub (str): The sub of the user that started a interactive prompt session
        user_created_at (float): The time the user was created
        started_at (float): The time the interactive prompt session started
    """
    redis = await itgs.redis()

    created_at_unix_date = unix_dates.unix_timestamp_to_unix_date(
        user_created_at, tz=STATS_TIMEZONE
    )
    started_at_unix_date = unix_dates.unix_timestamp_to_unix_date(
        started_at, tz=STATS_TIMEZONE
    )
    started_at_unix_month = unix_dates.unix_timestamp_to_unix_month(
        started_at, tz=STATS_TIMEZONE
    )

    await ensure_set_if_lower_script_exists(redis)

    async with redis.pipeline() as pipe:
        pipe.multi()

        # The order here matters; set_if_lower will fail if the script was
        # flushed since the last call, but won't rollback earlier calls
        await set_if_lower(
            pipe, "stats:daily_active_users:earliest", started_at_unix_date
        )
        await pipe.sadd(f"stats:daily_active_users:{started_at_unix_date}", sub)  # type: ignore

        await pipe.sadd(f"stats:monthly_active_users:{started_at_unix_month}", sub)  # type: ignore
        await set_if_lower(
            pipe, "stats:monthly_active_users:earliest", started_at_unix_month
        )
        if started_at_unix_date - created_at_unix_date <= 182:
            for period_days in RETENTION_PERIOD_DAYS:
                if started_at_unix_date - created_at_unix_date < period_days:
                    continue

                await pipe.smove(  # type: ignore
                    f"stats:retention:{period_days}day:false:{created_at_unix_date}",  # type: ignore
                    f"stats:retention:{period_days}day:true:{created_at_unix_date}",  # type: ignore
                    sub,
                )
                await set_if_lower(
                    pipe,
                    f"stats:retention:{period_days}day:true:earliest",
                    created_at_unix_date,
                )

        await pipe.execute()
