"""Rotates the completed daily by-subcategory totals into the total by-subcategory totals"""
from typing import List, Set
from error_middleware import handle_warning
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
    i.e.,

    - `stats:interactive_prompt_sessions:bysubcat:total_views:{unix_date}` into
      `stats:interactive_prompt_sessions:bysubcat:total_views` (raw views)

    - `stats:interactive_prompt_sessions:bysubcat:total_users:{unix_date}` into
      `stats:interactive_prompt_sessions:bysubcat:total_users` (unique users)

    as well as moving the old daily totals into the database under the
    `journey_subcategory_view_stats` table.

    The values aren't incremented live for consistency with other parts of the
    admin dashboard.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    redis = await itgs.redis()

    now = time.time()
    curr_unix_date = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )

    earliest_key = "stats:interactive_prompt_sessions:bysubcat:earliest"
    earliest_raw = await redis.get(earliest_key)
    if earliest_raw is None:
        return

    subcategories_raw = await redis.smembers(
        b"stats:interactive_prompt_sessions:bysubcat:subcategories"  # type: ignore
    )
    subcategories: List[str] = []
    for subcategory_raw in subcategories_raw:
        if isinstance(subcategory_raw, bytes):
            subcategories.append(subcategory_raw.decode("utf-8"))
        elif isinstance(subcategory_raw, str):
            subcategories.append(subcategory_raw)
        else:
            raise TypeError(
                f"unexpected type {type(subcategory_raw)=} for {subcategory_raw=}"
            )

    never_seen_subcategories_set = set(subcategories)

    earliest_unix_date = int(earliest_raw)
    for unix_date in range(earliest_unix_date, curr_unix_date):
        if gd.received_term_signal:
            return

        await move_views_in_redis(itgs, unix_date)
        await move_unique_views_in_redis(itgs, unix_date, subcategories)
        await move_views_and_unique_views_to_db(
            itgs, unix_date, subcategories, never_seen_subcategories_set
        )

        keys_to_delete: List[str] = [
            f"stats:interactive_prompt_sessions:bysubcat:total_views:{unix_date}",
            *[
                f"stats:interactive_prompt_sessions:{subcategory}:{unix_date}:subs"
                for subcategory in subcategories
            ],
        ]
        for key in keys_to_delete:
            logging.debug(f"redis delete {key=}")

        await redis.delete(*[key.encode("utf-8") for key in keys_to_delete])

        logging.debug(f"redis set {earliest_key=} {unix_date + 1=}")
        await redis.set(earliest_key, unix_date + 1)


async def move_views_in_redis(itgs: Itgs, unix_date: int) -> None:
    redis = await itgs.redis()

    base_key = "stats:interactive_prompt_sessions:bysubcat:total_views"
    daily_key = f"{base_key}:{unix_date}"
    daily = await redis.hgetall(daily_key.encode("utf-8"))  # type: ignore
    if not daily:
        return

    async with redis.pipeline() as pipe:
        pipe.multi()

        for key, value in daily.items():
            logging.debug(f"redis hincrby {base_key=} {key=} {value=}")
            await pipe.hincrby(base_key.encode("utf-8"), key, value)  # type: ignore

        await pipe.execute()


async def move_unique_views_in_redis(
    itgs: Itgs, unix_date: int, subcategories: List[str]
) -> None:
    for subcategory in subcategories:
        await move_subcategory_unique_views_in_redis(itgs, unix_date, subcategory)


async def move_subcategory_unique_views_in_redis(
    itgs: Itgs, unix_date: int, subcategory: str
) -> None:
    redis = await itgs.redis()

    base_key = "stats:interactive_prompt_sessions:bysubcat:total_users"
    subs_key = f"stats:interactive_prompt_sessions:{subcategory}:{unix_date}:subs"

    unique_views = await redis.scard(subs_key.encode("utf-8"))  # type: ignore
    if unique_views > 0:
        await redis.hincrby(
            base_key.encode("utf-8"), subcategory.encode("utf-8"), unique_views  # type: ignore
        )


async def move_views_and_unique_views_to_db(
    itgs: Itgs,
    unix_date: int,
    subcategories: List[str],
    never_seen_subcategories: Set[str],
) -> None:
    """Checks each subcategory in `subcategories`. If

    - it has views:
        - inserts it in the database
        - removes the subcategory from `never_seen_subcategories`
    - it has no views:
        - does nothing
    """
    for subcategory in subcategories:
        found = await move_subcategory_views_and_unique_views_to_db(
            itgs, unix_date, subcategory
        )
        if found:
            never_seen_subcategories.remove(subcategory)


async def move_subcategory_views_and_unique_views_to_db(
    itgs: Itgs, unix_date: int, subcategory: str
) -> bool:
    redis = await itgs.redis()

    subs_key = f"stats:interactive_prompt_sessions:{subcategory}:{unix_date}:subs"
    unique_views = await redis.scard(subs_key.encode("utf-8"))  # type: ignore

    views_key = f"stats:interactive_prompt_sessions:bysubcat:total_views:{unix_date}"
    views_raw = await redis.hget(views_key.encode("utf-8"), subcategory.encode("utf-8"))  # type: ignore
    views = int(views_raw) if views_raw is not None else 0

    if unique_views == 0 and views == 0:
        return False

    conn = await itgs.conn()
    cursor = conn.cursor()
    response = await cursor.execute(
        """
        INSERT INTO journey_subcategory_view_stats (
            subcategory, retrieved_for, retrieved_at, total_users, total_views
        )
        SELECT
            ?, ?, ?, ?, ?
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
            unique_views,
            views,
            subcategory,
            unix_dates.unix_date_to_date(unix_date).isoformat(),
        ),
    )
    if response.rows_affected is None or response.rows_affected < 1:
        await handle_warning(
            f"{__name__}:duplicate",
            f"for {subcategory=} and {unix_date=}, found data in redis that was already in rqlite: {unique_views=}, {views=}",
        )

    logging.debug(
        f"stored {subcategory=} {unix_date=} with {views=}, {unique_views=} in the database"
    )
    return True
