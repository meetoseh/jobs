import secrets
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from lib.utms.parse import (
    get_canonical_utm_representation_from_wrapped,
    get_utm_parts,
)
import unix_dates
from jobs import JobCategory
import time
import pytz


category = JobCategory.LOW_RESOURCE_COST

earliest_key = b"stats:visitors:daily:earliest"


def utms_set_key(unix_date: int) -> bytes:
    return f"stats:visitors:daily:{unix_date}:utms".encode("utf-8")


def counts_key(utm: str, unix_date: int) -> bytes:
    return f"stats:visitors:daily:{utm}:{unix_date}:counts".encode("utf-8")


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates data from the redis keys

    - `stats:visitors:daily:earliest`
    - `stats:visitors:daily:{unix_date}:utms`
    - `stats:visitors:daily:{utm}:{unix_date}:counts`

    into the database table `daily_utm_conversion_stats`

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    current_unix_date = unix_dates.unix_date_today(tz=tz)

    redis = await itgs.redis()
    earliest_raw = await redis.get(earliest_key)
    if earliest_raw is None:
        logging.debug("No earliest date found for utm conversion stats, not rotating")
        return

    earliest_unix_date = int(earliest_raw)
    if earliest_unix_date >= current_unix_date:
        logging.debug("Earliest date is not in the past, not rotating")
        return

    for date_to_rate in range(earliest_unix_date, current_unix_date):
        if gd.received_term_signal:
            logging.debug(
                f"Exiting early, just before starting {date_to_rate=} because of signal"
            )
            return

        cursor = None
        while cursor is None or int(cursor) != 0:
            cursor, utms_bytes = await redis.sscan(
                utms_set_key(date_to_rate), cursor if cursor is not None else 0
            )
            for raw_utm in utms_bytes:
                if isinstance(raw_utm, str):
                    utm = raw_utm
                elif isinstance(raw_utm, bytes):
                    utm = raw_utm.decode("utf-8")
                else:
                    raise Exception(f"Unexpected type for {raw_utm=}: {type(raw_utm)}")

                await rotate_utm_conversion_stats(itgs, date_to_rate, utm)

        async with redis.pipeline() as pipe:
            pipe.multi()
            logging.debug(f"redis del {utms_set_key(date_to_rate)}")
            await pipe.delete(utms_set_key(date_to_rate))
            logging.debug(f"redis set {earliest_key} {date_to_rate + 1}")
            await pipe.set(earliest_key, date_to_rate + 1)
            await pipe.execute()


async def rotate_utm_conversion_stats(itgs: Itgs, unix_date: int, utm: str) -> None:
    """Rotates just the given utm on the given unix date into the database. If this
    returns without error then:

    - `stats:visitors:daily:{unix_date}:utms` no longer contains `utm`
    - `stats:visitors:daily:{utm}:{unix_date}:counts` no longer exists
    - `daily_utm_conversion_stats` contains up to one more row

    Args:
        itgs (Itgs): the integrations to (re)use
        unix_date (int): the unix date to rotate
        utm (str): the utm to rotate
    """
    utm_parts = get_utm_parts(utm)
    assert utm_parts is not None, f"{utm=} {utm_parts=}"
    redis = await itgs.redis()
    retrieved_at = time.time()
    (
        visits_raw,
        holdover_preexisting_raw,
        holdover_last_click_signups_raw,
        holdover_any_click_signups_raw,
        preexisting_raw,
        last_click_signups_raw,
        any_click_signups_raw,
    ) = await redis.hmget(
        counts_key(utm, unix_date),  # type: ignore
        [
            b"visits",
            b"holdover_preexisting",
            b"holdover_last_click_signups",
            b"holdover_any_click_signups",
            b"preexisting",
            b"last_click_signups",
            b"any_click_signups",
        ],
    )

    visits = int(visits_raw) if visits_raw is not None else 0
    holdover_preexisting = (
        int(holdover_preexisting_raw) if holdover_preexisting_raw is not None else 0
    )
    holdover_last_click_signups = (
        int(holdover_last_click_signups_raw)
        if holdover_last_click_signups_raw is not None
        else 0
    )
    holdover_any_click_signups = (
        int(holdover_any_click_signups_raw)
        if holdover_any_click_signups_raw is not None
        else 0
    )
    preexisting = int(preexisting_raw) if preexisting_raw is not None else 0
    last_click_signups = (
        int(last_click_signups_raw) if last_click_signups_raw is not None else 0
    )
    any_click_signups = (
        int(any_click_signups_raw) if any_click_signups_raw is not None else 0
    )

    debug_info = (
        f"{utm=} {unix_date=} {visits=} {holdover_preexisting=} {holdover_last_click_signups=} "
        f"{holdover_any_click_signups=} {preexisting=} {last_click_signups=} {any_click_signups=}"
    )
    logging.debug(f"Rotating {debug_info=} into the database")

    conn = await itgs.conn()
    cursor = conn.cursor()

    response = await cursor.executemany3(
        (
            (
                """
                INSERT INTO utms (
                    uid, verified, canonical_query_param, utm_source, utm_medium,
                    utm_campaign, utm_term, utm_content, created_at
                )
                SELECT
                    ?, 0, ?, ?, ?, ?, ?, ?, ?
                WHERE
                    NOT EXISTS (
                        SELECT 1 FROM utms WHERE canonical_query_param = ?
                    )
                """,
                (
                    f"oseh_utm_{secrets.token_urlsafe(16)}",
                    get_canonical_utm_representation_from_wrapped(utm_parts),
                    utm_parts.source,
                    utm_parts.medium,
                    utm_parts.campaign,
                    utm_parts.term,
                    utm_parts.content,
                    retrieved_at,
                    get_canonical_utm_representation_from_wrapped(utm_parts),
                ),
            ),
            (
                """
                INSERT INTO daily_utm_conversion_stats (
                    utm_id, retrieved_for, visits, holdover_preexisting,
                    holdover_last_click_signups, holdover_any_click_signups,
                    preexisting, last_click_signups, any_click_signups, retrieved_at
                )
                SELECT
                    utms.id, ?, ?, ?, ?, ?, ?, ?, ?, ?
                FROM utms
                WHERE
                    utms.canonical_query_param = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM daily_utm_conversion_stats
                        WHERE
                            daily_utm_conversion_stats.utm_id = utms.id
                            AND daily_utm_conversion_stats.retrieved_for = ?
                    )
                """,
                (
                    unix_dates.unix_date_to_date(unix_date).isoformat(),
                    visits,
                    holdover_preexisting,
                    holdover_last_click_signups,
                    holdover_any_click_signups,
                    preexisting,
                    last_click_signups,
                    any_click_signups,
                    retrieved_at,
                    get_canonical_utm_representation_from_wrapped(utm_parts),
                    unix_dates.unix_date_to_date(unix_date).isoformat(),
                ),
            ),
        )
    )

    if response[0].rows_affected is not None and response[0].rows_affected > 0:
        logging.info(f"Inserted {utm=} into the database table utms")

    if response[1].rows_affected is None or response[1].rows_affected <= 0:
        await handle_contextless_error(
            extra_info=(
                f"Failed to insert {debug_info} into the database table daily_utm_conversion_stats "
                "(already exists)"
            )
        )
    else:
        logging.info(
            f"Inserted {debug_info=} into the database table daily_utm_conversion_stats"
        )

    redis = await itgs.redis()
    async with redis.pipeline() as pipe:
        pipe.multi()
        logging.debug(f"redis delete {counts_key(utm, unix_date)}")
        await pipe.delete(counts_key(utm, unix_date))
        logging.debug(f"redis srem {utms_set_key(unix_date)} {utm}")
        await pipe.srem(utms_set_key(unix_date), utm)  # type: ignore
        await pipe.execute()


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_utm_conversion_stats")
            print("daily_utm_conversion_stats job enqueued")

    asyncio.run(main())
