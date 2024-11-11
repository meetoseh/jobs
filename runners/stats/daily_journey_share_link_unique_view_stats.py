"""Rotates journey share link stats to the database"""

import json
import time
from typing import Dict, List
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import pytz
from lib.process_redis_hgetall import process_redis_hgetall_ints
import unix_dates
import logging

category = JobCategory.LOW_RESOURCE_COST

REGULAR_EVENTS = tuple()
"""Which events aren't broken down by an additional dimension"""

BREAKDOWN_EVENTS = ("by_code", "by_journey_subcategory", "by_sharer_sub")
"""Which events are broken down by an additional dimension."""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue journey share link unique statistics from redis to the database. Specifically,
    this rotates data from 2 days ago in the America/Los_Angeles timezone.

    Note that these are slightly irregular in the sense that it also rotates the
    set at `journey_share_links:visitors:{unix_date}` for each date, keeping the
    cardinality of it in the database before deleting under the `unique_views`
    column.

    This follows the same pattern of implementation of the standard helper
    (`lib.stats.rotate.rotate`) with the necessary modifications made, rather
    than complicating the helper with the required callbacks

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    job_name = __name__
    regular_events = REGULAR_EVENTS
    breakdown_events = BREAKDOWN_EVENTS
    table_name = "journey_share_link_unique_views"
    earliest_key = "stats:journey_share_links:unique_views:daily:earliest"
    tz = pytz.timezone("America/Los_Angeles")
    num_days_held = 2

    curr_unix_date = unix_dates.unix_date_today(tz=tz)
    redis = await itgs.redis()
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info(f"{job_name} stats have not been initialized yet")
        return

    earliest_stored_date = int(earliest_stored_date)

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    for unix_date in range(earliest_stored_date, curr_unix_date - num_days_held + 1):
        if gd.received_term_signal:
            logging.info(
                f"{job_name} not rotating further dates (term signal received) (next date: {unix_dates.unix_date_to_date(unix_date).isoformat()})"
            )
            return
        logging.debug(
            f"{job_name} rotating stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
        )

        async with redis.pipeline(transaction=False) as pipe:
            await pipe.hgetall(key_for_date(unix_date))  # type: ignore
            for event in breakdown_events:
                await pipe.hgetall(key_for_date_and_event(unix_date, event))  # type: ignore
            await pipe.scard(visitors_key_for_date(unix_date))  # type: ignore
            result = await pipe.execute()

        assert isinstance(result, (list, tuple)), f"{type(result)=}"
        assert len(result) == 2 + len(breakdown_events), f"{len(result)=}"
        assert isinstance(result[-1], int), f"{type(result[-1])=}"

        processed_result: List[Dict[str, int]] = []
        for idx, raw in enumerate(result[:-1]):
            try:
                processed_result.append(process_redis_hgetall_ints(raw))
            except ValueError:
                raise ValueError(f"while processing {idx=} for {unix_date=}")

        overall = processed_result[0]
        breakdowns_by_event = dict(zip(breakdown_events, processed_result[1:]))
        unique_views = result[-1]

        for key in breakdown_events:
            if overall.get(key, 0) != sum(breakdowns_by_event[key].values()):
                await handle_contextless_error(
                    extra_info=f"{job_name} stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} are inconsistent: "
                    f"{key=}, {overall.get(key, 0)=} != {sum(breakdowns_by_event[key].values())=}"
                )

        regular_columns = ", ".join(regular_events)
        if regular_events:
            regular_columns = ", " + regular_columns

        breakdown_columns = ", ".join(
            f"{ev}, {ev}_breakdown" for ev in breakdown_events
        )
        if breakdown_events:
            breakdown_columns = ", " + breakdown_columns

        qmarks = ", ".join(
            "?" for _ in range(3 + len(regular_events) + 2 * len(breakdown_events))
        )
        response = await cursor.execute(
            f"""
            INSERT INTO {table_name} (
                retrieved_for, 
                retrieved_at,
                unique_views
                {regular_columns}
                {breakdown_columns}
            )
            SELECT
                {qmarks}
            WHERE
                NOT EXISTS (
                    SELECT 1 FROM {table_name} WHERE retrieved_for = ?
                )
            """,
            (
                unix_dates.unix_date_to_date(unix_date).isoformat(),
                time.time(),
                unique_views,
                *[overall.get(ev, 0) for ev in regular_events],
                *[
                    item
                    for sublist in [
                        [
                            overall.get(ev, 0),
                            json.dumps(breakdowns_by_event[ev], sort_keys=True),
                        ]
                        for ev in breakdown_events
                    ]
                    for item in sublist
                ],
                unix_dates.unix_date_to_date(unix_date).isoformat(),
            ),
        )

        if response.rows_affected != 1:
            logging.warning(
                f"{job_name} failed to insert stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )
            existing_row = await cursor.execute(
                f"SELECT * FROM {table_name} WHERE retrieved_for = ?",
                (unix_dates.unix_date_to_date(unix_date).isoformat(),),
            )
            if not existing_row.results:
                raise Exception(
                    f"Failed to insert stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} "
                    "and no existing row was found"
                )
            slack = await itgs.slack()
            msg = (
                f"{job_name} failed to rotate stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}; "
                f"wanted to insert:\n\n"
                f"- `{unique_views=}`\n- `{overall=}`\n- `{breakdowns_by_event=}`\n\n"
                f"but found existing data (`{existing_row.results[0]}`)"
                "\n\ngoing to destroy the data in redis and keep the existing data in the database"
            )
            logging.warning(msg)
            await slack.send_ops_message(msg, preview=f"{job_name} Rotation Failure")
        else:
            logging.info(
                f"{job_name} successfully inserted stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )

        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(key_for_date(unix_date))
            for event in breakdown_events:
                await pipe.delete(key_for_date_and_event(unix_date, event))
            await pipe.delete(visitors_key_for_date(unix_date))
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()


def key_for_date(unix_date: int) -> bytes:
    return f"stats:journey_share_links:unique_views:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:journey_share_links:unique_views:daily:{unix_date}:extra:{event}".encode(
        "ascii"
    )


def visitors_key_for_date(unix_date: int) -> bytes:
    return f"journey_share_links:visitors:{unix_date}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.stats.daily_journey_share_link_unique_view_stats"
            )

    asyncio.run(main())
