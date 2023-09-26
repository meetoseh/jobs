"""Helper module for rotating a standard stats table, which looks like

CREATE TABLE example (
    id INTEGER PRIMARY KEY,
    retrieved_for TEXT UNIQUE NOT NULL,
    retrieved_at REAL NOT NULL,
    basic_field INTEGER NOT NULL,
    fancy_field INTEGER NOT NULL,
    fancy_field_breakdown TEXT NOT NULL
);

where the basic field is a single integer, and a fancy field is the 
combination of an integer and a json object whose values are integers
which sum up to the fancy field and keys act as a further breakdown
of the fancy field.
"""

import json
import time
from typing import Callable, Dict, List
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from lib.process_redis_hgetall import process_redis_hgetall_ints
import unix_dates
import pytz


async def rotate(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    job_name: str,
    regular_events: List[str],
    breakdown_events: List[str],
    table_name: str,
    earliest_key: bytes,
    tz: pytz.BaseTzInfo,
    key_for_date: Callable[[int], bytes],
    key_for_date_and_event: Callable[[int, str], bytes],
    num_days_held: int,
):
    """Rotates statistics from redis to a database table. This assumes
    statistics are handled in the same way that the backend module
    `admin.lib.read_daily_stats` expects:

    - There is a table with the following schema

     ```sql
    CREATE TABLE my_daily_stats (
        id INTEGER PRIMARY KEY,
        retrieved_for TEXT UNIQUE NOT NULL,
        retrieved_at REAL NOT NULL,
        my_basic_stat INTEGER NOT NULL,
        my_fancy_stat INTEGER NOT NULL,
        my_fancy_stat_breakdown TEXT NOT NULL
    );
    ```

    where the basic stats are a single integer column and fancy stats
    are both an integer column and a text column, where the text column
    is named with the _breakdown suffix and corresponds to a json object
    whose keys are strings and whose values are integers which sum up
    to the fancy stat.

    - The following redis keys exist:
      - A key per unix date that goes to a hash whose keys are the basic
        stat names and whose values are integers (`key_for_date`)
      - A key per unix date and fancy field which goes to a hash whose keys
        are strings and values are integers (`key_for_date_and_event`)
      - A key which contains the earliest unix date that has not been
        rotated yet (`earliest_key`)

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        job_name (str): used as the prefix for logging messages, just
            `__name__` is fine if not feeling creative
        regular_events (List[str]): the list of basic stat names
        breakdown_events (List[str]): the list of fancy stat names
        table_name (str): the name of the table to insert into
        earliest_key (bytes): the redis key for the earliest date that
            has not been rotated yet
        tz (pytz.BaseTzInfo): the timezone to use for determining the
            current date so we know how far to rotate
        key_for_date (Callable[[int], bytes]): a function that takes a
            unix date and returns the redis key for that date
        key_for_date_and_event (Callable[[int, str], bytes]): a function
            that takes a unix date and an event name and returns the redis
            key for that date and event
        num_days_held (int): the number of days of stats to hold onto
            before rotating them, typically 2 (today & yesterday)
    """

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
            await pipe.hgetall(key_for_date(unix_date))
            for event in breakdown_events:
                await pipe.hgetall(key_for_date_and_event(unix_date, event))
            result = await pipe.execute()

        assert isinstance(result, (list, tuple)), f"{type(result)=}"
        assert len(result) == 1 + len(breakdown_events), f"{len(result)=}"

        processed_result: List[Dict[str, int]] = []
        for idx, raw in enumerate(result):
            try:
                processed_result.append(process_redis_hgetall_ints(raw))
            except ValueError:
                raise ValueError(f"while processing {idx=} for {unix_date=}")

        overall = processed_result[0]
        breakdowns_by_event = dict(zip(breakdown_events, processed_result[1:]))

        for key in breakdown_events:
            if overall.get(key, 0) != sum(breakdowns_by_event[key].values()):
                await handle_contextless_error(
                    extra_info=f"{job_name} stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} are inconsistent: "
                    f"{key=}, {overall.get(key, 0)=} != {sum(breakdowns_by_event[key].values())=}"
                )

        regular_columns = ", ".join(regular_events)
        breakdown_columns = ", ".join(
            f"{ev}, {ev}_breakdown" for ev in breakdown_events
        )
        qmarks = ", ".join(
            "?" for _ in range(2 + len(regular_events) + 2 * len(breakdown_events))
        )
        response = await cursor.execute(
            f"""
            INSERT INTO {table_name} (
                retrieved_for, 
                retrieved_at, 
                {regular_columns},
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
                f"- {overall=}\n- {breakdowns_by_event=}\n\n"
                f"but found existing data ({existing_row.results[0]})"
                "\n\ngoing to destroy the data in redis and keep the existing data in the database"
            )
            logging.warning(msg)
            await slack.send_ops_message(msg, preview=f"{job_name} Rotation Failure")
        else:
            logging.info(
                f"{job_name} successfully insertedstats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )

        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(key_for_date(unix_date))
            for event in breakdown_events:
                await pipe.delete(key_for_date_and_event(unix_date, event))
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()
