"""Rotates sms polling stats from redis to the database"""
import json
import time
from typing import Dict, List
from error_middleware import handle_contextless_error, handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.process_redis_hgetall import process_redis_hgetall_ints
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST

BREAKDOWN_EVENTS = (
    "detected_stale",
    "queued_for_recovery",
    "abandoned",
    "received",
    "error_client_other",
    "error_server",
)
"""Which events are broken down by an additional dimension.

NOTE: The order and number of these is coupled to the implementation below to improve
logging and error handling.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates overdue sms polling stats from redis to the database.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    curr_unix_date = unix_dates.unix_date_today(tz=tz)
    redis = await itgs.redis()
    earliest_key = b"stats:sms_polling:daily:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info("SMS polling stats have not been initialized yet")
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
            f"Rotating SMS polling stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
        )

        async with redis.pipeline(transaction=False) as pipe:
            await pipe.hgetall(key_for_date(unix_date))
            for event in BREAKDOWN_EVENTS:
                await pipe.hgetall(key_for_date_and_event(unix_date, event))
            result = await pipe.execute()

        assert isinstance(result, (list, tuple)), f"{type(result)=}"
        assert len(result) == 1 + len(BREAKDOWN_EVENTS), f"{len(result)=}"

        processed_result: List[Dict[str, int]] = []
        for idx, raw in enumerate(result):
            try:
                processed_result.append(process_redis_hgetall_ints(raw))
            except ValueError:
                raise ValueError(f"while processing {idx=} for {unix_date=}")

        overall = processed_result[0]
        breakdowns = dict(zip(BREAKDOWN_EVENTS, processed_result[1:]))

        for breakdown_event in BREAKDOWN_EVENTS:
            overall_value = overall.get(breakdown_event, 0)
            sum_breakdown = sum(breakdowns[breakdown_event].values())
            if overall_value != sum_breakdown:
                await handle_contextless_error(
                    f"Daily SMS polling stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} are inconsistent "
                    f"on {breakdown_event=}: {overall_value=} vs {sum_breakdown=}"
                )

        response = await cursor.execute(
            """
            INSERT INTO sms_polling_stats (
                retrieved_for, retrieved_at, detected_stale, detected_stale_breakdown, queued_for_recovery,
                queued_for_recovery_breakdown, abandoned, abandoned_breakdown, attempted,
                received, received_breakdown, error_client_404, error_client_429, error_client_other,
                error_client_other_breakdown, error_server, error_server_breakdown, error_network,
                error_internal
            )
            SELECT
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE
                NOT EXISTS (
                    SELECT 1 FROM sms_polling_stats WHERE retrieved_for = ?
                )
            """,
            (
                unix_dates.unix_date_to_date(unix_date).isoformat(),
                time.time(),
                overall.get("detected_stale", 0),
                json.dumps(breakdowns["detected_stale"]),
                overall.get("queued_for_recovery", 0),
                json.dumps(breakdowns["queued_for_recovery"]),
                overall.get("abandoned", 0),
                json.dumps(breakdowns["abandoned"]),
                overall.get("attempted", 0),
                overall.get("received", 0),
                json.dumps(breakdowns["received"]),
                overall.get("error_client_404", 0),
                overall.get("error_client_429", 0),
                overall.get("error_client_other", 0),
                json.dumps(breakdowns["error_client_other"]),
                overall.get("error_server", 0),
                json.dumps(breakdowns["error_server"]),
                overall.get("error_network", 0),
                overall.get("error_internal", 0),
                unix_dates.unix_date_to_date(unix_date).isoformat(),
            ),
        )

        if response.rows_affected != 1:
            logging.warning(
                f"Failed to insert sms polling stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )
            existing_row = await cursor.execute(
                "SELECT * FROM sms_polling_stats WHERE retrieved_for = ?",
                (unix_dates.unix_date_to_date(unix_date).isoformat(),),
            )
            if not existing_row.results:
                raise Exception(
                    f"Failed to insert sms polling stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} "
                    "and no existing row was found"
                )
            slack = await itgs.slack()
            msg = (
                f"Failed to rotate daily sms polling stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}; "
                f"wanted to insert:\n\n"
                f"- {overall=}\n"
                f"- {breakdowns=}\n\n"
                f"but found existing data ({existing_row.results[0]})"
                "\n\ngoing to destroy the data in redis and keep the existing data in the database"
            )
            logging.warning(msg)
            await slack.send_ops_message(
                msg, preview="Daily SMS Polling Stats Rotation Failure"
            )
        else:
            logging.info(
                f"Successfully inserted daily polling stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )

        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(key_for_date(unix_date))
            for event in BREAKDOWN_EVENTS:
                await pipe.delete(key_for_date_and_event(unix_date, event))
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()


def key_for_date(unix_date: int) -> bytes:
    return f"stats:sms_polling:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:sms_polling:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_sms_polling_stats")

    asyncio.run(main())
