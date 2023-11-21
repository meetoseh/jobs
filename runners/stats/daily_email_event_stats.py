"""Rotates email event stats to the database"""
import json
import time
from typing import Dict, List
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.process_redis_hgetall import process_redis_hgetall_ints
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST

BREAKDOWN_EVENTS = ("attempted", "succeeded", "bounced", "complaint")
"""Which events are broken down by an additional dimension.

NOTE: The order and number of these is coupled to the implementation below to improve
logging and error handling.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue email event statistics from redis to the database. Specifically,
    this rotates data from before yesterday in the America/Los_Angeles timezone.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    curr_unix_date = unix_dates.unix_date_today(tz=tz)
    redis = await itgs.redis()
    earliest_key = b"stats:email_events:daily:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info("Email event stats have not been initialized yet")
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
            f"Rotating email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
        )

        async with redis.pipeline(transaction=False) as pipe:
            await pipe.hgetall(key_for_date(unix_date))  # type: ignore
            for event in BREAKDOWN_EVENTS:
                await pipe.hgetall(key_for_date_and_event(unix_date, event))  # type: ignore
            result = await pipe.execute()

        assert isinstance(result, (list, tuple)), f"{type(result)=}"
        assert len(result) == 5, f"{len(result)=}"

        processed_result: List[Dict[str, int]] = []
        for idx, raw in enumerate(result):
            try:
                processed_result.append(process_redis_hgetall_ints(raw))
            except ValueError:
                raise ValueError(f"while processing {idx=} for {unix_date=}")

        (
            overall,
            attempted_breakdown,
            succeeded_breakdown,
            bounced_breakdown,
            complaint_breakdown,
        ) = processed_result

        if overall.get("attempted", 0) != sum(attempted_breakdown.values()):
            await handle_contextless_error(
                extra_info=f"Daily email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} are inconsistent: "
                f"{overall.get('attempted', 0)=} != {sum(attempted_breakdown.values())=}"
            )

        if overall.get("succeeded", 0) != sum(succeeded_breakdown.values()):
            await handle_contextless_error(
                extra_info=f"Daily email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} are inconsistent: "
                f"{overall.get('succeeded', 0)=} != {sum(succeeded_breakdown.values())=}"
            )

        if overall.get("bounced", 0) != sum(bounced_breakdown.values()):
            await handle_contextless_error(
                extra_info=f"Daily email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} are inconsistent: "
                f"{overall.get('bounced', 0)=} != {sum(bounced_breakdown.values())=}"
            )

        if overall.get("complaint", 0) != sum(complaint_breakdown.values()):
            await handle_contextless_error(
                extra_info=f"Daily email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} are inconsistent: "
                f"{overall.get('complaint', 0)=} != {sum(complaint_breakdown.values())=}"
            )

        logging.debug(
            f"successfully fetched daily email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}: "
            f"{overall=}, {attempted_breakdown=}, {succeeded_breakdown=}, {bounced_breakdown=}, {complaint_breakdown=}"
        )

        response = await cursor.execute(
            """
            INSERT INTO email_event_stats (
                retrieved_for, retrieved_at, attempted, attempted_breakdown,
                succeeded, succeeded_breakdown, bounced, bounced_breakdown, complaint,
                complaint_breakdown
            )
            SELECT
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE
                NOT EXISTS (
                    SELECT 1 FROM email_event_stats WHERE retrieved_for = ?
                )
            """,
            (
                unix_dates.unix_date_to_date(unix_date).isoformat(),
                time.time(),
                overall.get("attempted", 0),
                json.dumps(attempted_breakdown, sort_keys=True),
                overall.get("succeeded", 0),
                json.dumps(succeeded_breakdown, sort_keys=True),
                overall.get("bounced", 0),
                json.dumps(bounced_breakdown, sort_keys=True),
                overall.get("complaint", 0),
                json.dumps(complaint_breakdown, sort_keys=True),
                unix_dates.unix_date_to_date(unix_date).isoformat(),
            ),
        )

        if response.rows_affected != 1:
            logging.warning(
                f"Failed to insert email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )
            existing_row = await cursor.execute(
                "SELECT * FROM email_event_stats WHERE retrieved_for = ?",
                (unix_dates.unix_date_to_date(unix_date).isoformat(),),
            )
            if not existing_row.results:
                raise Exception(
                    f"Failed to insert email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} "
                    "and no existing row was found"
                )
            slack = await itgs.slack()
            msg = (
                f"Failed to rotate daily email event stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}; "
                f"wanted to insert:\n\n"
                f"- {overall=}\n- {attempted_breakdown=}\n- {succeeded_breakdown=}\n- {bounced_breakdown=}\n- {complaint_breakdown=}\n\n"
                f"but found existing data ({existing_row.results[0]})"
                "\n\ngoing to destroy the data in redis and keep the existing data in the database"
            )
            logging.warning(msg)
            await slack.send_ops_message(
                msg, preview="Daily Email Event Stats Rotation Failure"
            )
        else:
            logging.info(
                f"Successfully inserted daily email stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )

        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(key_for_date(unix_date))
            for event in BREAKDOWN_EVENTS:
                await pipe.delete(key_for_date_and_event(unix_date, event))
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()


def key_for_date(unix_date: int) -> bytes:
    return f"stats:email_events:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:email_events:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_email_event_stats")

    asyncio.run(main())
