"""Rotates push ticket stats from redis to the database"""
import time
from error_middleware import handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates overdue push ticket stats from redis to the database. Because
    push ticket stats are backdated due to the time they were initially queued
    at, this will only move data that is over a day old (i.e., from 2 days ago).
    Today and yesterday will remain in redis until tomorrow.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    curr_unix_date = unix_dates.unix_date_today(tz=tz)
    redis = await itgs.redis()
    earliest_key = b"stats:push_tickets:daily:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info("Push ticket stats have not been initialized yet")
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
            f"Rotating push ticket stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
        )
        raw_values = await redis.hgetall(  # type: ignore
            f"stats:push_tickets:daily:{unix_date}".encode("ascii")  # type: ignore
        )
        if raw_values is None:
            raw_values = dict()
        else:
            # ensure we are bytes-bytes since the library makes no promises and
            # we don't want silent failure in the next part
            raw_values = dict(
                (
                    k.encode("ascii") if isinstance(k, str) else k,
                    v.encode("ascii") if isinstance(v, str) else v,
                )
                for k, v in raw_values.items()
            )
            assert all(
                isinstance(k, bytes) for k in raw_values.keys()
            ), raw_values.keys()
            assert all(
                isinstance(v, bytes) for v in raw_values.values()
            ), raw_values.values()

        try:
            queued = int(raw_values.get(b"queued", 0))
            succeeded = int(raw_values.get(b"succeeded", 0))
            abandoned = int(raw_values.get(b"abandoned", 0))
            failed_due_to_device_not_registered = int(
                raw_values.get(b"failed_due_to_device_not_registered", 0)
            )
            failed_due_to_client_error_other = int(
                raw_values.get(b"failed_due_to_client_error_other", 0)
            )
            failed_due_to_internal_error = int(
                raw_values.get(b"failed_due_to_internal_error", 0)
            )
            retried = int(raw_values.get(b"retried", 0))
            failed_due_to_client_error_429 = int(
                raw_values.get(b"failed_due_to_client_error_429", 0)
            )
            failed_due_to_server_error = int(
                raw_values.get(b"failed_due_to_server_error", 0)
            )
            failed_due_to_network_error = int(
                raw_values.get(b"failed_due_to_network_error", 0)
            )
        except Exception as exc:
            await handle_error(
                exc,
                extra_info=f"while rotating push ticket stats; {raw_values=}",
            )
            raise

        logging.debug(
            f"successfully fetched push ticket stats as of EOD {unix_dates.unix_date_to_date(unix_date).isoformat()}: "
            f"{queued=}, {succeeded=}, {abandoned=}, "
            f"{failed_due_to_device_not_registered=}, {failed_due_to_client_error_other=}, "
            f"{failed_due_to_internal_error=}, {retried=}, {failed_due_to_client_error_429=}, "
            f"{failed_due_to_server_error=}, {failed_due_to_network_error=}"
        )

        response = await cursor.execute(
            """
            INSERT INTO push_ticket_stats (
                retrieved_for,
                retrieved_at,
                queued,
                succeeded,
                abandoned,
                failed_due_to_device_not_registered,
                failed_due_to_client_error_other,
                failed_due_to_internal_error,
                retried,
                failed_due_to_client_error_429,
                failed_due_to_server_error,
                failed_due_to_network_error
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE
                NOT EXISTS (
                    SELECT 1 FROM push_ticket_stats AS pts
                    WHERE pts.retrieved_for = ?
                )
            """,
            (
                unix_dates.unix_date_to_date(unix_date).isoformat(),
                time.time(),
                queued,
                succeeded,
                abandoned,
                failed_due_to_device_not_registered,
                failed_due_to_client_error_other,
                failed_due_to_internal_error,
                retried,
                failed_due_to_client_error_429,
                failed_due_to_server_error,
                failed_due_to_network_error,
                unix_dates.unix_date_to_date(unix_date).isoformat(),
            ),
        )

        if response.rows_affected != 1:
            logging.warning(
                f"Failed to insert push ticket stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )
            existing_row = await cursor.execute(
                """
                SELECT
                    queued,
                    succeeded,
                    abandoned,
                    failed_due_to_device_not_registered,
                    failed_due_to_client_error_other,
                    failed_due_to_internal_error,
                    retried,
                    failed_due_to_client_error_429,
                    failed_due_to_server_error,
                    failed_due_to_network_error
                FROM push_ticket_stats
                WHERE retrieved_for = ?
                """,
                (unix_dates.unix_date_to_date(unix_date).isoformat(),),
            )
            if not existing_row.results:
                raise Exception(
                    f"Failed to insert push ticket stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} "
                    "and no existing row was found"
                )
            slack = await itgs.slack()
            msg = (
                f"Failed to rotate daily push ticket stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}; "
                f"wanted to insert:\n\n"
                f"- {queued=}\n- {succeeded=}\n- {abandoned=}\n- {failed_due_to_device_not_registered=}\n"
                f"- {failed_due_to_client_error_other=}\n- {failed_due_to_internal_error=}\n"
                f"- {retried=}\n- {failed_due_to_client_error_429=}\n- {failed_due_to_server_error=}\n"
                f"- {failed_due_to_network_error=}"
                f"\n\nbut found:\n\n"
                f"- queued={existing_row.results[0][0]}\n- succeeded={existing_row.results[0][1]}\n"
                f"- abandoned={existing_row.results[0][2]}\n- failed_due_to_device_not_registered={existing_row.results[0][3]}\n"
                f"- failed_due_to_client_error_other={existing_row.results[0][4]}\n- failed_due_to_internal_error={existing_row.results[0][5]}\n"
                f"- retried={existing_row.results[0][6]}\n- failed_due_to_client_error_429={existing_row.results[0][7]}\n"
                f"- failed_due_to_server_error={existing_row.results[0][8]}\n- failed_due_to_network_error={existing_row.results[0][9]}\n"
                "\n\ngoing to destroy the data in redis and keep the found data in the database"
            )
            logging.warning(msg)
            await slack.send_ops_message(msg)
        else:
            logging.info(
                f"Successfully inserted push ticket stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )

        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(f"stats:push_tickets:daily:{unix_date}".encode("ascii"))
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_push_ticket_stats")

    asyncio.run(main())
