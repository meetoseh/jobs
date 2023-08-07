"""Rotates push token stats from redis to the database"""
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
    """Rotates overdue push token stats from redis to the database. This job
    needs to run reasonably close to midnight (but after midnight) to ensure
    the `total` field is accurate.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    curr_unix_date = unix_dates.unix_date_today(tz=tz)
    redis = await itgs.redis()
    earliest_key = b"stats:push_tokens:daily:earliest"
    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info("Push token stats have not been initialized yet")
        return

    earliest_stored_date = int(earliest_stored_date)

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    for unix_date in range(earliest_stored_date, curr_unix_date):
        if gd.received_term_signal:
            logging.info(
                f"Not rotating further dates (term signal received) (next date: {unix_dates.unix_date_to_date(unix_date).isoformat()})"
            )
            return
        logging.debug(
            f"Rotating push token stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
        )
        raw_values = await redis.hgetall(
            f"stats:push_tokens:daily:{unix_date}".encode("ascii")
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
            created = int(raw_values.get(b"created", 0))
            reassigned = int(raw_values.get(b"reassigned", 0))
            refreshed = int(raw_values.get(b"refreshed", 0))
            deleted_due_to_user_deletion = int(
                raw_values.get(b"deleted_due_to_user_deletion", 0)
            )
            deleted_due_to_unrecognized_ticket = int(
                raw_values.get(b"deleted_due_to_unrecognized_ticket", 0)
            )
            deleted_due_to_unrecognized_receipt = int(
                raw_values.get(b"deleted_due_to_unrecognized_receipt", 0)
            )
            deleted_due_to_token_limit = int(
                raw_values.get(b"deleted_due_to_token_limit", 0)
            )
        except Exception as exc:
            await handle_error(
                exc,
                extra_info=f"while rotating push token stats; {raw_values=}",
            )
            raise

        response = await cursor.execute(
            """
            SELECT COUNT(*) FROM user_push_tokens
            WHERE created_at < ?
            """,
            (unix_dates.unix_date_to_timestamp(unix_date + 1, tz=tz),),
        )
        total: int = response.results[0][0]

        logging.debug(
            f"successfully fetched push token stats as of EOD {unix_dates.unix_date_to_date(unix_date).isoformat()}: "
            f"{created=}, {reassigned=}, {refreshed=}, {deleted_due_to_user_deletion=}, "
            f"{deleted_due_to_unrecognized_ticket=}, {deleted_due_to_unrecognized_receipt=}, "
            f"{deleted_due_to_token_limit=}, {total=}"
        )

        response = await cursor.execute(
            """
            INSERT INTO push_token_stats (
                retrieved_for,
                retrieved_at,
                created,
                reassigned,
                refreshed,
                deleted_due_to_user_deletion,
                deleted_due_to_unrecognized_ticket,
                deleted_due_to_unrecognized_receipt,
                deleted_due_to_token_limit,
                total
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM push_token_stats
                WHERE push_token_stats.retrieved_for = ?
            )
            """,
            (
                unix_dates.unix_date_to_date(unix_date).isoformat(),
                time.time(),
                created,
                reassigned,
                refreshed,
                deleted_due_to_user_deletion,
                deleted_due_to_unrecognized_ticket,
                deleted_due_to_unrecognized_receipt,
                deleted_due_to_token_limit,
                total,
                unix_dates.unix_date_to_date(unix_date).isoformat(),
            ),
        )
        if response.rows_affected != 1:
            logging.warning(
                f"Failed to insert push token stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )
            existing_row = await cursor.execute(
                """
                SELECT
                    created,
                    reassigned,
                    refreshed,
                    deleted_due_to_user_deletion,
                    deleted_due_to_unrecognized_ticket,
                    deleted_due_to_unrecognized_receipt,
                    deleted_due_to_token_limit,
                    total
                FROM push_token_stats
                WHERE push_token_stats.retrieved_for = ?
                """,
                (unix_dates.unix_date_to_date(unix_date).isoformat(),),
            )
            if not existing_row.results:
                raise Exception(
                    f"Failed to insert push token stats for {unix_dates.unix_date_to_date(unix_date).isoformat()} "
                    "and no existing row was found"
                )
            slack = await itgs.slack()
            msg = (
                f"Failed to rotate daily push token stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}; "
                f"wanted to insert:\n\n"
                f"- {created=}\n- {reassigned=}\n- {refreshed=}\n- {deleted_due_to_user_deletion=}\n"
                f"- {deleted_due_to_unrecognized_ticket=}\n- {deleted_due_to_unrecognized_receipt=}\n"
                f"- {deleted_due_to_token_limit=}\n- {total=}"
                f"\n\nbut found:\n\n"
                f"- created={existing_row.results[0][0]}\n- reassigned={existing_row.results[0][1]}\n"
                f"- refreshed={existing_row.results[0][2]}\n- deleted_due_to_user_deletion={existing_row.results[0][3]}\n"
                f"- deleted_due_to_unrecognized_ticket={existing_row.results[0][4]}\n"
                f"- deleted_due_to_unrecognized_receipt={existing_row.results[0][5]}\n"
                f"- deleted_due_to_token_limit={existing_row.results[0][6]}\n- total={existing_row.results[0][7]}"
                "\n\ngoing to destroy the data in redis and keep the found data in the database"
            )
            logging.warning(msg)
            await slack.send_ops_message(msg)
        else:
            logging.info(
                f"Successfully inserted push token stats for {unix_dates.unix_date_to_date(unix_date).isoformat()}"
            )

        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(f"stats:push_tokens:daily:{unix_date}".encode("ascii"))
            await pipe.set(earliest_key, unix_date + 1)
            await pipe.execute()


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_push_token_stats")

    asyncio.run(main())
