from itgs import Itgs
from graceful_death import GracefulDeath
from unix_dates import unix_timestamp_to_unix_month, unix_month_to_date_of_first
from jobs import JobCategory
import time
import pytz
from datetime import datetime

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Repairs some redis statistics which can be recovered from the database. This is
    primarily used in development when restoring from a database backup. It does not
    completely recover all the redis keys which are required for accurate statistics.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    # TODO: this hasn't been tested since journeys -> interactive prompts
    # TODO: missing user_notification_settings_stats

    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    redis = await itgs.redis()

    response = await cursor.execute(
        """
        SELECT COUNT(*) FROM interactive_prompt_sessions
        WHERE 
            EXISTS (
                SELECT 1 FROM interactive_prompt_events
                WHERE interactive_prompt_events.interactive_prompt_session_id = interactive_prompt_sessions.id
            )
        """
    )
    await redis.set(
        b"stats:interactive_prompt_sessions:count",
        str(response.results[0][0]).encode("utf-8"),
    )

    current_month = unix_timestamp_to_unix_month(
        time.time(), tz=pytz.timezone("America/Los_Angeles")
    )
    start_of_current_month = unix_month_to_date_of_first(current_month)
    start_of_current_month_datetime = datetime(
        year=start_of_current_month.year,
        month=start_of_current_month.month,
        day=start_of_current_month.day,
        tzinfo=pytz.timezone("America/Los_Angeles"),
    )
    response = await cursor.execute(
        """
        SELECT COUNT(*) FROM interactive_prompt_sessions
        WHERE
            EXISTS (
                SELECT 1 FROM interactive_prompt_events
                WHERE interactive_prompt_events.interactive_prompt_id = interactive_prompts.id
                  AND interactive_prompt_events.created_at >= ?
                  AND interactive_prompt_events.evtype = 'join'
            )
        """,
        (start_of_current_month_datetime.timestamp(),),
    )
    await redis.set(
        f"stats:interactive_prompts:monthly:{current_month}:count".encode("ascii"),
        str(response.results[0][0]).encode("utf-8"),
    )
    await redis.set(
        b"stats:interactive_prompt_sessions:monthly:earliest",
        str(current_month).encode("ascii"),
        nx=True,
    )

    response = await cursor.execute("SELECT COUNT(*) FROM users")
    await redis.set(b"stats:users:count", str(response.results[0][0]).encode("utf-8"))

    response = await cursor.execute(
        "SELECT COUNT(*) FROM users WHERE created_at >= ?",
        (start_of_current_month_datetime.timestamp(),),
    )
    await redis.set(
        f"stats:users:monthly:{current_month}:count".encode("ascii"),
        str(response.results[0][0]).encode("utf-8"),
    )
    await redis.set(
        b"stats:users:monthly:earliest", str(current_month).encode("ascii"), nx=True
    )

    response = await cursor.execute("SELECT COUNT(*) FROM instructors")
    await redis.set(
        b"stats:instructors:count", str(response.results[0][0]).encode("utf-8")
    )

    response = await cursor.execute(
        "SELECT COUNT(*) FROM instructors WHERE created_at >= ?",
        (start_of_current_month_datetime.timestamp(),),
    )
    await redis.set(
        f"stats:instructors:monthly:{current_month}:count".encode("ascii"),
        str(response.results[0][0]).encode("utf-8"),
    )
    await redis.set(
        b"stats:instructors:monthly:earliest",
        str(current_month).encode("ascii"),
        nx=True,
    )

    response = await cursor.execute("SELECT COUNT(*) FROM journeys")
    await redis.set(
        b"stats:journeys:count", str(response.results[0][0]).encode("utf-8")
    )

    response = await cursor.execute(
        "SELECT COUNT(*) FROM journeys WHERE created_at >= ?",
        (start_of_current_month_datetime.timestamp(),),
    )
    await redis.set(
        f"stats:journeys:monthly:{current_month}:count".encode("ascii"),
        str(response.results[0][0]).encode("utf-8"),
    )
    await redis.set(
        b"stats:journeys:monthly:earliest", str(current_month).encode("ascii"), nx=True
    )

    response = await cursor.execute(
        """
        SELECT sub FROM users 
        WHERE
            EXISTS (
                SELECT 1 FROM interactive_prompt_events
                WHERE interactive_prompt_events.created_at >= ?
                  AND EXISTS (
                    SELECT 1 FROM interactive_prompt_sessions
                    WHERE interactive_prompt_sessions.id = interactive_prompt_events.interactive_prompt_session_id
                        AND interactive_prompt_sessions.user_id = users.id
                  )
            )
        ORDER BY sub ASC
        LIMIT 1000
        """,
        (start_of_current_month_datetime.timestamp(),),
    )
    while response.results:
        await redis.sadd(
            f"stats:monthly_active_users:{current_month}".encode("ascii"),
            *[row[0].encode("ascii") for row in response.results],
        )

        response = await cursor.execute(
            """
            SELECT sub FROM users 
            WHERE
                EXISTS (
                    SELECT 1 FROM interactive_prompt_events
                    WHERE interactive_prompt_events.created_at >= ?
                    AND EXISTS (
                        SELECT 1 FROM interactive_prompt_sessions
                        WHERE interactive_prompt_sessions.id = interactive_prompt_events.interactive_prompt_session_id
                            AND interactive_prompt_sessions.user_id = users.id
                    )
                )
                AND sub > ?
            ORDER BY sub ASC
            LIMIT 1000
            """,
            (start_of_current_month_datetime.timestamp(), response.results[-1][0]),
        )

    await redis.set(
        b"stats:monthly_active_users:earliest",
        str(current_month).encode("ascii"),
        nx=True,
    )


if __name__ == "__main__":

    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.repair_redis")

    asyncio.run(main())
