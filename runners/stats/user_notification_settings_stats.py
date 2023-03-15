"""Rotates completed user notification settings info to redis"""
import time
from typing import List, Tuple
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import unix_dates
from jobs import JobCategory
import pytz

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates user notification setting statistics which are currently in redis for
    faster inserts, but can no longer be changed, into the database.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    tz = pytz.timezone("America/Los_Angeles")
    cur_unix_date = unix_dates.unix_date_today(tz=tz)

    redis = await itgs.redis()
    earliest_key = b"stats:daily_user_notification_settings:earliest"
    unix_date_to_key = (
        lambda unix_date: f"stats:daily_user_notification_settings:{unix_date}".encode(
            "utf-8"
        )
    )

    earliest_stored_date = await redis.get(earliest_key)
    if earliest_stored_date is None:
        logging.info(
            "No earliest date stored for user notification settings stats, not rotating"
        )
        return

    earliest_stored_date = int(earliest_stored_date)
    if earliest_stored_date >= cur_unix_date:
        logging.info(
            "Earliest date stored for user notification settings stats is not yet frozen, not rotating: "
            f"{unix_dates.unix_date_to_date(earliest_stored_date)}"
        )
        return

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    for unix_date_to_rotate in range(earliest_stored_date, cur_unix_date):
        logging.debug(
            f"Rotating {unix_dates.unix_date_to_date(unix_date_to_rotate)} ({unix_date_to_rotate})..."
        )

        retrieved_at = time.time()
        redis_value = await redis.hgetall(unix_date_to_key(unix_date_to_rotate))
        if redis_value is None or len(redis_value) == 0:
            logging.debug("No data to rotate for this date")
            logging.debug(f"redis del {unix_date_to_key(unix_date_to_rotate)}")
            await redis.delete(unix_date_to_key(unix_date_to_rotate))
            logging.debug(f"redis set {earliest_key} {unix_date_to_rotate + 1}")
            await redis.set(earliest_key, unix_date_to_rotate + 1)
            continue

        parsed_values: List[Tuple[str, str, int]] = []
        for preferences, total in redis_value.items():
            if isinstance(preferences, bytes):
                preferences = preferences.decode("utf-8")
            assert isinstance(
                preferences, str
            ), f"{redis_value=}, {preferences=}, {type(preferences)=}"
            total = int(total)

            assert preferences.count(":") == 1, f"{preferences=}"
            old_preference, new_preference = preferences.split(":")

            assert (
                old_preference is not None and old_preference != ""
            ), f"{preferences=}"
            assert (
                new_preference is not None and new_preference != ""
            ), f"{preferences=}"

            parsed_values.append((old_preference, new_preference, total))

        logging.debug(f"Parsed information: {parsed_values}")
        for old_preference, new_preference, total in parsed_values:
            response = await cursor.execute(
                """
                INSERT INTO user_notification_setting_stats (
                    retrieved_for, old_preference, new_preference, retrieved_at, total
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (retrieved_for, old_preference, new_preference) DO NOTHING
                """,
                (
                    unix_dates.unix_date_to_date(unix_date_to_rotate).isoformat(),
                    old_preference,
                    new_preference,
                    retrieved_at,
                    total,
                ),
            )
            logging.debug(
                f"For {old_preference=}, {new_preference=}, {total=}: {response.rows_affected=}"
            )
            if response.rows_affected is None or response.rows_affected < 1:
                await handle_contextless_error(
                    extra_info=(
                        f"user_notification_setting_stats failed to insert {old_preference=}, {new_preference=}, {total=} "
                        f"for {unix_dates.unix_date_to_date(unix_date_to_rotate).isoformat()}"
                    )
                )

        logging.debug(f"redis del {unix_date_to_key(unix_date_to_rotate)}")
        await redis.delete(unix_date_to_key(unix_date_to_rotate))
        logging.debug(f"redis set {earliest_key} {unix_date_to_rotate + 1}")
        await redis.set(earliest_key, unix_date_to_rotate + 1)

    logging.debug("Finished rotating user notification settings stats")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.user_notification_settings_stats")

    asyncio.run(main())
