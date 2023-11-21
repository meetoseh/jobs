"""Double checks the daily reminder counts are still accurate"""
from typing import Dict, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import asyncio
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Checks that the daily reminder counts key in redis still matches the
    database. If it's wrong for 5 consecutive checks by the same amount, it's
    updated

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    redis = await itgs.redis()

    seen_drift: Optional[Dict[str, int]] = None
    streak = 0

    while True:
        if gd.received_term_signal:
            logging.info("Daily reminders recheck counts interrupted, exiting")
            break

        response = await cursor.execute(
            "SELECT channel, COUNT(*) from user_daily_reminders GROUP BY channel"
        )

        if not response.results:
            logging.warning("No daily reminders found, ignoring redis")
            break

        real_counts = {channel: count for channel, count in response.results}
        redis_counts_bytes: Dict[bytes, bytes] = await redis.hgetall(  # type: ignore
            b"daily_reminders:counts"  # type: ignore
        )
        redis_counts = {
            channel.decode("utf-8"): int(count)
            for channel, count in redis_counts_bytes.items()
        }

        drift = dict()
        for key in set(list(real_counts.keys()) + list(redis_counts.keys())):
            real_count = real_counts.get(key, 0)
            redis_count = redis_counts.get(key, 0)
            if real_count != redis_count:
                drift[key] = real_count - redis_count

        if not drift:
            logging.info("Daily reminders counts match, exiting")
            break

        if seen_drift != drift:
            logging.debug(f"Found new drift: {drift=}")
            seen_drift = drift
            streak = 0
            await asyncio.sleep(1)
            continue

        streak += 1
        if streak < 5:
            logging.debug(f"Continued streak, new streak: {streak=}")
            await asyncio.sleep(1)
            continue

        await handle_warning(
            f"{__name__}:drift",
            "Daily reminder redis counts are out of sync with the database. "
            f"Applying drift correction:\n\n```\n{drift=}\n```",
        )
        async with redis.pipeline() as pipe:
            pipe.multi()
            for channel, count in drift.items():
                await pipe.hincrby(b"daily_reminders:counts", channel, count)  # type: ignore
            await pipe.execute()
        break


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.daily_reminders.recheck_counts")

    asyncio.run(main())
