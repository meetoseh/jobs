"""Rotates touch send stats to the database"""
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import pytz
import lib.stats.rotate

category = JobCategory.LOW_RESOURCE_COST

REGULAR_EVENTS = ("queued",)
"""Which events aren't broken down by an additional dimension"""

BREAKDOWN_EVENTS = ("attempted", "reachable", "unreachable")
"""Which events are broken down by an additional dimension."""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue touch send statistics from redis to the database. Specifically,
    this rotates data from before yesterday in the America/Los_Angeles timezone.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    await lib.stats.rotate.rotate(
        itgs,
        gd,
        job_name=__name__,
        regular_events=REGULAR_EVENTS,
        breakdown_events=BREAKDOWN_EVENTS,
        table_name="touch_send_stats",
        earliest_key=b"stats:touch_send:daily:earliest",
        tz=pytz.timezone("America/Los_Angeles"),
        key_for_date=key_for_date,
        key_for_date_and_event=key_for_date_and_event,
        num_days_held=2,
    )


def key_for_date(unix_date: int) -> bytes:
    return f"stats:touch_send:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:touch_send:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_touch_send_stats")

    asyncio.run(main())