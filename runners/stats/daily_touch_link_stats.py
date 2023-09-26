"""Rotates touch link stats to the database"""
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import pytz
import lib.stats.rotate

category = JobCategory.LOW_RESOURCE_COST

BREAKDOWN_EVENTS = (
    "persist_queue_failed",
    "persists_queued",
    "persisted",
    "persists_failed",
    "clicks_buffered",
    "clicks_direct_to_db",
    "clicks_delayed",
    "clicks_failed",
    "persisted_clicks",
    "delayed_clicks_persisted",
    "delayed_clicks_failed",
    "abandoned",
    "abandon_failed",
    "leaked",
)
"""Which events are broken down by an additional dimension.
"""

REGULAR_EVENTS = (
    "created",
    "persist_queue_attempts",
    "persisted_in_failed_batch",
    "click_attempts",
    "persisted_clicks_in_failed_batch",
    "persist_click_failed",
    "delayed_clicks_attempted",
    "delayed_clicks_delayed",
    "abandons_attempted",
)
"""Which events are not broken down by an additional dimension"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue touch link statistics from redis to the database. Specifically,
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
        table_name="touch_link_stats",
        earliest_key=b"stats:touch_links:daily:earliest",
        tz=pytz.timezone("America/Los_Angeles"),
        key_for_date=key_for_date,
        key_for_date_and_event=key_for_date_and_event,
        num_days_held=2,
    )


def key_for_date(unix_date: int) -> bytes:
    return f"stats:touch_links:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:touch_links:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_touch_link_stats")

    asyncio.run(main())
