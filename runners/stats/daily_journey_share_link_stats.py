"""Rotates journey share link stats to the database"""
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import pytz
import lib.stats.rotate

category = JobCategory.LOW_RESOURCE_COST

REGULAR_EVENTS = ("view_hydration_requests", "view_hydration_rejected")
"""Which events aren't broken down by an additional dimension"""

BREAKDOWN_EVENTS = (
    "created",
    "reused",
    "view_hydrated",
    "view_hydration_failed",
    "view_client_confirmation_requests",
    "view_client_confirmed",
    "view_client_confirm_failed",
    "view_client_follow_requests",
    "view_client_followed",
    "view_client_follow_failed",
)
"""Which events are broken down by an additional dimension."""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue journey share link statistics from redis to the database. Specifically,
    this rotates data from 2 days ago in the America/Los_Angeles timezone.

    Note that unique views are rotated separately; see
    `daily_journey_share_link_unique_view_stats.py`. They are also
    slightly irregular.

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
        table_name="journey_share_link_stats",
        earliest_key=b"stats:journey_share_links:daily:earliest",
        tz=pytz.timezone("America/Los_Angeles"),
        key_for_date=key_for_date,
        key_for_date_and_event=key_for_date_and_event,
        num_days_held=2,
    )


def key_for_date(unix_date: int) -> bytes:
    return f"stats:journey_share_links:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:journey_share_links:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_journey_share_link_stats")

    asyncio.run(main())
