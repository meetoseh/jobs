"""Rotates sign in with oseh authorize stats to the database"""
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import pytz
import lib.stats.rotate

category = JobCategory.LOW_RESOURCE_COST

REGULAR_EVENTS = (
    "check_attempts",
    "check_elevation_acknowledged",
    "login_attempted",
    "create_attempted",
    "password_reset_attempted",
    "password_update_attempted",
)
"""Which events aren't broken down by an additional dimension"""

BREAKDOWN_EVENTS = (
    "check_failed",
    "check_elevated",
    "check_elevation_failed",
    "check_elevation_succeeded",
    "check_succeeded",
    "login_failed",
    "login_succeeded",
    "create_failed",
    "create_succeeded",
    "password_reset_failed",
    "password_reset_confirmed",
    "password_update_failed",
    "password_update_succeeded",
)
"""Which events are broken down by an additional dimension."""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue sign in with oseh authorize statistics from redis to the database. Specifically,
    this rotates data from 2 days ago in the America/Los_Angeles timezone.

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
        table_name="siwo_authorize_stats",
        earliest_key=b"stats:sign_in_with_oseh:authorize:daily:earliest",
        tz=pytz.timezone("America/Los_Angeles"),
        key_for_date=key_for_date,
        key_for_date_and_event=key_for_date_and_event,
        num_days_held=2,
    )


def key_for_date(unix_date: int) -> bytes:
    return f"stats:sign_in_with_oseh:authorize:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:sign_in_with_oseh:authorize:daily:{unix_date}:extra:{event}".encode(
        "ascii"
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_siwo_authorize_stats")

    asyncio.run(main())
