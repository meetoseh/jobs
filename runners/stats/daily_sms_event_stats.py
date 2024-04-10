"""Rotates sms event stats to the database"""

from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import pytz
import lib.stats.rotate

category = JobCategory.LOW_RESOURCE_COST

REGULAR_EVENTS = ("found",)
"""Which events aren't broken down by an additional dimension"""

BREAKDOWN_EVENTS = (
    "attempted",
    "received_via_webhook",
    "received_via_polling",
    "pending",
    "succeeded",
    "failed",
    "updated",
    "duplicate",
    "out_of_order",
    "removed",
    "unknown",
)
"""Which events are broken down by an additional dimension."""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue sms event statistics from redis
    to the database. Specifically, this rotates data from 2 days ago in the
    America/Los_Angeles timezone.

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
        table_name="sms_event_stats",
        earliest_key=b"stats:sms_events:daily:earliest",
        tz=pytz.timezone("America/Los_Angeles"),
        key_for_date=key_for_date,
        key_for_date_and_event=key_for_date_and_event,
        num_days_held=2,
    )


def key_for_date(unix_date: int) -> bytes:
    return f"stats:sms_events:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:sms_events:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_sms_event_stats")

    asyncio.run(main())
