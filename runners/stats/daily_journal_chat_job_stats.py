"""Rotates journal stats to the database"""

from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import pytz
import lib.stats.rotate

category = JobCategory.LOW_RESOURCE_COST

REGULAR_EVENTS = tuple()
"""Which events aren't broken down by an additional dimension"""

BREAKDOWN_EVENTS = (
    "requested",
    "failed_to_queue",
    "queued",
    "started",
    "completed",
    "failed",
)
"""Which events are broken down by an additional dimension."""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Rotates any overdue journal statistics from redis to the database. Specifically,
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
        table_name="journal_chat_job_stats",
        earliest_key=b"stats:journal_chat_jobs:daily:earliest",
        tz=pytz.timezone("America/Los_Angeles"),
        key_for_date=key_for_date,
        key_for_date_and_event=key_for_date_and_event,
        num_days_held=2,
    )


def key_for_date(unix_date: int) -> bytes:
    return f"stats:journal_chat_jobs:daily:{unix_date}".encode("ascii")


def key_for_date_and_event(unix_date: int, event: str) -> bytes:
    return f"stats:journal_chat_jobs:daily:{unix_date}:extra:{event}".encode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.stats.daily_journal_chat_job_stats")

    asyncio.run(main())
