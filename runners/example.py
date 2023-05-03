"""a very simple job"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath, *, message: str):
    """an example job execute - this is invoked when 'runners.example' is the name of the job

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        message (str): the only keyword argument for this job; to be printed out
    """
    logging.info(message)


if __name__ == "__main__":
    import asyncio

    async def main():
        message = input("Message: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.example", message=message)

    asyncio.run(main())
