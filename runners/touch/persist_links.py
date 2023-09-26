"""Helper job to persist links by code, usually used as a success callback for a touch"""
import time
from typing import List
from error_middleware import handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.touch.links import persist_link

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, codes: List[str]):
    """Persists the links with the given codes

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        codes (list[str]): the codes of the links to persist
    """
    now = time.time()
    for code in codes:
        try:
            success = await persist_link(itgs, code=code, now=now)
            if success:
                logging.debug(f"Successfully persisted link with {code=}")
            else:
                logging.debug(
                    f"Link with {code=} was already persisted or did not exist"
                )
        except Exception as e:
            await handle_error(e, extra_info=f"Failed to persist link with {code=}")


if __name__ == "__main__":
    import asyncio
    import json

    async def main():
        links = json.loads(input("Codes (json list): "))
        assert isinstance(links, list)
        assert all(isinstance(link, str) for link in links)
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.persist_links", codes=links)

    asyncio.run(main())
