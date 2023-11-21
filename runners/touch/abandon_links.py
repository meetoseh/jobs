"""Helper job to abandon links by code, usually used as a failure callback for a touch"""
from typing import List
from error_middleware import handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
from lib.touch.links import abandon_link

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, codes: List[str]):
    """Abandon the links with the given codes

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        codes (list[str]): the codes of the links to abandon
    """
    for code in codes:
        try:
            await abandon_link(itgs, code=code)
        except Exception as e:
            await handle_error(e, extra_info=f"Failed to abandon link with {code=}")


if __name__ == "__main__":
    import asyncio
    import json

    async def main():
        links = json.loads(input("Codes (json list): "))
        assert isinstance(links, list)
        assert all(isinstance(link, str) for link in links)
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.abandon_links", codes=links)

    asyncio.run(main())
