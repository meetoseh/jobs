"""Checks the frontend is still responding to index.html requests with 200 OK"""

import json
from typing import Union
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import aiohttp
import asyncio
import os


category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, num_requests: int = 4, path: str = "/index.html"
):
    """Verifies the frontend is still responding to index.html requests with 200 OK.
    Does 4 requests in parallel, and if any of them fail, raises a warning to slack

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        num_requests (int): the number of requests to make in parallel
        path (str): the path to request
    """
    url = os.environ["ROOT_FRONTEND_URL"] + path
    try:
        async with aiohttp.ClientSession() as session:

            async def request() -> Union[int, str]:
                try:
                    async with session.get(url) as response:
                        return response.status
                except Exception as e:
                    return str(e)

            statuses = await asyncio.gather(*[request() for _ in range(num_requests)])
    except Exception as e:
        statuses = [str(e) for _ in range(num_requests)]

    num_succeeded = sum(status == 200 for status in statuses)
    if num_succeeded != num_requests:
        warning = f"Frontend health check *failed*: {num_succeeded}/{num_requests} succeeded:\n```{json.dumps(statuses)}```"
        if os.environ["ENVIRONMENT"] == "dev":
            logging.warning(warning)
        else:
            await handle_warning(f"{__name__}:failed", warning, is_urgent=True)
    else:
        logging.debug("Frontend health check succeeded")


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.alerting.check_frontend")

    asyncio.run(main())
