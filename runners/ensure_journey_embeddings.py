"""Queues runners.generate_journey_embeddings if journey_embeddings_needs_refresh is set or
journey_embeddings is not set
"""

import logging
from typing import Optional, cast
from itgs import Itgs
from graceful_death import GracefulDeath
import socket

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Queues another job to generate journey embeddings if needed

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    redis = await itgs.redis()
    if not await redis.exists(b"journey_embeddings"):
        slack = await itgs.slack()
        await slack.send_ops_message(
            f"`{socket.gethostname()}` `{__name__}` queueing job to generate journey embeddings (none available)"
        )
        jobs = await itgs.jobs()
        await jobs.enqueue("runners.generate_journey_embeddings")
        return

    need_refresh = cast(
        Optional[bytes], await redis.get(b"journey_embeddings_needs_refresh")
    )
    if need_refresh is not None:
        hint = need_refresh.decode("utf-8")
        slack = await itgs.slack()
        await slack.send_ops_message(
            f"`{socket.gethostname()}` `{__name__}` queueing job to generate journey embeddings\n\n```\n{hint}\n```"
        )
        jobs = await itgs.jobs()
        await jobs.enqueue("runners.generate_journey_embeddings")
        return

    logging.info("Journey embeddings are up to date")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.ensure_journey_embeddings")

    asyncio.run(main())
