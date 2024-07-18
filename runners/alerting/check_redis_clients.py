"""
Checks if any of the redis nodes or sentinels have more than 55 clients connected,
which implies something is leaking connections and will eventually cause redis to
stop accepting connections. Eventually, this will cause a redis failover which will
reset the connections, but there is an interval of time where the redis sentinels
are able to connect to the redis nodes but clients can't connect, which is "very bad"
TM.

Originally this happened because pythons redis implementation was not smart about
cleaning up connections to redis sentinel instances, so the max clients connected
was happening on the redis sentinels! Talk about a confusing debugging experience.
"""

import os
from typing import Dict, Literal, cast
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import redis.asyncio
import logging

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Verifies that all redis sentinel servers and redis servers discoverable from the
    sentinels have less than 55 clients connected. If any have more than 55 clients,
    awarning is sent to slack. If they have more than 105 clients, the warning is urgent.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    redis_ips = os.environ["REDIS_IPS"].split(",")
    for ip in redis_ips:
        async with redis.asyncio.Redis(
            host=ip, port=26379, single_connection_client=True
        ) as sentinel:
            await check_instance_clients(sentinel, "sentinel")

    for ip in redis_ips:
        async with redis.asyncio.Redis(
            host=ip, port=6379, single_connection_client=True
        ) as replica:
            await check_instance_clients(replica, "replica")

    redis_leader = await itgs.redis()
    await check_instance_clients(redis_leader, "leader")


async def check_instance_clients(
    redis: redis.asyncio.Redis, category: Literal["sentinel", "replica", "leader"]
) -> None:
    host = await get_redis_host(redis)
    response = cast(Dict, await redis.execute_command("INFO", "clients"))
    num_clients = response.get("connected_clients")
    logging.info(f"redis {host=} has {num_clients=}")
    if not isinstance(num_clients, int):
        await handle_warning(
            f"{__name__}:bad_connected_clients_format",
            f"expected int, got {type(num_clients)}",
        )
        return
    if num_clients > 55:
        await handle_warning(
            f"{__name__}:{category}_too_many_clients:{host}",
            f"{host} has {num_clients} clients connected: may be leaking connections",
            is_urgent=num_clients > 105,
        )


async def get_redis_host(redis: redis.asyncio.Redis) -> str:
    client_info = await redis.client_info()
    return client_info.get("laddr", "unknown")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.alerting.check_redis_clients")

    asyncio.run(main())
