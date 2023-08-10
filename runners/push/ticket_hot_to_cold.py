import time
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from redis_helpers.move_cold_to_hot import move_cold_to_hot_safe

category = JobCategory.LOW_RESOURCE_COST


MOVE_BATCH_SIZE = 100
"""How many items we attempt to move at once within redis, to avoid
blocking the redis server for too long.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """The Push Ticket Cold to Hot job. Moves any overdue tickets from the
    Push Receipt Cold Set redis sorted set, to the Push Receipt Hot Set
    redis list. This facilitates batching while also allowing the cold
    set to be moved to a different store if it grows excessively large.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"push:ticket_hot_to_cold:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:push_receipts:cold_to_hot_job", b"last_started_at", started_at
        )

        num_moved = 0
        while True:
            if gd.received_term_signal:
                logging.info("ticket_hot_to_cold interrupting early due to term signal")
                break
            moved_this_batch = await move_cold_to_hot_safe(
                itgs,
                b"push:push_tickets:cold",
                b"push:push_tickets:hot",
                started_at - 60 * 15,
                MOVE_BATCH_SIZE,
            )
            num_moved += moved_this_batch
            if moved_this_batch < MOVE_BATCH_SIZE:
                break

        finished_at = time.time()
        await redis.hset(
            b"stats:push_receipts:cold_to_hot_job",
            mapping={
                b"last_finished_at": finished_at,
                b"last_running_time": finished_at - started_at,
                b"last_num_moved": num_moved,
            },
        )
        logging.info(
            f"Moved {num_moved} push tickets from the push ticket cold set to the push ticket hot set"
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.push.ticket_hot_to_cold")

    asyncio.run(main())
