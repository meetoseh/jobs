from typing import List
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import random

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Randomly seeds some votes for emotions, so that users don't see a tiny number
    of people selecting any given emotion. See backend/emotions/lib/emotion_users.py

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    redis = await itgs.redis()
    key = b"emotion_users:choices"

    response = await cursor.execute("SELECT word FROM emotions")
    emotions: List[str] = [row[0] for row in response.results or []]
    if not emotions:
        logging.info("No emotions, so no votes to seed")
        await redis.delete(key)
        return

    votes = [random.randint(20, 50) for _ in emotions]
    total_votes = sum(votes)

    async with redis.pipeline(transaction=True) as pipe:
        pipe.multi()
        await pipe.delete(key)

        for emotion, votes in zip(emotions, votes):
            await pipe.hincrby(key, emotion.encode("utf-8"), votes)  # type: ignore
        await pipe.hincrby(key, b"__total", total_votes)  # type: ignore
        await pipe.execute()

    logging.info(f"Seeded {total_votes} votes for {len(emotions)} emotions")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.reseed_emotion_votes")

    asyncio.run(main())
