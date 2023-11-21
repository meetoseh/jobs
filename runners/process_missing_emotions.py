from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Determines what journeys, if any, have no journey emotions attached to
    them and queue the appropriate job

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    jobs = await itgs.jobs()
    batch_size = 50
    last_uid: Optional[str] = None
    num_found = 0

    while True:
        response = await cursor.execute(
            """
            SELECT uid FROM journeys 
            WHERE 
                NOT EXISTS (
                    SELECT 1 FROM journey_emotions 
                    WHERE journey_emotions.journey_id = journeys.id
                )
                AND journeys.deleted_at IS NULL
                AND (? IS NULL OR uid > ?)
            LIMIT ?
            """,
            (
                last_uid,
                last_uid,
                batch_size,
            ),
        )

        if not response.results:
            break

        for row in response.results:
            if gd.received_term_signal:
                logging.debug(
                    "Received term signal, not completing process_missing_journeys"
                )
                return

            uid: str = row[0]
            last_uid = uid

            logging.debug(f"Queueing refresh_journey_emotions for journey {uid=}")
            await jobs.enqueue("runners.refresh_journey_emotions", journey_uid=uid)
            num_found += 1

        if len(response.results) < batch_size:
            break

    logging.debug(f"Finished process_missing_emotions (handled {num_found=} journeys)")


if __name__ == "__main__":
    import asyncio
    import yaml
    import logging.config

    async def main():
        gd = GracefulDeath()
        with open("logging.yaml") as f:
            logging_config = yaml.safe_load(f)

        logging.config.dictConfig(logging_config)
        async with Itgs() as itgs:
            await execute(itgs, gd)

    asyncio.run(main())
