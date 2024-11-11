"""Queues a job to create missing journey share images"""

from itgs import Itgs
from graceful_death import GracefulDeath

from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, force: bool = False):
    """Looks for journeys missing a share image file id and queues a job to
    initialize the share image for each one.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        force (bool): if true, we process all journeys, not just those missing
            a share image file id
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue("runners.process_missing_journey_share_images")

    conn = await itgs.conn()
    cursor = conn.cursor("none")
    last_uid = None
    jobs = await itgs.jobs()

    sql_filter = "AND journeys.share_image_file_id IS NULL" if not force else ""

    while True:
        if gd.received_term_signal:
            await bounce()
            return

        response = await cursor.execute(
            f"""
            SELECT journeys.uid FROM journeys
            WHERE
                (? IS NULL OR journeys.uid > ?)
                {sql_filter}
            ORDER BY journeys.uid ASC
            LIMIT 100
            """,
            (last_uid, last_uid),
        )

        if not response.results:
            break

        for (journey_uid,) in response.results:
            last_uid = journey_uid
            await jobs.enqueue(
                "runners.process_journey_share_image", journey_uid=journey_uid
            )

            if gd.received_term_signal:
                await bounce()
                return


if __name__ == "__main__":
    import asyncio

    async def main():
        force = input("Force? (y/n) ").lower() == "y"
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.process_missing_journey_share_images", force=force
            )

    asyncio.run(main())
