"""Temporary job that corrects the codecs and format parameters on content file exports"""
from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import asyncio
import logging

from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Iterates through content file exports which have the old codec style and queues
    a job to update them to the new codec style.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    jobs = await itgs.jobs()

    async def bounce():
        await jobs.enqueue("runners.find_and_fix_content_file_codecs")

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    last_uid: Optional[str] = None
    while True:
        response = await cursor.execute(
            """
            SELECT
                uid
            FROM content_file_exports
            WHERE 
                codecs="aac"
                AND (? IS NULL OR uid > ?)
            ORDER BY uid ASC
            LIMIT 100
            """,
            (last_uid, last_uid),
        )

        if not response.results:
            logging.info("No more content file exports to update")
            return

        for (row_uid,) in response.results:
            if gd.received_term_signal:
                logging.info("Received TERM signal, bouncing")
                await bounce()
                return

            last_uid = row_uid
            await jobs.enqueue(
                "runners.fix_content_file_codecs", content_file_export_uid=row_uid
            )


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.find_and_fix_content_file_codecs")

    asyncio.run(main())
