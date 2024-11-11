"""RQLite database backups"""

from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import time
from temp_files import temp_file
from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, hint: Optional[str] = None):
    """Creates a backup of the rqlite database and stores it in s3.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        hint (str, None): If specified, included at the end of the file name. Should be
            a short string of lowercase letters and underscores.
    """
    if hint is None:
        hint = ""

    conn = await itgs.conn()

    logging.info("Starting database backup...")

    started_at = time.time()
    backup_key = f"s3_files/backup/database/{int(started_at)}{hint}.bak"
    with temp_file() as backup_file:
        with open(backup_file, "wb") as f:
            await conn.backup(f)

        backup_created_at = time.time()
        logging.info(
            f"Database backup created in {backup_created_at - started_at:.3f} seconds"
        )

        files = await itgs.files()
        with open(backup_file, "rb") as f:
            await files.upload(
                f, bucket=files.default_bucket, key=backup_key, sync=True
            )

        backup_uploaded_at = time.time()
        logging.info(
            f"Database backup uploaded in {backup_uploaded_at - backup_created_at:.3f} seconds"
        )

    logging.info(
        f"Database backup completed in {backup_uploaded_at - started_at:.3f} seconds -> {backup_key}"
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.backup_database", hint="manual")

    asyncio.run(main())
