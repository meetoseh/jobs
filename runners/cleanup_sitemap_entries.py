"""Cleans up rows in sitemap_entries that are no longer in the sitemap"""

import json
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import logging
import os
import aiohttp
import io
import time
import socket

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Removes entries in sitemap_entries that are at least 1 day old and no
    longer in the sitemap returned from /sitemap.txt

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    root_frontend_url = os.environ["ROOT_FRONTEND_URL"]
    url = root_frontend_url + "/sitemap.txt"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            sitemap_txt_bytes = await response.read()

    sitemap_txt = sitemap_txt_bytes.decode("utf-8")
    sitemap_entries = [v for v in sitemap_txt.split("\n") if v]
    if not sitemap_entries:
        logging.warning(
            "Found no sitemap entries in sitemap.txt, not cleaning sitemap_entries"
        )
        return

    valid_paths = [
        v[len(root_frontend_url) :]
        for v in sitemap_entries
        if v.startswith(root_frontend_url)
    ]
    if len(valid_paths) != len(sitemap_entries):
        bad_prefixes = [
            v for v in sitemap_entries if not v.startswith(root_frontend_url)
        ]
        await handle_warning(
            f"{__name__}:bad_prefixes",
            f"Found {len(bad_prefixes)} bad prefixes in sitemap.txt, first 6:\n```\n{json.dumps(bad_prefixes[6:], indent=2)}\n```",
        )
        return

    logging.debug(f"Found {len(valid_paths)} entries in sitemap.txt, pruning...")

    sql = io.StringIO()
    qargs = []

    sql.write("WITH batch(path) AS (VALUES (?)")
    qargs.append(valid_paths[0])
    for idx in range(1, len(valid_paths)):
        sql.write(", (?)")
        qargs.append(valid_paths[idx])
    sql.write(
        ") DELETE FROM sitemap_entries WHERE path NOT IN batch AND created_at < ?"
    )
    qargs.append(time.time() - 60 * 60 * 24)

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(sql.getvalue(), qargs)
    num_deleted = response.rows_affected or 0
    logging.debug(f"Deleted {num_deleted} rows from sitemap_entries")

    if num_deleted > 0:
        slack = await itgs.slack()
        await slack.send_ops_message(
            f"{socket.gethostname()} Cleaned up {num_deleted} rows from sitemap_entries"
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.cleanup_sitemap_entries")

    asyncio.run(main())
