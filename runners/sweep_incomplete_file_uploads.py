"""Checks for any s3_file_uploads past their expires_at"""

import json
from typing import Optional, cast
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import time

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 10
"""The maximum duration in seconds this job can run before stopping itself to allow
other jobs to run
"""

BATCH_SIZE = 10
"""The maximum number of s3_file_upload expirations to process per iteration"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Checks for incomplete file uploads and triggers the failure job on them

    PERF: currently assumes relatively low throughput

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at_wall = time.time()
    started_at_perf = time.perf_counter()
    conn = await itgs.conn()
    cursor = conn.cursor("none")
    last_uid: Optional[str] = None

    while True:
        if time.perf_counter() - started_at_perf > MAX_JOB_TIME_SECONDS:
            await handle_warning(
                f"{__name__}:job_took_too_long",
                f"Job took longer than {MAX_JOB_TIME_SECONDS} seconds, ending early!",
            )
            break

        response = await cursor.execute(
            """
            SELECT
                uid,
                failure_job_name,
                failure_job_kwargs,
                job_progress_uid
            FROM s3_file_uploads
            WHERE
                expires_at < ?
                AND (? IS NULL OR uid > ?)
            ORDER BY uid
            LIMIT ?
            """,
            (
                started_at_wall,
                last_uid,
                last_uid,
                BATCH_SIZE,
            ),
        )

        if not response.results:
            logging.info("found no remaining incomplete file uploads")
            break

        for row in response.results:
            row_uid = cast(str, row[0])
            row_failure_job_name = cast(str, row[1])
            row_failure_job_kwargs_raw = cast(str, row[2])
            row_job_progress_uid = cast(Optional[str], row[3])

            last_uid = row_uid
            row_failure_job_kwargs = json.loads(row_failure_job_kwargs_raw)
            assert isinstance(row_failure_job_kwargs, dict), row_uid

            jobs = await itgs.jobs()
            if row_job_progress_uid is not None:
                await jobs.push_progress(
                    row_job_progress_uid,
                    {
                        "type": "failed",
                        "message": "failure job queued due to expiration",
                        "indicator": None,
                        "occurred_at": time.time(),
                    },
                )
            await jobs.enqueue(row_failure_job_name, **row_failure_job_kwargs)


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sweep_incomplete_file_uploads")

    asyncio.run(main())
