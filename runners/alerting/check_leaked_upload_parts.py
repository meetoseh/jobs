"""Checks for leaked file upload parts, to detect a regression"""

from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Verifies there aren't any file upload parts that have no corresponding
    file upload, to test for a regression. Initially, the bug was caused by using
    row ids instead of uids.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        "SELECT COUNT(*) FROM s3_file_upload_parts "
        "WHERE"
        " NOT EXISTS ("
        "  SELECT 1 FROM s3_file_uploads"
        "  WHERE s3_file_uploads.id = s3_file_upload_parts.s3_file_upload_id"
        " )"
    )
    assert response.results
    assert (
        response.results[0][0] == 0
    ), f"Leaked file upload parts: {response.results[0][0]}"


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.alerting.check_leaked_upload_parts")

    asyncio.run(main())
