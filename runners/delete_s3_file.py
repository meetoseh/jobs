"""Deletes a single s3 file. Usually used as part of a larger task,
e.g., deleting a user.
"""

from typing import Optional, cast
from itgs import Itgs
from graceful_death import GracefulDeath
from error_middleware import handle_warning

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    uid: Optional[str] = None,
    key: Optional[str] = None,
):
    """Deletes an s3 file indicated either by uid or key.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        uid (str, None): the uid of the s3 file to delete, or None if the key is used instead
        key (str, None): the key of the s3 file to delete, or None if the uid is used instead
    """
    if uid is None and key is None:
        await handle_warning(
            f"{__name__}:no_identifier", "Ignoring job with no uid or key"
        )
        return

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    if key is None:
        response = await cursor.execute("SELECT key FROM s3_files WHERE uid=?", (uid,))
        if not response.results:
            await handle_warning(
                f"{__name__}:no_such_uid", f"No s3 file with uid {uid}"
            )
            return
        key = cast(str, response.results[0][0])

    files = await itgs.files()
    await files.delete(bucket=files.default_bucket, key=key)
    await cursor.execute("DELETE FROM s3_files WHERE key=?", (key,))
