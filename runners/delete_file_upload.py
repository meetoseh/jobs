"""a generic handler for when an s3 file upload should be aborted"""
import asyncio
from itgs import Itgs
from graceful_death import GracefulDeath


async def execute(itgs: Itgs, gd: GracefulDeath, *, file_upload_uid: str):
    """Cleans up the file upload with the given uid. This can be used as a
    failure job, and is typically queued up from a success job once it has been
    stitched and processed.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): the uid of the aborted file upload
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_failed_upload", file_upload_uid=file_upload_uid
        )

    conn = await itgs.conn()
    cursor = conn.cursor("strong")
    files = await itgs.files()
    while True:
        if gd.received_term_signal:
            return await bounce()

        response = await cursor.execute(
            """
            SELECT
                s3_files.key
            FROM s3_files
            WHERE
                EXISTS (
                    SELECT 1 FROM s3_file_upload_parts
                    WHERE s3_file_upload_parts.s3_file_id = s3_files.id
                      AND EXISTS (
                        SELECT 1 FROM s3_file_uploads
                        WHERE s3_file_uploads.id = s3_file_upload_parts.s3_file_upload_id
                          AND s3_file_uploads.uid = ?
                      )
                )
            ORDER BY s3_files.uid ASC
            LIMIT 10
            """,
            (file_upload_uid,),
        )

        if not response.results:
            await cursor.execute(
                "DELETE FROM s3_file_uploads WHERE uid=?", (file_upload_uid,)
            )
            return

        keys_to_delete = [row[0] for row in response.results]
        await asyncio.wait(
            [
                files.delete(bucket=files.default_bucket, key=key)
                for key in keys_to_delete
            ]
        )
        await cursor.execute(
            "DELETE FROM s3_files WHERE key IN ({})".format(
                ",".join("?" for _ in keys_to_delete)
            ),
            keys_to_delete,
        )
