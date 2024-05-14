"""Deletes a content file that's not in use"""

import json
from typing import Dict
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
from content import get_content_file
import logging
import time
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, uid: str):
    """Deletes the content file with the given uid, including all of its exports
    and export parts, but only if the content file is not in use.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        uid (str): The UID of the content file to delete.
    """
    content_file = await get_content_file(itgs, uid, consistency_level="strong")
    if content_file is None:
        await handle_warning(
            f"{__name__}:does_not_exist",
            f"Content file with {uid=} does not exist",
        )
        return

    conn = await itgs.conn()
    cursor = conn.cursor("strong")

    response = await cursor.execute(
        """
        DELETE FROM content_files
        WHERE
            content_files.uid = ?
            AND NOT EXISTS (
                SELECT 1 FROM journeys
                WHERE journeys.audio_content_file_id = content_files.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM journeys
                WHERE journeys.sample_content_file_id = content_files.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM journeys
                WHERE journeys.video_content_file_id = content_files.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM courses
                WHERE courses.video_content_file_id = content_files.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM onboarding_videos
                WHERE onboarding_videos.video_content_file_id = content_files.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM client_flow_content_files
                WHERE client_flow_content_files.content_file_id = content_files.id
            )
        """,
        (uid,),
    )
    if response.rows_affected is None or response.rows_affected < 1:
        await handle_warning(
            f"{__name__}:in_use",
            f"Content file with {uid=} is in use",
        )
        return

    logging.info(
        f"Deleted content file with {uid=} from database, marking to clean up S3"
    )
    redis = await itgs.redis()

    mapping: Dict[str, float] = {}
    soon = time.time() + 60 * 10
    part_uids_to_purgatory_keys: Dict[str, str] = dict()
    for exp in content_file.exports:
        for part in exp.parts:
            purgatory_key = json.dumps(
                {
                    "key": part.s3_file.key,
                    "bucket": part.s3_file.bucket,
                    "hint": "jobs/runners/delete_content_file",
                    "expected": False,
                },
                sort_keys=True,
            )
            mapping[purgatory_key] = soon
            part_uids_to_purgatory_keys[part.uid] = purgatory_key

    await redis.zadd("files:purgatory", mapping=mapping)

    files = await itgs.files()
    for exp in content_file.exports:
        for part in exp.parts:
            if gd.received_term_signal:
                return

            await files.delete(bucket=part.s3_file.bucket, key=part.s3_file.key)
            await cursor.execute(
                "DELETE FROM s3_files WHERE s3_files.uid = ?",
                (part.s3_file.uid,),
            )
            await redis.zrem("files:purgatory", part_uids_to_purgatory_keys[part.uid])

    logging.info(f"Deleted content file with {uid=} from S3")
