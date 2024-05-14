"""Deletes the image file with the given uid"""

from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
from error_middleware import handle_warning
from images import get_image_file
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    uid: str,
    job_uid: Optional[str] = None,
    force: bool = False,
):
    """Deletes the image file with the given uid, but only if it's not in use

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        uid (str): The uid of the image file to delete
        job_uid (str): If specified, we send a message to ps:job:{job_uid} when done
        force (bool): skips in-use checks if True
    """

    async def report_done():
        if job_uid is not None:
            redis = await itgs.redis()
            await redis.publish(f"ps:job:{job_uid}", "done")

    image_file = await get_image_file(itgs, uid)
    if image_file is None:
        await handle_warning(f"{__name__}:image_file_not_found", f"{uid=} not found")
        await report_done()
        return

    conn = await itgs.conn()
    cursor = conn.cursor()

    if not force:
        response = await cursor.execute(
            """
            DELETE FROM image_files
            WHERE
                image_files.uid = ?
                AND NOT EXISTS (
                    SELECT 1 FROM user_profile_pictures
                    WHERE user_profile_pictures.image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM journeys
                    WHERE journeys.background_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM journeys
                    WHERE journeys.blurred_background_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM journeys
                    WHERE journeys.darkened_background_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM journeys
                    WHERE journeys.share_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM instructors
                    WHERE instructors.picture_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM static_public_images
                    WHERE static_public_images.image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM vip_chat_requests
                    WHERE
                        vip_chat_requests.variant = 'phone-04102023'
                        AND (
                            json_extract(vip_chat_requests.display_data, '$.image_uid') = image_files.uid
                            OR json_extract(vip_chat_requests.display_data, '$.background_image_uid') = image_files.uid
                        )
                )
                AND NOT EXISTS (
                    SELECT 1 FROM courses
                    WHERE
                        courses.background_original_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM courses
                    WHERE
                        courses.background_darkened_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM courses
                    WHERE
                        courses.video_thumbnail_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM courses
                    WHERE
                        courses.logo_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM courses
                    WHERE
                        courses.hero_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM courses
                    WHERE
                        courses.share_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM journey_pinterest_pins
                    WHERE
                        journey_pinterest_pins.image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM onboarding_videos
                    WHERE
                        onboarding_videos.thumbnail_image_file_id = image_files.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM client_flow_images
                    WHERE
                        client_flow_images.image_file_id = image_files.id
                )
            """,
            (uid,),
        )
    else:
        response = await cursor.execute(
            """
            DELETE FROM image_files
            WHERE
                image_files.uid = ?
            """,
            (uid,),
        )

    if response.rows_affected is None or response.rows_affected < 1:
        await handle_warning(f"{__name__}:image_file_in_use", f"{uid=} is in use")
        await report_done()
        return

    files = await itgs.files()
    s3_files_to_delete = [export.s3_file for export in image_file.exports]

    if image_file.original_s3_file is not None:
        s3_files_to_delete.append(image_file.original_s3_file)

    for s3_file in s3_files_to_delete:
        await files.delete(bucket=s3_file.bucket, key=s3_file.key)
        await cursor.execute("DELETE FROM s3_files WHERE uid=?", (s3_file.uid,))

    await report_done()


if __name__ == "__main__":
    import asyncio

    async def main():
        uid = input("uid:")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.delete_image_file", uid=uid)

    asyncio.run(main())
