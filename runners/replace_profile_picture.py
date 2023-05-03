import json
import secrets
import time
from images import ProcessImageAbortedException, process_image
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from runners.check_profile_picture import TARGETS
from temp_files import temp_file

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, user_sub: str, s3_key: str):
    """Uploads a new profile picture for the user with the given sub, if they
    do not already have a matching profile picture. this is generally intended
    to be executed manually, but it could be slightly modified to accept an
    uploaded file once there is an actual flow to upload a profile picture.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): The sub of the user to upload the profile picture for
        s3_key (str): The S3 key of the profile picture to upload
    """

    async def _bounce():
        logging.debug("Bouncing replace profile picture job")
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.replace_profile_picture", user_sub=user_sub, s3_key=s3_key
        )

    with temp_file() as src_path:
        files = await itgs.files()

        with open(src_path, "wb") as f:
            await files.download(f, bucket=files.default_bucket, key=s3_key, sync=True)

        if gd.received_term_signal:
            await _bounce()
            return

        try:
            image = await process_image(
                src_path,
                TARGETS,
                itgs=itgs,
                gd=gd,
                max_width=4096,
                max_height=4096,
                max_area=2048 * 2048,
                max_file_size=1024 * 1024 * 50,
                name_hint="admin-profile_picture",
            )
        except ProcessImageAbortedException:
            await _bounce()
            return

        logging.info(
            f"Processed {s3_key=} into the image {image.uid=}, assigning profile picture.."
        )

        conn = await itgs.conn()
        cursor = conn.cursor()

        upp_uid = f"oseh_upp_{secrets.token_urlsafe(16)}"
        now = time.time()
        response = await cursor.executemany3(
            (
                (
                    """
                    INSERT INTO user_profile_pictures (
                        uid, user_id, latest, image_file_id, source, created_at
                    )
                    SELECT
                        ?, users.id, 0, image_files.id, ?, ?
                    FROM users, image_files
                    WHERE
                        users.sub = ?
                        AND image_files.uid = ?
                        AND NOT EXISTS (
                            SELECT 1 FROM user_profile_pictures AS upp
                            WHERE 
                                upp.user_id = users.id
                                AND upp.image_file_id = image_files.id
                        )
                    """,
                    (
                        upp_uid,
                        json.dumps(
                            {"src": "admin", "admin_user_sub": None, "uploaded_at": now}
                        ),
                        now,
                        user_sub,
                        image.uid,
                    ),
                ),
                (
                    """
                    UPDATE user_profile_pictures
                    SET latest = 0
                    WHERE
                        EXISTS (
                            SELECT 1 FROM users
                            WHERE users.id = user_profile_pictures.user_id
                                AND users.sub = ?
                        )
                        AND EXISTS (
                            SELECT 1 FROM user_profile_pictures
                            WHERE user_profile_pictures.uid = ?
                        )
                    """,
                    (user_sub, upp_uid),
                ),
                (
                    """
                    UPDATE user_profile_pictures
                    SET latest = 1
                    WHERE uid = ?
                    """,
                    (upp_uid,),
                ),
            )
        )

        if response[0].rows_affected is None or response[0].rows_affected < 1:
            logging.info(
                f"User {user_sub=} either does not exist or already had a matching profile picture"
            )
        else:
            logging.info(
                f"User {user_sub}'s profile picture was updated to {image.uid} via {upp_uid} from {s3_key}"
            )


if __name__ == "__main__":
    import asyncio

    async def main():
        user_sub = input("User sub: ")
        s3_key = input("S3 key: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.replace_profile_picture", user_sub=user_sub, s3_key=s3_key
            )

    asyncio.run(main())
