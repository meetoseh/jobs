"""Checks if a given users profile picture matches the given url, and if it does not,
downloads the image, modifies it appropriately, and updates our database
"""
from typing import Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import aiohttp
from temp_files import temp_file
from images import process_image, ImageTarget, ProcessImageAbortedException
import logging
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


TARGETS = [
    ImageTarget(
        required=True,
        width=38,  # journey screen on web
        height=38,
        format="png",
        quality_settings={"optimize": True},
    ),
    ImageTarget(
        required=False,
        width=38,
        height=38,
        format="webp",
        quality_settings={"lossless": True, "quality": 100, "method": 6},
    ),
    ImageTarget(
        required=True,
        width=45,  # journey screen on web, in the chat area
        height=45,
        format="png",
        quality_settings={"optimize": True},
    ),
    ImageTarget(
        required=False,
        width=45,
        height=45,
        format="webp",
        quality_settings={"lossless": True, "quality": 100, "method": 6},
    ),
    ImageTarget(
        required=True,
        width=60,
        height=60,
        format="png",
        quality_settings={"optimize": True},
    ),
    ImageTarget(
        required=False,
        width=60,
        height=60,
        format="webp",
        quality_settings={"lossless": True, "quality": 100, "method": 6},
    ),
    ImageTarget(
        required=True,
        width=76,  # journey screen on web, 2x
        height=76,
        format="png",
        quality_settings={"optimize": True},
    ),
    ImageTarget(
        required=False,
        width=76,
        height=76,
        format="webp",
        quality_settings={"lossless": True, "quality": 100, "method": 6},
    ),
    ImageTarget(
        required=True,
        width=90,  # journey screen on web, in the chat area, 2x
        height=90,
        format="png",
        quality_settings={"optimize": True},
    ),
    ImageTarget(
        required=False,
        width=90,
        height=90,
        format="webp",
        quality_settings={"lossless": True, "quality": 100, "method": 6},
    ),
    ImageTarget(
        required=False,
        width=120,
        height=120,
        format="webp",
        quality_settings={"lossless": True, "quality": 100, "method": 6},
    ),
    ImageTarget(
        required=False,
        width=120,
        height=120,
        format="png",
        quality_settings={"optimize": True},
    ),
    ImageTarget(
        required=False,
        width=256,
        height=256,
        format="webp",
        quality_settings={"lossless": False, "quality": 95, "method": 4},
    ),
    ImageTarget(
        required=False,
        width=512,
        height=512,
        format="webp",
        quality_settings={"lossless": False, "quality": 95, "method": 4},
    ),
]

LAST_TARGETS_CHANGED_AT = 1672953669


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, user_sub: str, picture_url: str, jwt_iat: float
):
    """If the given user exists, but its profile picture is either not set or does
    not match the given url, download the image, modify it appropriately, and update
    our database.

    When a user updates their profile picture, it will cause all new JWTs issued
    by cognito to have the updated URL, however, old JWTs will still have the old URL.
    Hence we ignore the picture url if it's from a JWT older than the one we used to
    get the users current profile picture.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): the user to check
        picture_url (str): the url of the profile picture to check against
        jwt_iat (float): the iat of the JWT that was used to get the picture
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            users.picture_url,
            image_files.uid,
            users.picture_image_file_updated_at
        FROM users
        LEFT OUTER JOIN image_files ON image_files.id = users.picture_image_file_id
        WHERE
            users.sub = ?
        """,
        (user_sub,),
    )
    if not response.results:
        await handle_warning(f"{__name__}:user_not_found", f"{user_sub=} not found")
        return

    old_picture_url: Optional[str] = response.results[0][0]
    old_picture_image_file_uid: Optional[str] = response.results[0][1]
    old_picture_image_file_updated_at: Optional[float] = response.results[0][2]

    if (
        old_picture_url == picture_url
        and old_picture_image_file_updated_at is not None
        and old_picture_image_file_updated_at >= LAST_TARGETS_CHANGED_AT
    ):
        return

    if (
        old_picture_image_file_updated_at
        and old_picture_image_file_updated_at > jwt_iat
    ):
        return

    jobs = await itgs.jobs()

    async def _bounce():
        jobs.enqueue(
            "runners.check_profile_picture",
            user_sub=user_sub,
            picture_url=picture_url,
            jwt_iat=jwt_iat,
        )

    with temp_file() as tmp_file_loc:
        async with aiohttp.ClientSession() as session:
            async with session.get(picture_url) as resp:
                if not resp.ok:
                    await handle_warning(
                        f"{__name__}:bad_picture_url",
                        f"{picture_url=} returned {resp.status}",
                    )
                    return

                with open(tmp_file_loc, "wb") as f:
                    while True:
                        chunk = await resp.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)

        try:
            image = await process_image(
                tmp_file_loc,
                TARGETS,
                itgs=itgs,
                gd=gd,
                max_width=4096,
                max_height=4096,
                max_area=2048 * 2048,
                max_file_size=1024 * 1024 * 50,
                name_hint="profile_picture",
            )
        except ProcessImageAbortedException:
            await _bounce()
            return

        if image.uid == old_picture_image_file_uid:
            return

        response = await cursor.execute(
            """
            UPDATE users
            SET
                picture_url=?,
                picture_image_file_id=image_files.id,
                picture_image_file_updated_at=?
            FROM image_files
            WHERE
                users.sub = ?
                AND image_files.uid = ?
                AND (
                    users.picture_image_file_updated_at IS NULL
                    OR users.picture_image_file_updated_at < ?
                )
                AND (? IS NULL OR EXISTS (
                    SELECT 1 FROM image_files AS old_image_files
                    WHERE
                        old_image_files.id = users.picture_image_file_id
                        AND old_image_files.uid = ?
                ))
            """,
            (
                picture_url,
                jwt_iat,
                user_sub,
                image.uid,
                jwt_iat,
                old_picture_image_file_uid,
                old_picture_image_file_uid,
            ),
        )

        if response.rows_affected is None or response.rows_affected < 1:
            await handle_warning(
                f"{__name__}:update_failed",
                (
                    f"Failed to update {user_sub=} (changed during processing)\n\n"
                    "```\n"
                    f"{old_picture_url=}\n"
                    f"{old_picture_image_file_uid=}\n"
                    f"{old_picture_image_file_updated_at=}\n"
                    f"{picture_url=}\n"
                    f"{jwt_iat=}\n"
                    f"{user_sub=}\n"
                    f"{image.uid=}\n"
                    "```\n"
                ),
            )
            await jobs.enqueue("runners.delete_image_file", uid=image.uid)
            return

        if old_picture_image_file_uid is not None:
            await jobs.enqueue(
                "runners.delete_image_file", uid=old_picture_image_file_uid
            )

        logging.info("Updated %s's profile picture to %s", user_sub, picture_url)
