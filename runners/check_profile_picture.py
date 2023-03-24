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
import time
import secrets
import json

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
    """Handles seeing an oauth2 profile claim for an identity of the user with
    the given sub issued at the given time.

    If we have already processed that profile picture, this does nothing. If the
    user has a profile picture from a better source, such as direct upload or a more
    recent jwt, this does nothing.

    If both of those are not true, this downloads the picture processes it and inserts
    it into user_profile_pictures, marking it latest.

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
            EXISTS (SELECT 1 FROM users WHERE sub=?) AS b1,
            EXISTS (
                SELECT 1 FROM user_profile_pictures
                WHERE 
                    EXISTS (
                        SELECT 1 FROM users
                        WHERE
                            users.id = user_profile_pictures.user_id
                            AND users.sub = ?
                    )
                    AND json_extract(user_profile_pictures.source, '$.src') = 'oauth2-token'
                    AND json_extract(user_profile_pictures.source, '$.url') = ?
            ) AS b2,
            EXISTS (
                SELECT 1 FROM user_profile_pictures
                WHERE
                    EXISTS (
                        SELECT 1 FROM users
                        WHERE
                            users.id = user_profile_pictures.user_id
                            AND users.sub = ?
                    )
                    AND json_extract(user_profile_pictures.source, '$.src') = 'oauth2-token'
                    AND json_extract(user_profile_pictures.source, '$.iat') > ?
            ) AS b3
        """,
        (user_sub, user_sub, picture_url, user_sub, jwt_iat),
    )
    user_exists = bool(response.results[0][0])
    picture_exists = bool(response.results[0][1])
    newer_picture_exists = bool(response.results[0][2])

    if not user_exists:
        await handle_warning(f"{__name__}:user_does_not_exist", f"{user_sub=}")
        return

    if picture_exists:
        logging.debug(
            f"{user_sub=} already has {picture_url=} as one of their profile pictures"
        )
        return

    if newer_picture_exists:
        logging.debug(
            f"{user_sub=} already has a newer profile picture than {picture_url=}, {jwt_iat=}"
        )
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

        now = time.time()
        new_upp_uid = f"oseh_upp_{secrets.token_urlsafe(16)}"
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
                                upp.image_file_id = image_files.id
                                AND upp.user_id = users.id
                        )
                    """,
                    (
                        new_upp_uid,
                        json.dumps(
                            {
                                "src": "oauth2-token",
                                "url": picture_url,
                                "iat": jwt_iat,
                            }
                        ),
                        now,
                        user_sub,
                        image.uid,
                    ),
                ),
                (
                    """
                    UPDATE user_profile_pictures
                    SET latest=0
                    WHERE
                        EXISTS (
                            SELECT 1 FROM users
                            WHERE
                                users.id = user_profile_pictures.user_id
                                AND users.sub = ?
                        )
                        AND latest=1
                        AND EXISTS (
                            SELECT 1 FROM user_profile_pictures AS upp
                            WHERE upp.uid = ?
                        )
                    """,
                    (user_sub, new_upp_uid),
                ),
                (
                    """
                    UPDATE user_profile_pictures
                    SET latest=1
                    WHERE uid=?
                    """,
                    (new_upp_uid,),
                ),
            )
        )

        if response[0].rows_affected is None or response[0].rows_affected < 1:
            # this is not dangerous, this just means the url changed but the image is the same
            logging.info(
                f"{user_sub=} still has profile picture {image.uid=}, though its now at {picture_url=}"
            )
            return

        if response[2].rows_affected is None or response[2].rows_affected < 1:
            await handle_warning(
                f"{__name__}:set_latest",
                f"Could not set latest for {user_sub=}, {new_upp_uid=}",
            )

        logging.info(
            f"Updated {user_sub=} profile picture to {image.uid=} ({picture_url=})"
        )
