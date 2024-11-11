"""Provides utility functions for working with image file jwts"""

from itgs import Itgs
import time
import jwt
import os


async def create_jwt(itgs: Itgs, image_file_uid: str, duration: int = 1800) -> str:
    """Produces a JWT for the given image file uid.

    Args:
        itgs (Itgs): The integrations to use to connect to networked services
        image_file_uid (str): The uid of the image file to create a JWT for
        duration (int, optional): The duration of the JWT in seconds. Defaults to 1800.

    Returns:
        str: The JWT
    """
    now = int(time.time())

    return jwt.encode(
        {
            "sub": image_file_uid,
            "iss": "oseh",
            "aud": "oseh-image",
            "iat": now - 1,
            "exp": now + duration,
        },
        os.environ["OSEH_IMAGE_FILE_JWT_SECRET"],
        algorithm="HS256",
    )
