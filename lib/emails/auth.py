"""Provides utility functions for working with email template jwts. We never
have to accept these, but we do issue them.
"""

from itgs import Itgs
import time
import jwt
import os
import secrets


async def create_jwt(itgs: Itgs, template_slug: str, duration: int = 1800) -> str:
    """Produces a JWT for the email template with the given slug, lasting the given
    duration in seconds.

    Args:
        itgs (Itgs): The integrations to use to connect to networked services
        template_slug (str): The slug of the email template to produce a JWT for
        duration (int, optional): The duration of the JWT in seconds. Defaults to 1800.

    Returns:
        str: The JWT
    """
    now = int(time.time())

    return jwt.encode(
        {
            "sub": template_slug,
            "iss": "oseh",
            "aud": "oseh-email-templates",
            "iat": now - 1,
            "exp": now + duration,
            "jti": secrets.token_urlsafe(8),
        },
        os.environ["OSEH_EMAIL_TEMPLATE_JWT_SECRET"],
        algorithm="HS256",
    )
