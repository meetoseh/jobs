"""Provides utility functions for working with transcript jwts"""

from typing import Any, Dict, Literal, Optional
from error_middleware import handle_error
from dataclasses import dataclass
from itgs import Itgs
import time
import jwt
import os


@dataclass
class SuccessfulAuthResult:
    transcript_uid: str
    """The UID of the transcript which they have access too"""

    claims: Optional[Dict[str, Any]]
    """The claims of the token, typically for debugging, if applicable for the token type"""


@dataclass
class AuthResult:
    result: Optional[SuccessfulAuthResult]
    """if the authorization was successful, the information verified"""

    error_type: Optional[Literal["not_set", "bad_format", "invalid"]]
    """if the authorization failed, why it failed"""

    @property
    def success(self) -> bool:
        """True if it succeeded, False otherwise"""
        return self.result is not None


async def auth_presigned(itgs: Itgs, authorization: Optional[str]) -> AuthResult:
    """Verifies that the authorization header is set and matches a bearer
    token which provides access to a particular transcript. In particular,
    the JWT should be signed with `OSEH_TRANSCRIPT_JWT_SECRET`, have the audience
    `oseh-transcript`, and have an iat and exp set and valid.

    Args:
        itgs (Itgs): The integrations to use to connect to networked services
        authorization (str, None): The authorization header provided

    Returns:
        AuthResult: The result of the authentication, which will include the
            suggested error response on failure and the authorized image files
            uid on success
    """
    if authorization is None:
        return AuthResult(result=None, error_type="not_set")

    if not authorization.startswith("bearer "):
        return AuthResult(result=None, error_type="bad_format")

    token = authorization[len("bearer ") :]
    secret = os.environ["OSEH_TRANSCRIPT_JWT_SECRET"]

    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            options={"require": ["sub", "iss", "exp", "aud", "iat"]},
            audience="oseh-transcript",
            issuer="oseh",
        )
    except Exception as e:
        if not isinstance(e, jwt.exceptions.ExpiredSignatureError):
            await handle_error(e, extra_info="failed to decode journey jwt")
        return AuthResult(result=None, error_type="invalid")

    return AuthResult(
        result=SuccessfulAuthResult(transcript_uid=claims["sub"], claims=claims),
        error_type=None,
    )


async def auth_any(itgs: Itgs, authorization: Optional[str]) -> AuthResult:
    """Verifies that the authorization matches one of the accepted authorization
    patterns for transcripts. This should be preferred over `auth_presigned` unless
    a JWT is required.

    Args:
        itgs (Itgs): The integrations to use to connect to networked services
        authorization (str, None): The authorization header provided

    Returns:
        AuthResult: The result of the authentication, which will include the
            suggested error response on failure and the authorized image files
            uid on success
    """
    return await auth_presigned(itgs, authorization)


async def create_jwt(itgs: Itgs, transcript_uid: str, duration: int = 1800) -> str:
    """Produces a JWT for the given transcript uid. The returned JWT will
    be acceptable for `auth_presigned`.

    Args:
        itgs (Itgs): The integrations to use to connect to networked services
        transcript_uid (str): The uid of the transcript to create a JWT for
        duration (int, optional): The duration of the JWT in seconds. Defaults to 1800.

    Returns:
        str: The JWT
    """
    now = int(time.time())

    return jwt.encode(
        {
            "sub": transcript_uid,
            "iss": "oseh",
            "aud": "oseh-transcript",
            "iat": now - 1,
            "exp": now + duration,
        },
        os.environ["OSEH_TRANSCRIPT_JWT_SECRET"],
        algorithm="HS256",
    )
