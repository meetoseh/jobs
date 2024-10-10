"""contains convenient functions for authorizing voice note access"""

from dataclasses import dataclass
import secrets
import time
from typing import Any, Dict, Literal, Optional
from error_middleware import handle_error
from itgs import Itgs
import jwt
import os


@dataclass
class SuccessfulAuthResult:
    voice_note_uid: str
    """the voice note uid for which access was authorized"""

    claims: Optional[Dict[str, Any]] = None
    """If the token was a JWT, this will contain the claims of the token"""


@dataclass
class AuthResult:
    result: Optional[SuccessfulAuthResult]
    """if the authorization was successful, the information of the user"""

    error_type: Optional[Literal["not_set", "bad_format", "invalid"]]
    """if the authorization failed, why it failed"""

    @property
    def success(self) -> bool:
        """True if it succeeded, False otherwise"""
        return self.result is not None


async def auth_presigned(
    itgs: Itgs, authorization: Optional[str], *, prefix: str = "bearer "
) -> AuthResult:
    """Verifies the given authorization token uses the given prefix and represents
    a valid JWT for accessing a voice note.

    Args:
        itgs (Itgs): the integrations to use
        authorization (str, None): the provided authorization header
    """
    if authorization is None:
        return AuthResult(None, error_type="not_set")
    if not authorization.startswith(prefix):
        return AuthResult(None, error_type="bad_format")

    token = authorization[len(prefix) :]

    try:
        payload = jwt.decode(
            token,
            key=os.environ["OSEH_VOICE_NOTE_JWT_SECRET"],
            algorithms=["HS256"],
            options={"require": ["sub", "iss", "aud", "exp", "iat", "jti"]},
            audience="oseh-voice-note",
            issuer="oseh",
        )
    except Exception as e:
        if not isinstance(e, jwt.exceptions.ExpiredSignatureError):
            await handle_error(e, extra_info="Failed to decode voice note token")
        return AuthResult(None, error_type="invalid")

    return AuthResult(
        result=SuccessfulAuthResult(voice_note_uid=payload["sub"], claims=payload),
        error_type=None,
    )


async def create_jwt(
    itgs: Itgs,
    /,
    *,
    voice_note_uid: str,
    duration: int = 1800,
) -> str:
    """Produces a JWT for the given voice note. The returned JWT will be acceptable for
    `auth_presigned` with no prefix.

    Args:
        itgs (Itgs): The integrations to use to connect to networked services
        voice_note_uid (str): the uid of the voice note to authorize
        duration (int, optional): The duration of the JWT in seconds. Defaults to 1800.

    Returns:
        str: The JWT
    """
    now = int(time.time())

    return jwt.encode(
        {
            "sub": voice_note_uid,
            "iss": "oseh",
            "aud": "oseh-voice-note",
            "iat": now - 1,
            "exp": now + duration,
            "jti": secrets.token_urlsafe(4),
        },
        os.environ["OSEH_VOICE_NOTE_JWT_SECRET"],
        algorithm="HS256",
    )
