from typing import Any, Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT = """
local voice_note_uid = ARGV[1]
local now_str = ARGV[2]

local voice_note_key = "voice_notes:processing:" .. voice_note_uid
local old_upload_success_job_at = redis.call("HGET", voice_note_key, "upload_success_job_at")
local old_stitched_s3_key = redis.call("HGET", voice_note_key, "stitched_s3_key")

if old_stitched_s3_key ~= 'not_yet' then
    if old_stitched_s3_key == false then return -1 end
    return -2
end

redis.call("HSET", voice_note_key, "upload_success_job_at", now_str)
if old_upload_success_job_at == "not_yet" then return 1 end
return 2
"""

VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT_HASH = hashlib.sha1(
    VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_voice_notes_upload_success_started_ensured_at: Optional[float] = None


async def ensure_voice_notes_upload_success_started_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the voice_notes_upload_success_started lua script is loaded into redis."""
    global _last_voice_notes_upload_success_started_ensured_at

    now = time.time()
    if (
        not force
        and _last_voice_notes_upload_success_started_ensured_at is not None
        and (now - _last_voice_notes_upload_success_started_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(
            VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT
        )
        assert (
            correct_hash == VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT_HASH=}"

    if (
        _last_voice_notes_upload_success_started_ensured_at is None
        or _last_voice_notes_upload_success_started_ensured_at < now
    ):
        _last_voice_notes_upload_success_started_ensured_at = now


@dataclass
class VoiceNoteUploadSuccessStartedResultSuccess:
    type: Literal["success"]
    """
    - `success`: the voice note was found in `voice_notes:processing:{uid}`
      and was consistent with the upload success job not having run yet.
      We set `upload_success_job_at`.
    """


@dataclass
class VoiceNoteUploadSuccessStartedResultReplaced:
    type: Literal["replaced"]
    """
    - `replaced`: the voice note was found in `voice_notes:processing:{uid}`,
      though it looks like this is a retry for the upload success job. A
      warning should be emitted to slack, but we can continue
    """


@dataclass
class VoiceNoteUploadSuccessStartedResultNotFound:
    type: Literal["not_found"]
    """
    - `not_found`: the voice note was not found in `voice_notes:processing:{uid}`
    """


@dataclass
class VoiceNoteUploadSuccessStartedResultBadState:
    type: Literal["bad_state"]
    """
    - `bad_state`: the voice note is not in an acceptable state for the upload
      success job, because it appears it already succeeded. Warn slack and do
      not proceed
    """


VoiceNoteUploadSuccessStartedResult = Union[
    VoiceNoteUploadSuccessStartedResultSuccess,
    VoiceNoteUploadSuccessStartedResultReplaced,
    VoiceNoteUploadSuccessStartedResultNotFound,
    VoiceNoteUploadSuccessStartedResultBadState,
]


async def voice_notes_upload_success_started(
    redis: redis.asyncio.client.Redis, voice_note_uid: bytes, now: int
) -> Optional[VoiceNoteUploadSuccessStartedResult]:
    """Updates the value in the given key to the given value iff
    the key is unset or the value is lower than the current value.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        voice_note_uid (str): The voice note uid to update
        now (int): The current time

    Returns:
        VoiceNoteUploadSuccessStartedResult, None: The result. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        VOICE_NOTES_UPLOAD_SUCCESS_STARTED_LUA_SCRIPT_HASH,
        0,
        voice_note_uid,  # type: ignore
        str(now).encode("ascii"),  # type: ignore
    )
    if res is redis:
        return None
    return parse_voice_notes_upload_success_result(res)


async def safe_voice_notes_upload_success_started(
    itgs: Itgs, /, *, voice_note_uid: bytes, now: int
) -> VoiceNoteUploadSuccessStartedResult:
    """Like voice_notes_upload_success_started, but runs in the main redis
    instance and thus can take care of ensuring the script exists and doesn't
    need to worry about an optional result.
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_voice_notes_upload_success_started_script_exists(
            redis, force=force
        )

    async def _execute():
        return await voice_notes_upload_success_started(redis, voice_note_uid, now)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_voice_notes_upload_success_result(
    result: Any,
) -> VoiceNoteUploadSuccessStartedResult:
    """Parses the result of the voice notes upload success script into a
    more useful format.
    """
    if result == 1:
        return VoiceNoteUploadSuccessStartedResultSuccess(type="success")
    if result == 2:
        return VoiceNoteUploadSuccessStartedResultReplaced(type="replaced")
    if result == -1:
        return VoiceNoteUploadSuccessStartedResultNotFound(type="not_found")
    if result == -2:
        return VoiceNoteUploadSuccessStartedResultBadState(type="bad_state")
    raise ValueError(f"Unknown result: {result}")
