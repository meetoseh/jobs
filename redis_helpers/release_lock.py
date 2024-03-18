import asyncio
import random
from typing import Any, Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep
from redis.exceptions import ConnectionError

RELEASE_LOCK_LUA_SCRIPT = """
local key = KEYS[1]
local lock_id = ARGV[1]
local expire_flag = ARGV[2] == "EXPIRE"

local current_value = redis.call("GET", key)
if current_value == false then
    return {-3, false}
end

local success, lock = pcall(function() return cjson.decode(current_value) end)
if
    not success
    or type(lock) ~= 'table'
    or type(lock.lock_id) ~= 'string'
then
    return {-2, current_value}
end

if lock.lock_id ~= lock_id then
    return {-1, current_value}
end

if expire_flag then
    redis.call("EXPIRE", key, 60, "NX")
    redis.call("EXPIRE", key, 60, "LT")
else
    redis.call("DEL", key)
end

return {1, false}
"""

RELEASE_LOCK_LUA_SCRIPT_HASH = hashlib.sha1(
    RELEASE_LOCK_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_release_lock_ensured_at: Optional[float] = None


async def ensure_release_lock_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the release_lock lua script is loaded into redis."""
    global _last_release_lock_ensured_at

    now = time.time()
    if (
        not force
        and _last_release_lock_ensured_at is not None
        and (now - _last_release_lock_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(RELEASE_LOCK_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(RELEASE_LOCK_LUA_SCRIPT)
        assert (
            correct_hash == RELEASE_LOCK_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {RELEASE_LOCK_LUA_SCRIPT_HASH=}"

    if _last_release_lock_ensured_at is None or _last_release_lock_ensured_at < now:
        _last_release_lock_ensured_at = now


@dataclass
class ReleaseLockSuccessResult:
    success: Literal[True]


@dataclass
class ReleaseLockNotSetResult:
    success: Literal[False]
    error_type: Literal["not_set"]
    """Could not release the lock because the key was unset"""


@dataclass
class ReleaseLockMalformedResult:
    success: Literal[False]
    error_type: Literal["malformed"]
    """Indicates that the current value at that key is not a valid lock."""
    current_value: bytes


@dataclass
class ReleaseLockStolenResult:
    success: Literal[False]
    error_type: Literal["stolen"]
    """Indicates that the lock is set, but not with the same lock_id"""
    current_value: bytes


ReleaseLockResult = Union[
    ReleaseLockSuccessResult,
    ReleaseLockNotSetResult,
    ReleaseLockMalformedResult,
    ReleaseLockStolenResult,
]


async def release_lock(
    redis: redis.asyncio.client.Redis,
    key: bytes,
    lock_id: bytes,
    *,
    expire: bool = False,
) -> Optional[ReleaseLockResult]:
    """Releases the lock at the given key, acquired as if by acquire_lock, but
    only if the lock_id matches the current lock_id.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (bytes): The key to release the lock on
        lock_id (bytes): The lock_id to release the lock with
        expire (bool): If true, instead of releasing the lock immediately on a match,
            set an expiration time of 60 seconds (if lower than the current expiration).
            This can be used if e.g. a SIGKILL is potentially incoming, to speed up
            recovery.

    Returns:
        ReleaseLockResult, None: The result of releasing the lock, if executed
            outside a pipeline, otherwise None since the result cannot be known
            until the pipeline is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        RELEASE_LOCK_LUA_SCRIPT_HASH,
        1,
        key,  # type: ignore
        lock_id,  # type: ignore
        *([b"EXPIRE"] if expire else []),  # type: ignore
    )  # type: ignore
    if res is redis:
        return None
    return parse_release_lock_result(res)


async def release_lock_safe(
    itgs: Itgs,
    key: bytes,
    lock_id: bytes,
    *,
    expire: bool = False,
) -> ReleaseLockResult:
    """Same as release_lock, but always run in the main redis instance and
    ensures the script is loaded first.

    Since we know this operation is idempotent, this will retry ConnectionErrors
    several times rather than raising them immediately.
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_release_lock_script_exists(redis, force=force)

    async def _execute():
        return await release_lock(redis, key, lock_id, expire=expire)

    max_attempts = 3
    attempt = -1
    while True:
        attempt += 1
        try:
            res = await run_with_prep(_prepare, _execute)
            assert res is not None
            return res
        except ConnectionError:
            if attempt == max_attempts - 1:
                raise

            del redis
            await asyncio.sleep(0.1 * attempt * (2**attempt) + random.random() * 0.3)
            await itgs.reconnect_redis()
            redis = await itgs.redis()


def parse_release_lock_result(res: Any) -> ReleaseLockResult:
    """Parses the result of the release_lock script"""
    assert isinstance(res, (list, tuple)), res
    assert len(res) == 2, res

    code = res[0]
    assert isinstance(code, int), res

    if code == 1:
        assert res[1] is None, res
        return ReleaseLockSuccessResult(success=True)
    elif code == -3:
        assert res[1] is False, res
        return ReleaseLockNotSetResult(success=False, error_type="not_set")
    elif code == -2:
        assert isinstance(res[1], bytes), res
        return ReleaseLockMalformedResult(
            success=False, error_type="malformed", current_value=res[1]
        )
    elif code == -1:
        assert isinstance(res[1], bytes), res
        return ReleaseLockStolenResult(
            success=False, error_type="stolen", current_value=res[1]
        )
    else:
        raise ValueError(f"Unknown code {code=}")
