from typing import Any, Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

ACQUIRE_LOCK_LUA_SCRIPT = """
local key = KEYS[1]
local hostname = ARGV[1]
local now = tonumber(ARGV[2])
local lock_id = ARGV[3]

local current_value = redis.call("GET", key)
if current_value == false then
    redis.call("SET", key, cjson.encode({
        hostname = hostname,
        acquired_at = now,
        lock_id = lock_id
    }))
    return {1, false}
end

local success, lock = pcall(function() return cjson.decode(current_value) end)
if 
    not success 
    or type(lock) ~= 'table' 
    or type(lock.hostname) ~= 'string' 
    or type(lock.acquired_at) ~= 'number' 
    or type(lock.lock_id) ~= 'string'
then
    redis.call("EXPIREAT", key, now + 300, "NX")
    return {-5, current_value}
end

if lock.hostname == hostname then
    if lock.lock_id == lock_id then
        return {-4, current_value}
    end

    redis.call("EXPIREAT", key, now + 300, "NX")
    redis.call("EXPIREAT", key, now + 300, "LT")
    return {-3, current_value}
end

if lock.acquired_at < (now - 60 * 60 * 6) then
    redis.call("EXPIREAT", key, now + 3600, "NX")
    redis.call("EXPIREAT", key, now + 3600, "LT")
    return {-2, current_value}
end

return {-1, current_value}
"""

ACQUIRE_LOCK_LUA_SCRIPT_HASH = hashlib.sha1(
    ACQUIRE_LOCK_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_acquire_lock_ensured_at: Optional[float] = None


async def ensure_acquire_lock_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the acquire_lock lua script is loaded into redis."""
    global _last_acquire_lock_ensured_at

    now = time.time()
    if (
        not force
        and _last_acquire_lock_ensured_at is not None
        and (now - _last_acquire_lock_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(ACQUIRE_LOCK_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(ACQUIRE_LOCK_LUA_SCRIPT)
        assert (
            correct_hash == ACQUIRE_LOCK_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {ACQUIRE_LOCK_LUA_SCRIPT_HASH=}"

    if _last_acquire_lock_ensured_at is None or _last_acquire_lock_ensured_at < now:
        _last_acquire_lock_ensured_at = now


@dataclass
class AcquireLockSuccessResult:
    success: Literal[True]
    error_type: Literal[None]


@dataclass
class AcquireLockMalformedResult:
    success: Literal[False]
    error_type: Literal["malformed"]
    """Indicates that the current value at that key is not a valid lock."""
    current_value: bytes


@dataclass
class AcquireLockAlreadyHeldResult:
    success: Literal[False]
    error_type: Literal["already_held"]
    """Indicates that the current value at that key is a lock for this host with the given lock_id"""
    current_value: bytes


@dataclass
class AcquireLockSameHostResult:
    success: Literal[False]
    error_type: Literal["same_host"]
    """
    Indicates that the current value at that key is a lock for this host with a
    different lock_id, which should mean that the owner of the lock crashed
    unexpectedly as the caller of acquire_lock needs to verify this host isn't
    already operating with the lock, so we set an expiration time on the lock of
    5 minutes (enough time for a human to intervene if e.g. we just deployed something
    with a bug, but also short enough that we will automatically recover pretty
    quickly if it was just an unexpected crash)
    """
    current_value: bytes


@dataclass
class AcquireLockOtherHostStaleResult:
    success: Literal[False]
    error_type: Literal["other_host_stale"]
    """Indicates that another host owns the lock, but they've owned it for 6+
    hours. We set a 1 hour expiration on the lock since probably they crashed
    unexpectedly, but our infrastructure tends to reuse hosts, so it's a little
    odd that they haven't attempted to re-acquire the lock and fixed the issue
    yet, assuming crashes are random and thus typically affect locks that are
    constantly being acquired/released.
    """
    current_value: bytes


@dataclass
class AcquireLockOtherHostFreshResult:
    success: Literal[False]
    error_type: Literal["other_host_fresh"]
    """Indicates that another host owns the lock, and they acquired it within the
    last 6 hours, so there's nothing particularly unusual about the lock. 
    """
    current_value: bytes


AcquireLockResult = Union[
    AcquireLockSuccessResult,
    AcquireLockMalformedResult,
    AcquireLockAlreadyHeldResult,
    AcquireLockSameHostResult,
    AcquireLockOtherHostStaleResult,
    AcquireLockOtherHostFreshResult,
]


async def acquire_lock(
    redis: redis.asyncio.client.Redis,
    key: bytes,
    hostname: bytes,
    now: int,
    lock_id: bytes,
) -> Optional[AcquireLockResult]:
    """Acquires the lock at the given key, if it is not already held. It is important
    that this is not called unless the caller can ensure that this host does not
    hold the lock to this key, in this process or others. Most of the time our deployment
    naturally ensures this, otherwise, a host-local lock (e.g., file lock) should be used.

    The lock should be released with `release_lock` using the same lock_id later. Since
    it's assumed lock_id is sufficiently random that no two hosts will accidentally
    generate the same value, there is no need to pass the hostname to `release_lock`,
    which means that the lock can be released on a different host if desired.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (bytes): The key to acquire the lock on
        hostname (bytes): The hostname of the process acquiring the lock
        now (int): The current time in seconds since the epoch
        lock_id (bytes): A random string that can be used to ensure when releasing
            the lock it wasn't stolen

    Returns:
        AcquireLockResult, None: The result of acquiring the lock, if not run in
            a pipeline. Otherwise, None as the result can't be known until the
            pipeline is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(ACQUIRE_LOCK_LUA_SCRIPT_HASH, 1, key, hostname, now, lock_id)  # type: ignore
    if res is redis:
        return None
    return parse_acquire_lock_result(res)


async def acquire_lock_safe(
    itgs: Itgs,
    key: bytes,
    hostname: bytes,
    now: int,
    lock_id: bytes,
) -> AcquireLockResult:
    """Same as acquire_lock, but ensures the script is loaded first and always
    returns a result.
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_acquire_lock_script_exists(redis, force=force)

    async def _execute():
        return await acquire_lock(redis, key, hostname, now, lock_id)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_acquire_lock_result(res: Any) -> AcquireLockResult:
    """Parses the result of the acquire_lock script"""
    assert isinstance(res, (list, tuple)), res
    assert len(res) == 2, res

    code = res[0]
    assert isinstance(code, int), res

    if code == 1:
        assert res[1] is None, res
        return AcquireLockSuccessResult(success=True, error_type=None)
    elif code == -5:
        assert isinstance(res[1], bytes), res
        return AcquireLockMalformedResult(
            success=False, error_type="malformed", current_value=res[1]
        )
    elif code == -4:
        assert isinstance(res[1], bytes), res
        return AcquireLockAlreadyHeldResult(
            success=False, error_type="already_held", current_value=res[1]
        )
    elif code == -3:
        assert isinstance(res[1], bytes), res
        return AcquireLockSameHostResult(
            success=False, error_type="same_host", current_value=res[1]
        )
    elif code == -2:
        assert isinstance(res[1], bytes), res
        return AcquireLockOtherHostStaleResult(
            success=False, error_type="other_host_stale", current_value=res[1]
        )
    elif code == -1:
        assert isinstance(res[1], bytes), res
        return AcquireLockOtherHostFreshResult(
            success=False, error_type="other_host_fresh", current_value=res[1]
        )
    else:
        raise ValueError(f"Unexpected code {code=}")
