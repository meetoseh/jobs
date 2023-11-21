from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT = """
local pending_zset = KEYS[1]
local callbacks_key = KEYS[2]
local remaining_key = KEYS[3]
local jobs_key = KEYS[4]

local uid = ARGV[1]
local attempt_uid = ARGV[2]
local now = tonumber(ARGV[3])

local zscore_pending_result = redis.call("ZSCORE", pending_zset, uid)
local in_pending = zscore_pending_result ~= nil and zscore_pending_result ~= false

if not in_pending then
    redis.call("DEL", callbacks_key, remaining_key)
    return 3
end

local srem_remaining_result = redis.call("SREM", remaining_key, attempt_uid)
local remaining_card = redis.call("SCARD", remaining_key)

if srem_remaining_result == 0 and remaining_card == 0 then
    redis.call("ZREM", pending_zset, uid)
    redis.call("DEL", callbacks_key, remaining_key)
    return 3
end

if remaining_card ~= 0 then
    return 1
end

local failure_callback = redis.call("HGET", callbacks_key, "failure_callback")
redis.call("ZREM", pending_zset, uid)
redis.call("DEL", callbacks_key, remaining_key)

if failure_callback == nil or failure_callback == false or failure_callback == "" then
    return 2
end

local parsed_failure_callback = cjson.decode(failure_callback)
parsed_failure_callback["queued_at"] = now
local encoded_job = cjson.encode(parsed_failure_callback)

redis.call("RPUSH", jobs_key, encoded_job)
return 2
"""

TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_send_destination_failed_ensured_at: Optional[float] = None


async def ensure_touch_send_destination_failed_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_send_destination_failed lua script is loaded into redis."""
    global _last_touch_send_destination_failed_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_send_destination_failed_ensured_at is not None
        and (now - _last_touch_send_destination_failed_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_send_destination_failed_ensured_at is None
        or _last_touch_send_destination_failed_ensured_at < now
    ):
        _last_touch_send_destination_failed_ensured_at = now


async def touch_send_destination_failed(
    redis: redis.asyncio.client.Redis,
    pending_zset: Union[str, bytes],
    callbacks_key: Union[str, bytes],
    remaining_key: Union[str, bytes],
    jobs_key: Union[str, bytes],
    uid: Union[str, bytes],
    attempt_uid: Union[str, bytes],
    now: float,
) -> Optional[int]:
    """There are three cases this handles. The case is determined and handled all
    atomically.

    # Case 1

    The touch is in the pending set, but there are other destinations remaining.

    ## Precondition

    - `pending_zset` (usually `b"touch:pending"`) contains `uid` (the touch uid)
    - `callbacks_key` (usually formed via `touch:pending:{uid}` as bytes) goes to
      a hash, which might contain `b"failure_callback"` as one of its keys
    - `remaining_key` (usually formed via `touch:pending:{uid}:remaining` as bytes)
      goes to a set with at least one item besides `attempt_uid` (the uid of the
      attempt that failed, e.g., an email uid).

    ## Result

    The `attempt_uid` is removed from `remaining_key`, if it is present.

    # Case 2

    The touch is in the pending set, and there are no other destinations remaining.

    ## Precondition

    - `pending_zset` (usually `b"touch:pending"`) contains `uid` (the touch uid)
    - `callbacks_key` (usually formed via `touch:pending:{uid}` as bytes) goes to
      a hash, which might contain `b"failure_callback"` as one of its keys
    - `remaining_key` (usually formed via `touch:pending:{uid}:remaining` as bytes)
      goes to a set with 1 item, `attempt_uid` (the attempt that failed).

    ## Result

    - `pending_zset` no longer contains `uid`
    - `callbacks_key` is deleted
    - `remaining_key` is deleted
    - If the `callbacks_key` had a failure callback, its rpush'd to the
      `jobs_key` (usually `b"jobs:hot"`) list with `queued_at` set to `now`

    # Case 3

    The touch is not in the pending set.

    ## Precondition

    Neither Case 1 nor Case 2. This will normally result in a no-op, but if the
    uid is corrupted it will be cleaned up.

    ## Result

    - `pending_zset` no longer contains `uid`
    - `callbacks_key` is deleted
    - `remaining_key` is deleted

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        pending_zset (Union[str, bytes]): The key of the pending zset, usually
            `b"touch:pending"`
        callbacks_key (Union[str, bytes]): The key of the callbacks hash, usually
            formed via `touch:pending:{uid}` as bytes
        remaining_key (Union[str, bytes]): The key of the remaining set, usually
            formed via `touch:pending:{uid}:remaining` as bytes
        jobs_key (Union[str, bytes]): The key of the jobs list, usually
            `b"jobs:hot"`
        uid (Union[str, bytes]): The uid of the touch
        attempt_uid (Union[str, bytes]): The uid of the attempt that failed, e.g.,
            `oseh_em_xxx`, `oseh_sms_xxx`, or `oseh_pma_xxx` for email/sms/push respectively
        now (float): The current time in seconds since the epoch, used as the
            `queued_at` for the failure callback, if queued


    Returns:
        int, None: The case that was handled. None if executed within a transaction,
            since the result is not known until the transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(  # type: ignore
        TOUCH_SEND_DESTINATION_FAILED_LUA_SCRIPT_HASH,
        4,
        pending_zset,  # type: ignore
        callbacks_key,  # type: ignore
        remaining_key,  # type: ignore
        jobs_key,  # type: ignore
        uid,  # type: ignore
        attempt_uid,  # type: ignore
        now,  # type: ignore
    )
    if res is redis:
        return None
    return int(res)
