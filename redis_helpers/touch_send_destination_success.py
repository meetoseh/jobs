from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT = """
local pending_zset = KEYS[1]
local callbacks_key = KEYS[2]
local remaining_key = KEYS[3]
local jobs_key = KEYS[4]
local touch_uid = ARGV[1]
local now = tonumber(ARGV[2])

local zrem_result = redis.call("ZREM", pending_zset, touch_uid)
local success_callback = redis.call("HGET", callbacks_key, "success_callback")
local num_deleted = redis.call("DEL", callbacks_key, remaining_key)

if zrem_result ~= 1 or num_deleted ~= 2 then
    return 0
end

if success_callback == nil or success_callback == false or success_callback == "" then
    return 1
end

local parsed_success_callback = cjson.decode(success_callback)
parsed_success_callback["queued_at"] = now
local encoded_job = cjson.encode(parsed_success_callback)

redis.call("RPUSH", jobs_key, encoded_job)
return 1
"""

TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_send_destination_success_ensured_at: Optional[float] = None


async def ensure_touch_send_destination_success_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_send_destination_success lua script is loaded into redis."""
    global _last_touch_send_destination_success_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_send_destination_success_ensured_at is not None
        and (now - _last_touch_send_destination_success_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(
            TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT
        )
        assert (
            correct_hash == TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_send_destination_success_ensured_at is None
        or _last_touch_send_destination_success_ensured_at < now
    ):
        _last_touch_send_destination_success_ensured_at = now


async def touch_send_destination_success(
    redis: redis.asyncio.client.Redis,
    pending_zset: Union[str, bytes],
    callbacks_key: Union[str, bytes],
    remaining_key: Union[str, bytes],
    jobs_key: Union[str, bytes],
    uid: Union[str, bytes],
    now: float,
) -> Optional[bool]:
    """There are two cases this handles. The case is determined and handled all
    atomically.

    # Case 1

    The touch is in the pending set

    ## Precondition

    - `pending_zset` (usually `b"touch:pending"`) contains `uid` (the touch uid)
    - `callbacks_key` (usually formed via `touch:pending:{uid}` as bytes) goes to
      a hash, which might contain `b"success_callback"` as on of its keys
    - `remaining_key` (usually formed via `touch:pending:{uid}:remaining` as bytes)
      goes to a list with at least one item

    ## Result

    The touch is removed from the pending set, the callbacks key and remaining key
    are deleted, and the success callback is decoded as json, the `queued_at` field
    is set to `now`, and then the result is `RPUSH`d to `jobs_key` (usually
    `b"jobs:hot"`)

    # Case 2

    The touch is not in the pending set

    ## Precondition

    Not case 1

    ## Result

    The touch is removed from the pending set (if in there), the callbacks key
    is deleted (if it exists), and the remaining key is deleted (if it exists).
    Usually, this is a no-op, but it does mean corrupted values are cleaned up.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        pending_zset (str, bytes): The key of the pending zset (usually `b"touch:pending"`)
        callbacks_key (str, bytes): The key of the callbacks hash (usually formed via
            `touch:pending:{uid}` as bytes)
        remaining_key (str, bytes): The key of the remaining list (usually formed via
            `touch:pending:{uid}:remaining` as bytes)
        jobs_key (str, bytes): The key of the jobs list (usually `b"jobs:hot"`)
        uid (str, bytes): the uid of the touch, i.e., the value within the pending zset
        now (float): the current time in seconds since the epoch, used for the jobs
            `queued_at` if we enqueue it

    Returns:
        bool, None: True for case 1, False for case 2, None if executed
        within a pipeline and thus the result is delayed until the pipeline is
        executed

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(  # type: ignore
        TOUCH_SEND_DESTINATION_SUCCESS_LUA_SCRIPT_HASH,
        4,
        pending_zset,  # type: ignore
        callbacks_key,  # type: ignore
        remaining_key,  # type: ignore
        jobs_key,  # type: ignore
        uid,  # type: ignore
        now,  # type: ignore
    )
    if res is redis:
        return None
    return bool(res)
