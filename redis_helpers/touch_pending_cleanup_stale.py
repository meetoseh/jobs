from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT = """
local pending_key = KEYS[1]
local jobs_key = KEYS[2]

local max_timestamp_for_stale = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local max_to_clean = tonumber(ARGV[3])


local num_cleaned = 0
while num_cleaned < max_to_clean do
    local oldest = redis.call("ZRANGE", pending_key, 0, 0, "WITHSCORES")
    if #oldest == 0 then break end

    local oldest_uid = oldest[1]
    local oldest_timestamp = tonumber(oldest[2])

    if oldest_timestamp >= max_timestamp_for_stale then break end

    local callbacks_key = pending_key .. ':' .. oldest_uid
    local remaining_key = callbacks_key .. ':remaining'

    local failure_callback = redis.call("HGET", callbacks_key, "failure_callback")
    local num_deleted = redis.call("DEL", callbacks_key, remaining_key)
    redis.call("ZREM", pending_key, oldest_uid)

    num_cleaned = num_cleaned + 1

    if failure_callback ~= nil and failure_callback ~= false and failure_callback ~= "" and num_deleted == 2 then
        local parsed_failure_callback = cjson.decode(failure_callback)
        parsed_failure_callback["queued_at"] = now
        local job = cjson.encode(parsed_failure_callback)
        redis.call("RPUSH", jobs_key, job)
    end
end

return num_cleaned
"""

TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_pending_cleanup_stale_ensured_at: Optional[float] = None


async def ensure_touch_pending_cleanup_stale_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_pending_cleanup_stale lua script is loaded into redis."""
    global _last_touch_pending_cleanup_stale_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_pending_cleanup_stale_ensured_at is not None
        and (now - _last_touch_pending_cleanup_stale_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_pending_cleanup_stale_ensured_at is None
        or _last_touch_pending_cleanup_stale_ensured_at < now
    ):
        _last_touch_pending_cleanup_stale_ensured_at = now


async def touch_pending_cleanup_stale(
    redis: redis.asyncio.client.Redis,
    pending_key: Union[str, bytes],
    jobs_key: Union[str, bytes],
    max_timestamp_for_stale: Union[int, float],
    max_to_clean: int,
    now: Union[int, float],
) -> Optional[int]:
    """Cleans up entries within the `pending_key` sorted set (usually `touch:pending`)
    whose score is strictly less than the `max_timestamp_for_stale`, but removing no
    more than `max_to_clean` entries. Entries are cleaned up by:

    - removing the entry from `pending_key` (usually `touch:pending`)
    - deleting the hash at `pending_key:{uid}` (usually `touch:pending:{uid}`)
    - deleting the set at `pending_key:{uid}:remaining` (usually `touch:pending:{uid}:remaining`)
    - if the hash had `failure_callback` set, it's parsed as json, the `queued_at`
      key is set to `now`, and the resulting json is pushed onto the `jobs_key` list
      (usually `jobs:hot`) (thus queueing the failure callback)

    If the stale entry is corrupt, e.g., there is no remaining set, it's cleaned
    up without calling its failure callback

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        pending_key (str): The key to update
        jobs_key (str): The key to push failure callbacks onto
        max_timestamp_for_stale (int): The maximum timestamp to consider stale
        max_to_clean (int): The maximum number of entries to clean up
        now (int): The current timestamp

    Returns:
        int, None: The number of stale entries cleaned. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(  # type: ignore
        TOUCH_PENDING_CLEANUP_STALE_LUA_SCRIPT_HASH,
        2,
        pending_key,  # type: ignore
        jobs_key,  # type: ignore
        max_timestamp_for_stale,  # type: ignore
        now,  # type: ignore
        max_to_clean,  # type: ignore
    )
    if res is redis:
        return None
    return int(res)
