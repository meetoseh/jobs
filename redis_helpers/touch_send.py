from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

TOUCH_SEND_LUA_SCRIPT = """
local touch_to_send_key = KEYS[1]
local stats_key = KEYS[2]
local stats_earliest_key = KEYS[3]
local touch = ARGV[1]
local max_length = tonumber(ARGV[2])
local unix_date = tonumber(ARGV[3])

local current_length = redis.call("LLEN", touch_to_send_key)
if tonumber(current_length) >= max_length then
    return 0
end

redis.call("RPUSH", touch_to_send_key, touch)

local stored_earliest = redis.call("GET", stats_earliest_key)
if stored_earliest == false or tonumber(stored_earliest) > unix_date then
    redis.call("SET", stats_earliest_key, unix_date)
end

redis.call("HINCRBY", stats_key, "queued", 1)
return 1
"""

TOUCH_SEND_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_SEND_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_send_ensured_at: Optional[float] = None


async def ensure_touch_send_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_send lua script is loaded into redis."""
    global _last_touch_send_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_send_ensured_at is not None
        and (now - _last_touch_send_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(TOUCH_SEND_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_SEND_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_SEND_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_SEND_LUA_SCRIPT_HASH=}"

    if _last_touch_send_ensured_at is None or _last_touch_send_ensured_at < now:
        _last_touch_send_ensured_at = now


async def touch_send(
    redis: redis.asyncio.client.Redis,
    touch_to_send_key: Union[str, bytes],
    stats_key: Union[str, bytes],
    stats_earliest_key: Union[str, bytes],
    touch: Union[str, bytes],
    max_length: int,
    unix_date: int,
) -> Optional[bool]:
    """Inserts the given touch into the right of the list at the given To Send
    queue as long as its length before the insert is less than `max_length`. Upon
    inserting, increments `queued` in the hash at the given stats key and sets
    the value at the stats_earliest_key to the given unix date if it is not set
    or is larger.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        touch_to_send_key (str, bytes): The key containing the touch To Send queue,
          e.g., `b"touch:to_send"`
        stats_key (str, bytes): The key containing the day's stats, e.g.,
          `stats:touch_send:daily:{unix_date}`
        stats_earliest_key (str, bytes): The key containing the earliest unix date
            in the stats, e.g., `stats:touch_send:daily:earliest`
        touch (str, bytes): The touch to insert, an encoded TouchToSend
        max_length (int): The maximum length of the To Send queue before the insert
            in order to actually insert the value
        unix_date (int): The unix date of the touch, used to update the stats

    Returns:
        bool, None: True if the value was inserted, False otherwise. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        TOUCH_SEND_LUA_SCRIPT_HASH,
        3,
        touch_to_send_key,
        stats_key,
        stats_earliest_key,
        touch,
        max_length,
        unix_date,
    )
    if res is redis:
        return None
    return bool(res)
