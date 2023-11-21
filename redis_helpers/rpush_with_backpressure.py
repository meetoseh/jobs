from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT = """
local key = KEYS[1]
local value = ARGV[1]
local max_length = tonumber(ARGV[2])

local current_length = redis.call("LLEN", key)
if tonumber(current_length) >= max_length then
    return 0
end

redis.call("RPUSH", key, value)
return 1
"""

RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT_HASH = hashlib.sha1(
    RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_rpush_with_backpressure_ensured_at: Optional[float] = None


async def ensure_rpush_with_backpressure_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the rpush_with_backpressure lua script is loaded into redis."""
    global _last_rpush_with_backpressure_ensured_at

    now = time.time()
    if (
        not force
        and _last_rpush_with_backpressure_ensured_at is not None
        and (now - _last_rpush_with_backpressure_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT)
        assert (
            correct_hash == RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT_HASH=}"

    if (
        _last_rpush_with_backpressure_ensured_at is None
        or _last_rpush_with_backpressure_ensured_at < now
    ):
        _last_rpush_with_backpressure_ensured_at = now


async def rpush_with_backpressure(
    redis: redis.asyncio.client.Redis,
    key: Union[str, bytes],
    value: Union[str, bytes],
    max_length: int,
) -> Optional[bool]:
    """Inserts the given value into the list at the given key, but only
    if the list is not longer than the given max length.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (str, bytes): The key containing the list
        value (str, bytes): The value to insert
        max_length (int): The maximum length of the list before the insert in order
            to actually insert the value

    Returns:
        bool, None: True if the value was inserted, False otherwise. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(  # type: ignore
        RPUSH_WITH_BACKPRESSURE_LUA_SCRIPT_HASH, 1, key, value, max_length  # type: ignore
    )
    if res is redis:
        return None
    return bool(res)
