from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

SET_IF_LOWER_LUA_SCRIPT = """
local key = KEYS[1]
local value = ARGV[1]

local current_value = redis.call("GET", key)
if (current_value ~= false) and (tonumber(current_value) <= tonumber(value)) then
    return 0
end

redis.call("SET", key, value)
return 1
"""

SET_IF_LOWER_LUA_SCRIPT_HASH = hashlib.sha1(
    SET_IF_LOWER_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_set_if_lower_ensured_at: Optional[float] = None


async def ensure_set_if_lower_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the set_if_lower lua script is loaded into redis."""
    global _last_set_if_lower_ensured_at

    now = time.time()
    if (
        not force
        and _last_set_if_lower_ensured_at is not None
        and (now - _last_set_if_lower_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(SET_IF_LOWER_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(SET_IF_LOWER_LUA_SCRIPT)
        assert (
            correct_hash == SET_IF_LOWER_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {SET_IF_LOWER_LUA_SCRIPT_HASH=}"

    if _last_set_if_lower_ensured_at is None or _last_set_if_lower_ensured_at < now:
        _last_set_if_lower_ensured_at = now


async def set_if_lower(
    redis: redis.asyncio.client.Redis, key: Union[str, bytes], val: int
) -> Optional[bool]:
    """Updates the value in the given key to the given value iff
    the key is unset or the value is lower than the current value.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (str): The key to update
        val (int): The value to update to

    Returns:
        bool, None: True if the value was updated, False otherwise. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(SET_IF_LOWER_LUA_SCRIPT_HASH, 1, key, val)
    if res is redis:
        return None
    return bool(res)
