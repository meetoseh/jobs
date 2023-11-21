from typing import Optional, List, Sequence, Union
import hashlib
import time
import redis.asyncio.client

TOUCH_PERSIST_CLEANUP_LUA_SCRIPT = """
local persist_key = KEYS[1]
local buffer_key = KEYS[2]
local codes = ARGV

for _, code in ipairs(codes) do
    redis.call("ZREM", persist_key, code)
    redis.call("ZREM", buffer_key, code)
    redis.call("DEL", buffer_key .. ":" .. code)
    
    local clicks_key = buffer_key .. ":clicks:" .. code
    local clicks = redis.call("LRANGE", clicks_key, 0, -1)

    for _, click in ipairs(clicks) do
        local decoded_click = cjson.decode(click)
        local click_uid = decoded_click["uid"]
        redis.call("DEL", buffer_key .. ":on_clicks_by_uid:" .. click_uid)
    end

    redis.call("DEL", clicks_key)
end

return redis.call("ZCARD", persist_key)
"""

TOUCH_PERSIST_CLEANUP_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_PERSIST_CLEANUP_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_persist_cleanup_ensured_at: Optional[float] = None


async def ensure_touch_persist_cleanup_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_persist_cleanup lua script is loaded into redis."""
    global _last_touch_persist_cleanup_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_persist_cleanup_ensured_at is not None
        and (now - _last_touch_persist_cleanup_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_PERSIST_CLEANUP_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_PERSIST_CLEANUP_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_PERSIST_CLEANUP_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_PERSIST_CLEANUP_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_persist_cleanup_ensured_at is None
        or _last_touch_persist_cleanup_ensured_at < now
    ):
        _last_touch_persist_cleanup_ensured_at = now


async def touch_persist_cleanup(
    redis: redis.asyncio.client.Redis,
    persist_key: Union[str, bytes],
    buffer_key: Union[str, bytes],
    codes: Sequence[Union[str, bytes]],
) -> Optional[int]:
    """Deletes the given codes from the touch persist purgatory, removing all related
    keys and returning the new purgatory size

    This will:

    - Remove code from the zset `{persist_key}`
    - Remove code from the zset `{buffer_key}`
    - Delete the hash `{buffer_key}:{code}`
    - Delete the list `{buffer_key}:clicks:{code}`
    - Delete the hash `{buffer_key}:on_clicks_by_uid:{uid}` for each uid in the list
      of codes, where each item in the list of codes is a json string with a uid key

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        persist_key (Union[str, bytes]): The key for the persist purgatory

    Returns:
        int, None: The new cardinality of the persist purgatory. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        TOUCH_PERSIST_CLEANUP_LUA_SCRIPT_HASH, 2, persist_key, buffer_key, *codes  # type: ignore
    )
    if res is redis:
        return None
    return int(res)
