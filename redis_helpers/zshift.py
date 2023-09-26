from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

ZSHIFT_LUA_SCRIPT = """
local src = KEYS[1]
local dst = KEYS[2]
local max_to_shift = tonumber(ARGV[1])
local max_score_to_shift = ARGV[2]

local items = {}

if max_score_to_shift == nil then
    items = redis.call("ZRANGE", src, 0, max_to_shift - 1, "WITHSCORES")
else
    items = redis.call("ZRANGE", src, "-inf", max_score_to_shift, "BYSCORE", "LIMIT", 0, max_to_shift, "WITHSCORES")
end

for i = 1, #items, 2 do
    local value = items[i]
    local score = items[i + 1]
    redis.call("ZREM", src, value)
    redis.call("ZADD", dst, score, value)
end

return #items / 2
"""

ZSHIFT_LUA_SCRIPT_HASH = hashlib.sha1(ZSHIFT_LUA_SCRIPT.encode("utf-8")).hexdigest()


_last_zshift_ensured_at: Optional[float] = None


async def ensure_zshift_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the zshift lua script is loaded into redis."""
    global _last_zshift_ensured_at

    now = time.time()
    if (
        not force
        and _last_zshift_ensured_at is not None
        and (now - _last_zshift_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(ZSHIFT_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(ZSHIFT_LUA_SCRIPT)
        assert (
            correct_hash == ZSHIFT_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {ZSHIFT_LUA_SCRIPT_HASH=}"

    if _last_zshift_ensured_at is None or _last_zshift_ensured_at < now:
        _last_zshift_ensured_at = now


async def zshift(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    dst: Union[str, bytes],
    max_to_shift: int,
    max_score_to_shift: Optional[float] = None,
) -> Optional[int]:
    """Moves up to the given maximum numbers of items from the src sorted set to the
    dst sorted set. Items are moved in order of score, from lowest to highest.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (str, bytes): The source sorted set
        dst (str, bytes): The destination sorted set
        max_to_shift (int): The maximum number of items to shift
        max_score_to_shift (float, None): The maximum score to shift. If None, no
            maximum score is applied. This does not slow down the inner loop.

    Returns:
        int, None: The number of items moved. None if executed within a
            transaction, since the result is not known until the transaction is
            executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    if max_score_to_shift is None:
        res = await redis.evalsha(ZSHIFT_LUA_SCRIPT_HASH, 2, src, dst, max_to_shift)
    else:
        res = await redis.evalsha(
            ZSHIFT_LUA_SCRIPT_HASH, 2, src, dst, max_to_shift, max_score_to_shift
        )

    if res is redis:
        return None
    return int(res)
