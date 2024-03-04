from typing import Optional, List, cast
import hashlib
import time
import redis.asyncio.client

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

COND_ZPOPMIN_LUA_SCRIPT = """
local sorted_set_key = KEYS[1]
local maximum_score = tonumber(ARGV[1])

local zrange_res = redis.call('zrange', sorted_set_key, -1, maximum_score, 'BYSCORE', 'LIMIT', 0, 1)
if zrange_res[1] == nil then
    return false
end
local item = zrange_res[1]

redis.call('zrem', sorted_set_key, item)
return item
"""

COND_ZPOPMIN_LUA_SCRIPT_HASH = hashlib.sha1(
    COND_ZPOPMIN_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_cond_zpopmin_ensured_at: Optional[float] = None


async def ensure_cond_zpopmin_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the cond_zpopmin lua script is loaded into redis."""
    global _last_cond_zpopmin_ensured_at

    now = time.time()
    if (
        not force
        and _last_cond_zpopmin_ensured_at is not None
        and (now - _last_cond_zpopmin_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(COND_ZPOPMIN_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(COND_ZPOPMIN_LUA_SCRIPT)
        assert (
            correct_hash == COND_ZPOPMIN_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {COND_ZPOPMIN_LUA_SCRIPT_HASH=}"

    if _last_cond_zpopmin_ensured_at is None or _last_cond_zpopmin_ensured_at < now:
        _last_cond_zpopmin_ensured_at = now


async def cond_zpopmin(
    redis: redis.asyncio.client.Redis,
    sorted_set_key: bytes,
    maximum_score: float,
) -> Optional[bytes]:
    """If there is a key in the sorted set whose score is less than or equal to
    the maximum score, acts as zpopmin. Otherwise, does nothing and returns
    None.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        sorted_set_key (bytes): The key of the sorted set to pop from
        maximum_score (float): The maximum score to pop

    Returns:
        Optional[bytes]: The value popped, or None if no value was popped
            or the request was within a pipeline.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(  # type: ignore
        COND_ZPOPMIN_LUA_SCRIPT_HASH,
        1,
        sorted_set_key,  # type: ignore
        maximum_score,  # type: ignore
    )
    if res is redis:
        return None
    return cast(Optional[bytes], res)


async def cond_zpopmin_safe(
    itgs: Itgs,
    sorted_set_key: bytes,
    maximum_score: float,
) -> Optional[bytes]:
    """The safe variant of cond_zpopmin, which ensures the script is loaded
    but in return can only be run outside of a pipeline.

    Args:
        itgs (Itgs): the integrations to (re)use
        sorted_set_key (bytes): The key of the sorted set to pop from
        maximum_score (float): The maximum score to pop

    Returns:
        Optional[bytes]: The value popped, or None if no value was popped
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_cond_zpopmin_script_exists(redis, force=force)

    async def func():
        return await cond_zpopmin(redis, sorted_set_key, maximum_score)

    return await run_with_prep(prep, func)
