from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

EXT_ZPOPMIN_LUA_SCRIPT = """
local sorted_set_key = KEYS[1]
local related_key_prefix = KEYS[2]
local maximum_score = tonumber(ARGV[1])

local matching = redis.call('zrange', sorted_set_key, -1, maximum_score, 'BYSCORE', 'LIMIT', 0, 1)
if matching[1] == nil then
    return 0
end

local related_key = related_key_prefix .. matching[1]

local related_type = redis.call('type', related_key)['ok']
if related_type ~= 'hash' then
    if related_type ~= 'none' then
        redis.call('del', related_key)
    end
    redis.call('zrem', sorted_set_key, matching[1])
    return 0
end

local related = redis.call('hgetall', related_key)
redis.call('zrem', sorted_set_key, matching[1])
redis.call('del', related_key)
return related
"""

EXT_ZPOPMIN_LUA_SCRIPT_HASH = hashlib.sha1(
    EXT_ZPOPMIN_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_ext_zpopmin_ensured_at: Optional[float] = None


async def ensure_ext_zpopmin_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the ext_zpopmin lua script is loaded into redis."""
    global _last_ext_zpopmin_ensured_at

    now = time.time()
    if (
        not force
        and _last_ext_zpopmin_ensured_at is not None
        and (now - _last_ext_zpopmin_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(EXT_ZPOPMIN_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(EXT_ZPOPMIN_LUA_SCRIPT)
        assert (
            correct_hash == EXT_ZPOPMIN_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {EXT_ZPOPMIN_LUA_SCRIPT_HASH=}"

    if _last_ext_zpopmin_ensured_at is None or _last_ext_zpopmin_ensured_at < now:
        _last_ext_zpopmin_ensured_at = now


async def ext_zpopmin(
    redis: redis.asyncio.client.Redis,
    sorted_set_key: Union[str, bytes],
    related_key_prefix: Union[str, bytes],
    maximum_score: float,
) -> Optional[List[Union[str, bytes]]]:
    """If there is a key in the sorted set whose score is less than or equal to
    the maximum score, removes the lowest scoring key from the sorted set,
    deletes the related key, and returns the old value of the related key.

    The related key is created by concatenating the related_key_prefix with the
    lowest scoring key and must be a hash. If the related key exists but is not
    a hash, it will be deleted but None will be returned.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        sorted_set_key (str, bytes): The key of the sorted set to pop from
        related_key_prefix (str, bytes): The prefix to use when constructing the
            related key
        maximum_score (float): The maximum score to pop

    Returns:
        Optional[List[Union[str, bytes]]]: The value of the related key, as if from
            hgetall, or None if no key was popped

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        EXT_ZPOPMIN_LUA_SCRIPT_HASH,
        2,
        sorted_set_key,
        related_key_prefix,
        maximum_score,
    )
    if res is redis:
        return None
    if res in (b"0", "0", 0):
        return None
    if not isinstance(res, list):
        raise ValueError(f"Unexpected response from redis: {res}")
    return res


async def ext_zpopmin_safe(
    itgs: Itgs,
    sorted_set_key: Union[str, bytes],
    related_key_prefix: Union[str, bytes],
    maximum_score: float,
) -> Optional[List[Union[str, bytes]]]:
    """The safe variant of ext_zpopmin, which ensures the script is loaded
    but in return can only be run outside of a pipeline.

    Args:
        itgs (Itgs): the integrations to (re)use
        sorted_set_key (Union[str, bytes]): The key of the sorted set to pop from
        related_key_prefix (Union[str, bytes]): The prefix to use when constructing the
            related key
        maximum_score (float): The maximum score to pop

    Returns:
        Optional[List[Union[str, bytes]]]: The value of the related key, as if from
            hgetall, or None if no key was popped
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_ext_zpopmin_script_exists(redis, force=force)

    async def func():
        return await ext_zpopmin(
            redis, sorted_set_key, related_key_prefix, maximum_score
        )

    return await run_with_prep(prep, func)
