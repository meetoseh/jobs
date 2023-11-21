from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

LMOVE_MANY_LUA_SCRIPT = """
local src = KEYS[1]
local dst = KEYS[2]
local max_count = tonumber(ARGV[1])

local current_src_length = redis.call("LLEN", src)
local num_moved = 0

while num_moved < max_count and current_src_length > 0 do
    redis.call('LMOVE', src, dst, 'LEFT', 'RIGHT')
    num_moved = num_moved + 1
    current_src_length = current_src_length - 1
end

return num_moved
"""

LMOVE_MANY_LUA_SCRIPT_HASH = hashlib.sha1(
    LMOVE_MANY_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_lmove_many_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the lmove_many lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(LMOVE_MANY_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(LMOVE_MANY_LUA_SCRIPT)
        assert (
            correct_hash == LMOVE_MANY_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {LMOVE_MANY_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def lmove_many(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    dst: Union[str, bytes],
    max_count: int,
) -> Optional[int]:
    """Repeats lmove src dst left right until max_count is reached or src is empty.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (str): The source list
        dst (str): The destination list
        max_count (int): The maximum number of items to move

    Returns:
        int: The number of items moved

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(LMOVE_MANY_LUA_SCRIPT_HASH, 2, src, dst, max_count)  # type: ignore
    if res is redis:
        return None
    return int(res)


async def lmove_many_safe(
    itgs: Itgs, src: Union[str, bytes], dst: Union[str, bytes], max_count: int
) -> int:
    """Loads the lmove many script if necessary and executes lmove_many
    within the standard redis instance on the integrations.

    Args:
        itgs (Itgs): The integrations
        src (str): The source list
        dst (str): The destination list
        max_count (int): The maximum number of items to move

    Returns:
        int: The number of items moved
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_lmove_many_script_exists(redis, force=force)

    async def func():
        return await lmove_many(redis, src, dst, max_count)

    res = await redis_helpers.run_with_prep.run_with_prep(prep, func)
    assert res is not None
    return res
