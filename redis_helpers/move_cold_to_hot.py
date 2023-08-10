from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

MOVE_COLD_TO_HOT_LUA_SCRIPT = """
local src = KEYS[1]
local dst = KEYS[2]
local max_score = tonumber(ARGV[1])
local max_count = tonumber(ARGV[2])

local current_src_length = tonumber(redis.call("ZCARD", src))
local num_moved = 0

while num_moved < max_count and current_src_length > 0 do
    local next_item_and_score = redis.call("ZRANGE", src, 0, 0, "WITHSCORES")
    if tonumber(next_item_and_score[2]) > max_score then
        break
    end

    redis.call('ZREMRANGEBYRANK', src, 0, 0)
    redis.call('RPUSH', dst, next_item_and_score[1])
    
    num_moved = num_moved + 1
    current_src_length = current_src_length - 1
end

return num_moved
"""

MOVE_COLD_TO_HOT_LUA_SCRIPT_HASH = hashlib.sha1(
    MOVE_COLD_TO_HOT_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_move_cold_to_hot_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the move_cold_to_hot lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(MOVE_COLD_TO_HOT_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(MOVE_COLD_TO_HOT_LUA_SCRIPT)
        assert (
            correct_hash == MOVE_COLD_TO_HOT_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {MOVE_COLD_TO_HOT_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def move_cold_to_hot(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    dst: Union[str, bytes],
    max_score: float,
    max_count: int,
) -> int:
    """Moves the lowest score item from the redis sorted set `src` to
    the right of the redis list `dst` until one of the following is true:
    - `src` is empty
    - `max_count` items have been moved
    - the score of the next item to move is greater than `max_score`

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (str): The source list
        dst (str): The destination list
        max_score (float): The maximum score to move (inclusive)
        max_count (int): The maximum number of items to move (inclusive)

    Returns:
        int: The number of items moved

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        MOVE_COLD_TO_HOT_LUA_SCRIPT_HASH, 2, src, dst, max_score, max_count
    )
    if res is redis:
        return None
    return int(res)


async def move_cold_to_hot_safe(
    itgs: Itgs, src: str, dst: str, max_score: int, max_count: int
) -> int:
    """Loads the move_cold_to_hot script if necessary and executes move_cold_to_hot
    within the standard redis instance on the integrations.

    Args:
        itgs (Itgs): The integrations
        src (str): The source list
        dst (str): The destination list
        max_score (int): The maximum score to move
        max_count (int): The maximum number of items to move

    Returns:
        int: The number of items moved
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_move_cold_to_hot_script_exists(redis, force=force)

    async def func():
        return await move_cold_to_hot(redis, src, dst, max_score, max_count)

    return await redis_helpers.run_with_prep.run_with_prep(prep, func)
