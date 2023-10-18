from typing import Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs
from dataclasses import dataclass

MOVE_COLD_TO_HOT_LUA_SCRIPT = """
local src = KEYS[1]
local dst = KEYS[2]
local max_score = tonumber(ARGV[1])
local max_count = tonumber(ARGV[2])
local backpressure = ARGV[3]

if backpressure == "" then
    backpressure = false
else
    backpressure = tonumber(backpressure)
end

local current_src_length = tonumber(redis.call("ZCARD", src))
local num_moved = 0

local current_dst_length = 0
if backpressure ~= false then
    current_dst_length = tonumber(redis.call("LLEN", dst))
end

while true do
    if current_src_length == 0 then
        return {-1, num_moved}
    end

    if num_moved == max_count then
        return {-2, num_moved}
    end

    if backpressure and current_dst_length >= backpressure then
        return {-3, num_moved}
    end

    local next_item_and_score = redis.call("ZRANGE", src, 0, 0, "WITHSCORES")
    if tonumber(next_item_and_score[2]) > max_score then
        return {-4, num_moved}
    end

    redis.call('ZREMRANGEBYRANK', src, 0, 0)
    redis.call('RPUSH', dst, next_item_and_score[1])
    
    num_moved = num_moved + 1
    current_src_length = current_src_length - 1
    current_dst_length = current_dst_length + 1
end
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


@dataclass
class MoveColdToHotResult:
    stop_reason: Literal["src_empty", "max_count", "backpressure", "max_score"]
    num_moved: int


async def move_cold_to_hot(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    dst: Union[str, bytes],
    max_score: float,
    max_count: int,
    *,
    backpressure: Optional[int] = None,
) -> Optional[MoveColdToHotResult]:
    """Moves the lowest score item from the redis sorted set `src` to
    the right of the redis list `dst` until one of the following is true:
    - `src` is empty
    - `max_count` items have been moved
    - the score of the next item to move is greater than `max_score`
    - `dst` has at least `backpressure` items

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (str): The source list
        dst (str): The destination list
        max_score (float): The maximum score to move (inclusive)
        max_count (int): The maximum number of items to move (inclusive)
        backpressure (int, None): The maximum length of the destination list
            or None for no limit

    Returns:
        MoveColdToHotResult, None: The number of items moved and stop reason or
            None if run within a transaction since the result isn't known until
            the transaction is executed

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        MOVE_COLD_TO_HOT_LUA_SCRIPT_HASH,
        2,
        src,
        dst,
        max_score,
        max_count,
        backpressure if backpressure is not None else b"",
    )
    if res is redis:
        return None
    return parse_move_cold_to_hot_result(res)


def parse_move_cold_to_hot_result(res) -> MoveColdToHotResult:
    """Parses the result of the move cold to hot redis script into
    a more usable format.
    """
    assert isinstance(res, (list, tuple)), res
    assert len(res) == 2, res
    assert isinstance(res[0], int), res
    assert isinstance(res[1], int), res

    if res[0] == -1:
        return MoveColdToHotResult("src_empty", res[1])
    elif res[0] == -2:
        return MoveColdToHotResult("max_count", res[1])
    elif res[0] == -3:
        return MoveColdToHotResult("backpressure", res[1])
    elif res[0] == -4:
        return MoveColdToHotResult("max_score", res[1])

    assert False, res


async def move_cold_to_hot_safe(
    itgs: Itgs,
    src: Union[str, bytes],
    dst: Union[str, bytes],
    max_score: int,
    max_count: int,
    *,
    backpressure: Optional[int] = None,
) -> MoveColdToHotResult:
    """Loads the move_cold_to_hot script if necessary and executes move_cold_to_hot
    within the standard redis instance on the integrations.

    Args:
        itgs (Itgs): The integrations
        src (str): The source list
        dst (str): The destination list
        max_score (int): The maximum score to move
        max_count (int): The maximum number of items to move
        backpressure (int, None): The maximum length of the destination list
            or None for no limit

    Returns:
        int: The number of items moved
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_move_cold_to_hot_script_exists(redis, force=force)

    async def func():
        return await move_cold_to_hot(
            redis, src, dst, max_score, max_count, backpressure=backpressure
        )

    return await redis_helpers.run_with_prep.run_with_prep(prep, func)
