from typing import Optional, List, Union, cast
import hashlib
import time
import redis.asyncio.client

from itgs import Itgs

ZCARD_EXACT_WINDOW_LUA_SCRIPT = """
local key = KEYS[1]
local later_than = tonumber(ARGV[1])

redis.call('zremrangebyscore', key, '-inf', later_than)
return redis.call('zcard', key)
"""

ZCARD_EXACT_WINDOW_LUA_SCRIPT_HASH = hashlib.sha1(
    ZCARD_EXACT_WINDOW_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_zcard_exact_window_ensured_at: Optional[float] = None


async def ensure_zcard_exact_window_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the zcard_exact_window lua script is loaded into redis."""
    global _last_zcard_exact_window_ensured_at

    now = time.time()
    if (
        not force
        and _last_zcard_exact_window_ensured_at is not None
        and (now - _last_zcard_exact_window_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(ZCARD_EXACT_WINDOW_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(ZCARD_EXACT_WINDOW_LUA_SCRIPT)
        assert (
            correct_hash == ZCARD_EXACT_WINDOW_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {ZCARD_EXACT_WINDOW_LUA_SCRIPT_HASH=}"

    if (
        _last_zcard_exact_window_ensured_at is None
        or _last_zcard_exact_window_ensured_at < now
    ):
        _last_zcard_exact_window_ensured_at = now


async def zcard_exact_window(
    redis: redis.asyncio.client.Redis, key: bytes, later_than: Union[int, float]
) -> Optional[int]:
    """Removes all elements from the sorted set at `key` with a score less than
    or equal to `later_than` and returns the number of elements remaining in the
    set.

    This is particularly useful for detecting outages on downstream providers. If
    you define an outage as X errors in the last Y seconds, then the sorted set
    can cantain one element per error where the score is the timestamp of the
    error. This prunes events that no longer matter for outage detection and then
    counts the remaining events, which if it exceeds X, indicates an outage.

    For inserting, use `zadd_exact_window` from `redis_helpers.zadd_exact_window`
    to select minimally sized identifiers, avoiding pointless memory usage that
    would be required if using random identifiers.

    This could be replicated without a script using a pipeline that calls
    zremrangebyscore and zcard with no difference in functionality except that
    it's more difficult to compose with other commands since, if it's run in a
    pipeline, that approach will take more than 1 index in the result

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (bytes): The key to update
        later_than (int, float): the maximum score to remove; all remaining elements
            will have a score strictly greater

    Returns:
        int, None: None if executed in a pipeline since the result is not known until
            the pipeline is executed, otherwise the number of elements remaining in the set

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(ZCARD_EXACT_WINDOW_LUA_SCRIPT_HASH, 1, key, later_than)  # type: ignore
    if res is redis:
        return None
    assert isinstance(res, int)
    return res


async def zcard_exact_window_safe(
    itgs: Itgs, key: bytes, later_than: Union[int, float]
) -> int:
    """Same as zcard_exact_window, but operates on the standard redis instance
    of the given itgs and hence knows it's not executing within a transaction.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (bytes): The key to update
        later_than (int, float): the maximum score to remove; all remaining elements
            will have a score strictly greater

    Returns:
        int: the number of elements remaining in the set
    """
    redis = await itgs.redis()
    async with redis.pipeline() as pipe:
        pipe.multi()
        await pipe.zremrangebyscore(key, "-inf", later_than)
        await pipe.zcard(key)
        res = await pipe.execute()

    return cast(int, res[1])
