from typing import Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

ZADD_EXACT_WINDOW_LUA_SCRIPT = """
local key = KEYS[1]
local counter_key = ARGV[1]
local event_at = tonumber(ARGV[2])

local next_identifier = redis.call("INCR", counter_key)
if next_identifier >= 9007199254740991 then
    redis.call("SET", counter_key, 0)
end

redis.call("ZADD", key, event_at, next_identifier)
return { ok = 'OK' }
"""

ZADD_EXACT_WINDOW_LUA_SCRIPT_HASH = hashlib.sha1(
    ZADD_EXACT_WINDOW_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_zadd_exact_window_ensured_at: Optional[float] = None


async def ensure_zadd_exact_window_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the zadd_exact_window lua script is loaded into redis."""
    global _last_zadd_exact_window_ensured_at

    now = time.time()
    if (
        not force
        and _last_zadd_exact_window_ensured_at is not None
        and (now - _last_zadd_exact_window_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(ZADD_EXACT_WINDOW_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(ZADD_EXACT_WINDOW_LUA_SCRIPT)
        assert (
            correct_hash == ZADD_EXACT_WINDOW_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {ZADD_EXACT_WINDOW_LUA_SCRIPT_HASH=}"

    if (
        _last_zadd_exact_window_ensured_at is None
        or _last_zadd_exact_window_ensured_at < now
    ):
        _last_zadd_exact_window_ensured_at = now


async def zadd_exact_window(
    redis: redis.asyncio.client.Redis,
    key: bytes,
    counter_key: bytes,
    event_at: Union[int, float],
) -> Optional[Literal["OK"]]:
    """Marks that the given event occurred at the given time in the sorted set at `key`,
    where identifiers are monotonically increasing integers where the last used identifier
    is stored at `counter_key`.

    Example:

    ```
    # redis data
    # counter_key = 3
    # key = {1: 100, 2: 200, 3: 300}
    await zadd_exact_window(redis, b"key", b"counter_key", 400)
    # redis data
    # counter_key = 4
    # key = {1: 100, 2: 200, 3: 300, 4: 400}
    ```

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (bytes): The key to update
        counter_key (bytes): The key to update
        event_at (Union[int, float]): The time the event occurred

    Returns:
        "OK", None: None if executed within a transaction, since the result is not
            known until the transaction is executed. Otherwise, always the string "OK"

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(ZADD_EXACT_WINDOW_LUA_SCRIPT_HASH, 1, key, counter_key, str(event_at).encode("ascii"))  # type: ignore
    if res is redis:
        return None
    assert res == "OK" or res == b"OK"
    return "OK"


async def zadd_exact_window_safe(
    itgs: Itgs, key: bytes, counter_key: bytes, event_at: Union[int, float]
) -> Literal["OK"]:
    """Same as zadd_exact_window, but operates on the standard redis instance
    of the given itgs and handles loading the script if necessary

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (bytes): The key to update
        counter_key (bytes): The key to update
        event_at (Union[int, float]): The time the event occurred

    Returns:
        "OK"
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_zadd_exact_window_script_exists(redis, force=force)

    async def _execute():
        return await zadd_exact_window(redis, key, counter_key, event_at)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res
