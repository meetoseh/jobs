from itgs import Itgs
import hashlib
import time
from redis.exceptions import NoScriptError
import asyncio
import logging

RATELIMIT_USING_REDIS_SCRIPT = """
local key = KEYS[1]
local time_between_requests = tonumber(ARGV[1])
local now = tonumber(ARGV[2])

local last_api_call_at = redis.call("GET", key)
local next_api_call_at = now
if last_api_call_at ~= false then
    last_api_call_at = tonumber(last_api_call_at)
    next_api_call_at = math.max(last_api_call_at + time_between_requests, now)
end

redis.call("SET", key, next_api_call_at)
redis.call("EXPIRE", key, time_between_requests)
return next_api_call_at
"""

RATELIMIT_USING_REDIS_SCRIPT_HASH = hashlib.sha1(
    RATELIMIT_USING_REDIS_SCRIPT.encode("utf-8")
).hexdigest()


async def ratelimit_using_redis(
    itgs: Itgs, *, key: str, time_between_requests: int, sleep_until: bool = True
) -> float:
    """When called with the same key, this will only return after time_between_requests
    seconds have passed since the last call. This uses redis as the underlying source,
    but is functionally similar to:

    ```py
    import time

    _last_api_call_at = None
    def ratelimit_using_global(time_between_requests: int) -> None:
        global _last_api_call_at
        now = time.time()
        if _last_api_call_time is not None:
            time_since_last = now - self._last_api_call_time
            if time_since_last < self.api_delay:
                time.sleep(self.api_delay - time_since_last)
        _last_api_call_time = now
    ```

    This is thread-safe and multiprocess safe.

    Args:
        itgs (Itgs): The integrations to (re)use
        key (str): The key to store the last time the api was called on redis
        time_between_requests (int): The minimum number of seconds between requests, i.e.,
            two function calls to this with the same key will not return within this
            many seconds of each other.
        sleep_until (bool): If True, this will sleep until the next time the api can be
            called. If false, the callee is responsible for the sleep.

    Returns:
        float: The time.time() at which the api can be called again
    """
    redis = await itgs.redis()
    now = time.time()
    try:
        result = await redis.evalsha(  # type: ignore
            RATELIMIT_USING_REDIS_SCRIPT_HASH, 1, key, time_between_requests, now  # type: ignore
        )
    except NoScriptError:
        correct_hash = await redis.script_load(RATELIMIT_USING_REDIS_SCRIPT)
        if correct_hash != RATELIMIT_USING_REDIS_SCRIPT_HASH:
            raise ValueError(
                f"Redis script hash mismatch: expected {RATELIMIT_USING_REDIS_SCRIPT_HASH}, got {correct_hash}"
            )
        now = time.time()
        result = await redis.evalsha(  # type: ignore
            RATELIMIT_USING_REDIS_SCRIPT_HASH, 1, key, time_between_requests, now  # type: ignore
        )

    next_api_call_at = float(result)
    if next_api_call_at > now:
        logging.info(
            f"Distributed ratelimit on {key} triggered for another {next_api_call_at-now:.3f}s"
        )
    if sleep_until and next_api_call_at > now:
        await asyncio.sleep(next_api_call_at - now)
    return next_api_call_at
