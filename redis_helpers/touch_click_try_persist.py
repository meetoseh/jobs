from typing import Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import dataclasses

from lib.touch.link_info import TouchLink

TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT = """
local score = ARGV[1]
local code = ARGV[2]

local already_queued = redis.call("ZSCORE", "touch_links:to_persist", code) ~= false
if already_queued then return {-1, {}} end

local persisting = redis.call("ZSCORE", "touch_links:persist_purgatory", code) ~= false
if persisting then return {-2, {}} end

local link = redis.call("HGETALL", "touch_links:buffer:" .. code)
if #link == 0 then return {-3, {}} end

redis.call("ZADD", "touch_links:to_persist", score, code)
return {1, link}
"""

TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_click_try_persist_ensured_at: Optional[float] = None


async def ensure_touch_click_try_persist_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_click_try_persist lua script is loaded into redis."""
    global _last_touch_click_try_persist_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_click_try_persist_ensured_at is not None
        and (now - _last_touch_click_try_persist_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_click_try_persist_ensured_at is None
        or _last_touch_click_try_persist_ensured_at < now
    ):
        _last_touch_click_try_persist_ensured_at = now


@dataclasses.dataclass
class TouchClickTryPersistResult:
    persist_queued: bool
    """True if we queued persisting the link, false otherwise"""
    failure_reason: Optional[Literal["already_queued", "persisting", "not_in_buffer"]]
    """The reason we didn't queue persisting the link, if we did not, otherwise None"""
    link: Optional[TouchLink]
    """If the persist was queued, the link that will be persisted, otherwise None"""


async def touch_click_try_persist(
    redis: redis.asyncio.client.Redis, score: float, code: Union[str, bytes]
) -> Optional[TouchClickTryPersistResult]:
    """Adds the given code to the touch links persist queue with the given
    score, assuming it's not there, it's not being persisted, and it's in
    the buffered links sorted set.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        score (float): The score to use for the code in the persist queue
        code (Union[str, bytes]): The code to add to the persist queue

    Returns:
        TouchClickTryPersistResult, None: The result. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(TOUCH_CLICK_TRY_PERSIST_LUA_SCRIPT_HASH, 0, score, code)
    if res is redis:
        return None
    return touch_click_try_persist_parse_result(res)


def touch_click_try_persist_parse_result(res) -> TouchClickTryPersistResult:
    """Parses the result of the touch_click_try_persist lua script."""
    assert isinstance(res, list)
    assert len(res) == 2
    assert res[0] in (-3, -2, -1, 1)
    assert isinstance(res[1], (list, tuple))
    if res[0] == -3:
        return TouchClickTryPersistResult(False, "not_in_buffer", None)
    if res[0] == -2:
        return TouchClickTryPersistResult(False, "persisting", None)
    if res[0] == -1:
        return TouchClickTryPersistResult(False, "already_queued", None)
    return TouchClickTryPersistResult(True, None, TouchLink.from_redis_mapping(res[1]))
