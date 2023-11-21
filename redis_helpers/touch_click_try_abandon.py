from typing import Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import dataclasses

from lib.touch.link_info import TouchLink

TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT = """
local code = ARGV[1]

local persisting = redis.call("ZSCORE", "touch_links:persist_purgatory", code) ~= false
if persisting then return { 2, {}, -1 } end

redis.call("ZREM", "touch_links:buffer", code)
local link = redis.call("HGETALL", "touch_links:buffer:" .. code)
redis.call("DEL", "touch_links:buffer:" .. code)

local clicks = redis.call("LRANGE", "touch_links:buffer:clicks:" .. code, 0, -1)
redis.call("DEL", "touch_links:buffer:clicks:" .. code)

for _, click in ipairs(clicks) do
    redis.call("DEL", "touch_links:buffer:on_clicks_by_uid:" .. click)
end

redis.call("ZREM", "touch_links:to_persist", code)
if #link == 0 then return { 1, {}, #clicks } end
return { 0, link, #clicks }
"""

TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_click_try_abandon_ensured_at: Optional[float] = None


async def ensure_touch_click_try_abandon_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_click_try_abandon lua script is loaded into redis."""
    global _last_touch_click_try_abandon_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_click_try_abandon_ensured_at is not None
        and (now - _last_touch_click_try_abandon_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_click_try_abandon_ensured_at is None
        or _last_touch_click_try_abandon_ensured_at < now
    ):
        _last_touch_click_try_abandon_ensured_at = now


@dataclasses.dataclass
class TouchClickTryAbandonResult:
    abandoned_link: Optional[TouchLink]
    """The link we abandoned, if we abandoned one"""
    number_of_clicks: int
    """The number of associated clicks for the abandoned link; -1 for already_persisting"""
    failure_reason: Optional[Literal["dne", "already_persisting"]]
    """The reason we could not abandon the link"""


async def touch_click_try_abandon(
    redis: redis.asyncio.client.Redis, code: Union[str, bytes]
) -> Optional[TouchClickTryAbandonResult]:
    """Abandons the link with the given code, if it's in the buffered links sorted
    set and not in the persist purgatory, cleaning up related keys.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        code (str, bytes): the code of the link to abandon

    Returns:
        TouchClickTryAbandonResult, None: The result. None if executed within a
            transaction, since the result is not known until the transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(TOUCH_CLICK_TRY_ABANDON_LUA_SCRIPT_HASH, 0, code)  # type: ignore
    if res is redis:
        return None
    return touch_click_try_abandon_parse_result(res)


def touch_click_try_abandon_parse_result(res) -> TouchClickTryAbandonResult:
    assert isinstance(res, list)
    assert len(res) == 3
    assert res[0] in (0, 1, 2)
    assert isinstance(res[1], list)
    assert isinstance(res[2], int)
    if res[0] == 0:
        assert res[1]
        return TouchClickTryAbandonResult(
            abandoned_link=TouchLink.from_redis_mapping(res[1]),
            number_of_clicks=res[2],
            failure_reason=None,
        )
    elif res[0] == 1:
        assert not res[1]
        return TouchClickTryAbandonResult(
            abandoned_link=None,
            number_of_clicks=res[2],
            failure_reason="dne",
        )
    elif res[0] == 2:
        assert not res[1]
        return TouchClickTryAbandonResult(
            abandoned_link=None,
            number_of_clicks=res[2],
            failure_reason="already_persisting",
        )
    assert False
