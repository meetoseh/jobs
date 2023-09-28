from typing import Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import dataclasses
from lib.touch.link_info import TouchLink


TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT = """
local code = ARGV[1]
local should_track = ARGV[2] == "1"
local visitor_uid = should_track and ARGV[3] ~= "0" and ARGV[3]
local user_sub = should_track and ARGV[4] ~= "0" and ARGV[4]
local track_type = should_track and ARGV[5]
local parent_uid = should_track and ARGV[6] ~= "0" and ARGV[6]
local clicked_at = should_track and ARGV[7]
local click_uid = should_track and ARGV[8]
local now = should_track and tonumber(ARGV[9])

local link = redis.call("HGETALL", "touch_links:buffer:" .. code)
if #link == 0 then
    link = false
end

if not should_track then
    return {0, link}
end

if link == false and parent_uid == false then
    return {-1, link}
end

local persisting = (
    (link ~= false) 
    and (redis.call("ZSCORE", "touch_links:persist_purgatory", code) ~= false)
)

local found_parent = parent_uid == false
if (not found_parent) and (link ~= false) then
    local result = redis.call(
        "HGET", 
        "touch_links:buffer:on_clicks_by_uid:" .. parent_uid, 
        "has_child"
    )
    if result ~= false and result ~= "0" then
        return {-2, link}
    end
    found_parent = result == "0"
end

if (not found_parent) and ((link == false) or persisting) then
    local parent_track_type = redis.call(
        "HGET",
        "touch_links:delayed_clicks:" .. parent_uid,
        "track_type"
    )

    if parent_track_type == "on_click" then
        local missing_child = redis.call("EXISTS", "touch_links:delayed_clicks:childof:" .. parent_uid) == 0
        if not missing_child then
            return {-2, link}
        end
        found_parent = true
    end
end

if not found_parent then
    return {-3, link}
end

if (link ~= false) and (not persisting) then
    redis.call(
        "RPUSH", 
        "touch_links:buffer:clicks:" .. code, 
        cjson.encode({
            uid = click_uid,
            clicked_at = clicked_at,
            visitor_uid = visitor_uid,
            user_sub = user_sub,
            track_type = track_type,
            parent_uid = parent_uid,
        })
    )
    return {1, link}
end

if (link == false) or persisting then
    redis.call("ZADD", "touch_links:delayed_clicks", tostring(now + 1800), click_uid)
    redis.call(
        "HSET", 
        "touch_links:delayed_clicks:" .. click_uid, 
        "uid", click_uid,
        "link_code", code,
        "track_type", track_type,
        "parent_uid", parent_uid or "",
        "user_sub", user_sub or "",
        "visitor_uid", visitor_uid or "",
        "clicked_at", tostring(clicked_at)
    )
    redis.call("SET", "touch_links:delayed_clicks:childof:" .. parent_uid, click_uid)
    return {2, link}
end

return {-4, link}
"""

TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_click_try_create_ensured_at: Optional[float] = None


async def ensure_touch_click_try_create_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_click_try_create lua script is loaded into redis."""
    global _last_touch_click_try_create_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_click_try_create_ensured_at is not None
        and (now - _last_touch_click_try_create_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_click_try_create_ensured_at is None
        or _last_touch_click_try_create_ensured_at < now
    ):
        _last_touch_click_try_create_ensured_at = now


@dataclasses.dataclass
class ClickLinkRedisResult:
    tracked: bool
    """
    True if the click was tracked via either the Buffered Link Clicks
    pseudo-set or the Delayed Link Clicks sorted set, False if the click
    was not tracked. This can be true even if the link was not in the Buffered
    Link sorted set and hence could not be returned.
    """

    tracked_in_buffer: bool
    """True iff tracked and the track was saved in the Buffered Link Clicks pseudo-set"""

    tracked_in_delayed: bool
    """True iff tracked and the track was saved in the Delayed Link Clicks sorted set"""

    failed_to_track_reason: Optional[
        Literal[
            "not_requested",
            "no_link_and_not_post_login",
            "parent_has_child",
            "no_parent",
            "implementation_error",
        ]
    ]
    """The reason we didn't track, one of:

    - `not_requested`: `should_track` was False
    - `no_link_and_not_post_login`: `should_track` was True, but the link was not in
        the buffered link set (meaning that, if it is anywhere, it is in the database)
        and there was no parent for the click, meaning it can be inserted directly into
        the database so long as the link is there. hence it should be inserted in the
        database directly.
    - `parent_has_child`: `should_track` was True, the link was in the buffered link set,
        the track type was `post_login`, the parent was found in either the Buffered Clicks 
        pseudo-set or the Delayed Clicks sorted set, but the parent already had a child
    - `no_parent`: `should_track` was True, and the link was in the buffered link set,
        meaning all the clicks for it will be in either the Buffered Clicks pseudo-set or the
        Delayed Clicks sorted set, but the referenced parent click was not found in either.
        This means that post_login click had no corresponding on_click click.
    - `implementation_error`: entered a part of the code that should be unreachable
    """

    link: Optional[TouchLink]
    """The link with that code, if it was found in the Buffered Link sorted set"""


async def touch_click_try_create(
    redis: redis.asyncio.client.Redis,
    *,
    code: str,
    visitor_uid: Optional[str],
    user_sub: Optional[str],
    track_type: Literal["on_click", "post_login"],
    parent_uid: Optional[str],
    clicked_at: float,
    click_uid: Optional[str],
    now: float,
    should_track: bool,
) -> ClickLinkRedisResult:
    """Attempts to fetch the touch link with the given code from the buffered link
    sorted set, and optionally tries to track the click.

    NOTE:
        Since so many keys are touched this hardcodes them for simplicity and to
        reduce network overhead.

    NOTE:
        This does not increment stats; it wouldn't be possible to do so without
        being able to convert unix timestamps to unix dates in a particular
        timezone within redis.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        code (str): The link code that was clicked
        visitor_uid (str, None): If the client provided a visitor uid, the uid of the
            visitor who clicked the link
        user_sub (str, None): If the client authorized as a user, the sub of the user
            who clicked the link
        track_type (str): The type of track that was sent to the server; on_click means
            that it was sent as part of the client directing the user to the links
            destination, post_login means that it was sent after the fact to augment
            the user sub of the original click now that the user logged in
        parent_uid (str, None): Iff track_type is post_login, the uid of the on_click
            click that it is augmenting, otherwise None
        clicked_at (float): The canonical time the click occurred, in unix
            seconds since the unix epoch
        click_uid (str, None): iff should_track is True, the uid to assign to the
            click, otherwise None
        should_track (bool): Whether or not the click should be tracked. This can
            be used to ratelimiting storage of clicks, since it's generally unfeasible
            to ratelimit queries (as it would break links), or because the data is
            being fetched for e.g. a preview
        now (float): the current time, in unix seconds since the unix epoch, used
            to calculate the score in `touch_links:delayed_clicks` if the click
            is tracked there

    Returns:
        ClickLinkRedisResult, None: What happened. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    if should_track:
        assert visitor_uid != "0", "reserved visitor uid"
        assert user_sub != "0", "reserved user sub"
        assert parent_uid != "0", "reserved parent uid"

        res = await redis.evalsha(
            TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT_HASH,
            0,
            code.encode("utf-8"),
            b"1",
            visitor_uid.encode("utf-8") if visitor_uid is not None else b"0",
            user_sub.encode("utf-8") if user_sub is not None else b"0",
            track_type.encode("ascii"),
            parent_uid.encode("utf-8") if parent_uid is not None else b"0",
            str(clicked_at).encode("ascii"),
            click_uid.encode("utf-8"),
            str(now).encode("ascii"),
        )
    else:
        res = await redis.evalsha(
            TOUCH_CLICK_TRY_CREATE_LUA_SCRIPT_HASH, 0, code.encode("utf-8"), b"0"
        )
    if res is redis:
        return None
    return touch_click_try_create_parse_result(res)


def touch_click_try_create_parse_result(res) -> ClickLinkRedisResult:
    """Parse the result of touch_click_try_create into a ClickLinkRedisResult."""
    assert isinstance(res, list)
    assert len(res) == 2
    assert isinstance(res[0], int)
    assert -4 <= res[0] <= 2
    assert isinstance(res[1], (list, type(None)))

    tracked = res[0] > 0
    tracked_in_buffer = res[0] == 1
    tracked_in_delayed = res[0] == 2
    failed_to_track_reason = None
    if not tracked:
        failed_to_track_reason = [
            "not_requested",
            "no_link_and_not_post_login",
            "parent_has_child",
            "no_parent",
            "implementation_error",
        ][-res[0]]
    link = None if res[1] is None else TouchLink.from_redis_mapping(res[1])
    return ClickLinkRedisResult(
        tracked=tracked,
        tracked_in_buffer=tracked_in_buffer,
        tracked_in_delayed=tracked_in_delayed,
        failed_to_track_reason=failed_to_track_reason,
        link=link,
    )
