from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client

TOUCH_LINK_TRY_CREATE_LUA_SCRIPT = """
local buffer_key = KEYS[1]
local stats_key = KEYS[2]
local stats_earliest_key = KEYS[3]

local uid = ARGV[1]
local code = ARGV[2]
local touch_uid = ARGV[3]
local page_identifier = ARGV[4]
local page_extra = ARGV[5]
local preview_identifier = ARGV[6]
local preview_extra = ARGV[7]
local created_at = ARGV[8]
local already_incremented_stats = tonumber(ARGV[9]) == 1
local unix_date = tonumber(ARGV[10])

local duplicate_code = redis.call("ZSCORE", buffer_key, code)
if duplicate_code ~= false then
    return 0
end

redis.call("ZADD", buffer_key, created_at, code)
redis.call(
    "HSET", 
    buffer_key .. ":" .. code, 
    "uid", uid, 
    "code", code,
    "touch_uid", touch_uid,
    "page_identifier", page_identifier,
    "page_extra", page_extra,
    "preview_identifier", preview_identifier,
    "preview_extra", preview_extra,
    "created_at", created_at
)

if not already_incremented_stats then
    local old_earliest = redis.call("GET", stats_earliest_key)
    if old_earliest == false or tonumber(old_earliest) > unix_date then
        redis.call("SET", stats_earliest_key, unix_date)
    end
    redis.call("HINCRBY", stats_key, "created", 1)
end

return 1
"""

TOUCH_LINK_TRY_CREATE_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_LINK_TRY_CREATE_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_link_try_create_ensured_at: Optional[float] = None


async def ensure_touch_link_try_create_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_link_try_create lua script is loaded into redis."""
    global _last_touch_link_try_create_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_link_try_create_ensured_at is not None
        and (now - _last_touch_link_try_create_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_LINK_TRY_CREATE_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_LINK_TRY_CREATE_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_LINK_TRY_CREATE_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_LINK_TRY_CREATE_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_link_try_create_ensured_at is None
        or _last_touch_link_try_create_ensured_at < now
    ):
        _last_touch_link_try_create_ensured_at = now


async def touch_link_try_create(
    redis: redis.asyncio.client.Redis,
    *,
    buffer_key: Union[str, bytes],
    stats_key: Union[str, bytes],
    stats_earliest_key: Union[str, bytes],
    uid: Union[str, bytes],
    code: Union[str, bytes],
    touch_uid: Union[str, bytes],
    page_identifier: Union[str, bytes],
    page_extra: Union[str, bytes],
    preview_identifier: Union[str, bytes],
    preview_extra: Union[str, bytes],
    created_at: Union[str, bytes, float],
    already_incremented_stats: bool,
    unix_date: int,
) -> Optional[bool]:
    """If the given code is not already in the buffer, adds it to the buffer
    and sets the related hash key (`{buffer_key}:{code}`) to the specified values.
    If the code is inserted and already_incremented_stats is False, increments
    the created event within the related stats hash key (`{stats_key}`) and sets
    the value within the stats earliest key to the lower of its current value or
    the specified unix_date.

    This is intended to only be used if trying to generate ultra short codes
    that have non-negligible chances of collisions. Otherwise, this doesn't need
    a special script.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        buffer_key (Union[str, bytes]): The sorted set acting to store in, typically
            `touch_links:buffer`
        stats_key (Union[str, bytes]): The hash key acting for where to increment
            the created stat, typically `stats:touch_links:daily:{unix_date}`
        stats_earliest_key (Union[str, bytes]): The key acting for where to store
            the earliest unix date that might still have stats in the database,
            typically `stats:touch_links:daily:earliest`
        uid (Union[str, bytes]): The uid of the touch link we are trying to store
        code (Union[str, bytes]): The code of the touch link we are trying to store,
            if it's not already in the buffer
        touch_uid (Union[str, bytes]): The uid of the touch that the link will be sent
            in
        page_identifier (Union[str, bytes]): see TouchLink
        page_extra (Union[str, bytes]): see TouchLink
        preview_identifier (Union[str, bytes]): see TouchLink
        preview_extra (Union[str, bytes]): see TouchLink
        created_at (float): the score to use for the link in the buffer, for leak
            detection
        already_incremented_stats (bool): True if the stats have already been
            incremented, False if not. This is False on the first attempt, and then
            if the redis zadd succeeds but double-checking the database finds a
            collision, True on consecutive attempts.
        unix_date (int): The unix date to use for the earliest key

    Returns:
        bool, None: True if the link was added, False if not. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(  # type: ignore
        TOUCH_LINK_TRY_CREATE_LUA_SCRIPT_HASH,  # type: ignore
        3,
        buffer_key,  # type: ignore
        stats_key,  # type: ignore
        stats_earliest_key,  # type: ignore
        uid,  # type: ignore
        code,  # type: ignore
        touch_uid,  # type: ignore
        page_identifier,  # type: ignore
        page_extra,  # type: ignore
        preview_identifier,  # type: ignore
        preview_extra,  # type: ignore
        created_at,  # type: ignore
        int(already_incremented_stats),  # type: ignore
        unix_date,  # type: ignore
    )
    if res is redis:
        return None
    return bool(res)
