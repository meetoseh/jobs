from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

ABANDON_PENDING_SMS_LUA_SCRIPT = """
local src = KEYS[1]
local sid = ARGV[1]
local expected_num_changes = tonumber(ARGV[2])

if redis.call("ZSCORE", src, sid) == false then
    return 0
end

local item_key = src .. ":" .. sid
local item_type = redis.call("TYPE", item_key)['ok']
if item_type ~= 'hash' then
    if item_type ~= 'none' then
        redis.call("DEL", item_key)
    end
    redis.call("ZREM", src, sid)
    return 0
end

local real_num_changes_raw = redis.call("HGET", item_key, "num_changes")
if real_num_changes_raw == false then
    redis.call("DEL", item_key)
    redis.call("ZREM", src, sid)
    return 0
end
if tonumber(real_num_changes_raw) ~= expected_num_changes then
    return 0
end

redis.call("DEL", item_key)
redis.call("ZREM", src, sid)
return 1
"""

ABANDON_PENDING_SMS_LUA_SCRIPT_HASH = hashlib.sha1(
    ABANDON_PENDING_SMS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_abandon_pending_sms_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the abandon_pending_sms lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(ABANDON_PENDING_SMS_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(ABANDON_PENDING_SMS_LUA_SCRIPT)
        assert (
            correct_hash == ABANDON_PENDING_SMS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {ABANDON_PENDING_SMS_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def abandon_pending_sms(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    sid: Union[str, bytes],
    num_changes: int,
) -> bool:
    """Removes the given sid from the receipt pending set, but only if it has the
    given number of changes. Returns if the sid was removed.

    Specifically, `src` should be a sorted set like the receipt pending set, where
    the items are `sid`s of message resources, and for which there exists a key at
    `{src}:{item}` for each item in the set. That key should be a hash with at
    least the `num_changes` key.

    If the sid is in the pending set but the corresponding item does not exist,
    then the sid is removed but this returns false. Further, if the sid is in
    the pending set, and the corresponding item exists, but it is missing the
    `num_changes` key, then the sid is removed, the item is deleted, but this
    still returns false. If the sid exists, the item exists, the item has the
    `num_changes` key, but it doesn't match, this does nothing and returns false.
    Finally, if the sid exists, the item exists, the item has the `num_changes`
    key, and it matches, then the sid is removed, the item is deleted, and this
    returns true.

    Note that if `redis` is a pipeline then the result is not known until the
    pipeline is executed, so this returns None. The pipeline result can be
    interpreted as 0 or b"0" meaning false and 1 or b"1" meaning true.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        sid (Union[str, bytes]): The sid to remove from the pending set
        num_changes (int): The number of changes the item should have in the pending
            set, to confirm it hasn't changed since it was fetched.

    Returns:
        bool: True if the sid was removed properly, false otherwise

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        ABANDON_PENDING_SMS_LUA_SCRIPT_HASH, 1, src, sid, num_changes
    )
    if res is redis:
        return None
    if isinstance(res, (str, bytes)):
        return bool(int(res))
    assert isinstance(res, int)
    return bool(res)


async def abandon_pending_sms_safe(
    itgs: Itgs,
    src: Union[str, bytes],
    sid: Union[str, bytes],
    num_changes: int,
) -> bool:
    """Loads the abandon_pending_sms script if necessary and executes abandon_pending_sms
    within the standard redis instance on the integrations.

    Args:
        itgs (Itgs): The integrations
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        sid (Union[str, bytes]): The sid to remove from the pending set
        num_changes (int): The number of changes the item should have in the pending
            set, to confirm it hasn't changed since it was fetched.

    Returns:
        bool: True if the sid was removed properly, false otherwise
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_abandon_pending_sms_script_exists(redis, force=force)

    async def func():
        return await abandon_pending_sms(redis, src, sid, num_changes)

    return await redis_helpers.run_with_prep.run_with_prep(prep, func)
