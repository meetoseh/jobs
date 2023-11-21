from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

RETRY_PENDING_SMS_LUA_SCRIPT = """
local src = KEYS[1]
local dst = KEYS[2]
local sid = ARGV[1]
local expected_num_changes = tonumber(ARGV[2])

if redis.call("ZSCORE", src, sid) == false then
    return 0
end

local item_key = src .. ":" .. sid
local item_type = redis.call("TYPE", item_key)['ok']
if item_type ~= 'hash' then
    redis.call("ZREM", src, sid)
    if item_type ~= 'none' then
        redis.call("DEL", item_key)
    end
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

redis.call("RPUSH", dst, sid)
return 1
"""

RETRY_PENDING_SMS_LUA_SCRIPT_HASH = hashlib.sha1(
    RETRY_PENDING_SMS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_retry_pending_sms_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the retry_pending_sms lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(RETRY_PENDING_SMS_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(RETRY_PENDING_SMS_LUA_SCRIPT)
        assert (
            correct_hash == RETRY_PENDING_SMS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {RETRY_PENDING_SMS_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def retry_pending_sms(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    dst: Union[str, bytes],
    sid: Union[str, bytes],
    num_changes: int,
) -> Optional[bool]:
    """Appends the given sid to the receipt recovery queue, but only if it has the
    given number of changes in the receipt pending set. Returns if the sid was
    appended.

    Specifically, `src` should be a sorted set like the receipt pending set, where
    the items are `sid`s of message resources, and for which there exists a key at
    `{src}:{item}` for each item in the set. That key should be a hash with at
    least the `num_changes` key.

    If the sid is not in the pending set, or the item does not exist, or the items
    `num_changes` is not equal to `num_changes`, then the sid is not pushed to
    the right of the recovery queue (`dst`) and this returns False. Otherwise,
    the sid is pushed to the right of the recovery queue and this returns True.

    Note that if `redis` is a pipeline then the result is not known until the
    pipeline is executed, so this returns None. The pipeline result can be
    interpreted as 0 or b"0" meaning false and 1 or b"1" meaning true.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        dst (Union[str, bytes]): The receipt recovery queue, typically `b"sms:recovery"`
        sid (Union[str, bytes]): The sid to append to the recovery queue
        num_changes (int): The number of changes the item should have in the pending
            set, to confirm it hasn't changed since it was fetched.

    Returns:
        bool: True if the sid was appended to the recovery queue, false otherwise

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(  # type: ignore
        RETRY_PENDING_SMS_LUA_SCRIPT_HASH, 2, src, dst, sid, num_changes  # type: ignore
    )
    if res is redis:
        return None
    if isinstance(res, (str, bytes)):
        return bool(int(res))
    assert isinstance(res, int)
    return bool(res)


async def retry_pending_sms_safe(
    itgs: Itgs,
    src: Union[str, bytes],
    dst: Union[str, bytes],
    sid: Union[str, bytes],
    num_changes: int,
) -> bool:
    """Loads the retry_pending_sms script if necessary and executes retry_pending_sms
    within the standard redis instance on the integrations.

    Args:
        itgs (Itgs): The integrations
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        dst (Union[str, bytes]): The receipt recovery queue, typically `b"sms:recovery"`
        sid (Union[str, bytes]): The sid to append to the recovery queue
        num_changes (int): The number of changes the item should have in the pending
            set, to confirm it hasn't changed since it was fetched.

    Returns:
        bool: True if the sid was appended to the recovery queue, false otherwise
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_retry_pending_sms_script_exists(redis, force=force)

    async def func():
        return await retry_pending_sms(redis, src, dst, sid, num_changes)

    res = await redis_helpers.run_with_prep.run_with_prep(prep, func)
    assert res is not None
    return res
