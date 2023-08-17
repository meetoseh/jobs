from typing import Dict, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

UPDATE_PENDING_SMS_LUA_SCRIPT = """
local src = KEYS[1]
local sid = ARGV[1]
local expected_num_changes = tonumber(ARGV[2])

local num_change_args = #ARGV - 2
if num_change_args == 0 or num_change_args % 2 ~= 0 then
    return redis.error_reply('ERR wrong number of arguments')
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

local hset_args = {'HSET', item_key}
for i = 3, #ARGV do
    table.insert(hset_args, ARGV[i])
end

redis.call(unpack(hset_args))
return 1
"""

UPDATE_PENDING_SMS_LUA_SCRIPT_HASH = hashlib.sha1(
    UPDATE_PENDING_SMS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_update_pending_sms_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the update_pending_sms lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(UPDATE_PENDING_SMS_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(UPDATE_PENDING_SMS_LUA_SCRIPT)
        assert (
            correct_hash == UPDATE_PENDING_SMS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {UPDATE_PENDING_SMS_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def update_pending_sms(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    sid: Union[str, bytes],
    expected_num_changes: int,
    delta: Dict[Union[str, bytes], Union[str, bytes]],
) -> bool:
    """Updates the given sid within the pending sms set, but only if it exists in
    the pending set, the item exists and is a hash, and the number of changes on
    the item matches the expected number of changes.

    If the item is invalid it is deleted and removed from the pending set.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        sid (Union[str, bytes]): The sid to update
        expected_num_changes (int): The number of changes the item should have in the pending
          set, to confirm it hasn't changed since it was fetched.
        delta (Dict[Union[str, bytes], Union[str, bytes]]): The changes to apply to the item

    Returns:
        bool: True if the changes were applied, false otherwise

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    flattened_delta: List[Union[str, bytes]] = []
    for k, v in delta.items():
        flattened_delta.append(k)
        flattened_delta.append(v)

    res = await redis.evalsha(
        UPDATE_PENDING_SMS_LUA_SCRIPT_HASH,
        1,
        src,
        sid,
        expected_num_changes,
        *flattened_delta,
    )

    if res is redis:
        return None
    if isinstance(res, (str, bytes)):
        return bool(int(res))
    assert isinstance(res, int)
    return bool(res)


async def update_pending_sms_safe(
    itgs: Itgs,
    src: Union[str, bytes],
    sid: Union[str, bytes],
    expected_num_changes: int,
    delta: Dict[Union[str, bytes], Union[str, bytes]],
) -> bool:
    """Loads the update_pending_sms script if necessary and executes update_pending_sms
    within the standard redis instance on the integrations.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        sid (Union[str, bytes]): The sid to update
        expected_num_changes (int): The number of changes the item should have in the pending
          set, to confirm it hasn't changed since it was fetched.
        delta (Dict[Union[str, bytes], Union[str, bytes]]): The changes to apply to the item

    Returns:
        bool: True if the changes were applied, false otherwise
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_update_pending_sms_script_exists(redis, force=force)

    async def func():
        return await update_pending_sms(redis, src, sid, expected_num_changes, delta)

    return await redis_helpers.run_with_prep.run_with_prep(prep, func)
