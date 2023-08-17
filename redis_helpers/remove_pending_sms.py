from typing import Dict, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

REMOVE_PENDING_SMS_LUA_SCRIPT = """
local src = KEYS[1]
local job_queue = KEYS[2]
local sid = ARGV[1]
local expected_num_changes = tonumber(ARGV[2])
local job = ARGV[3]

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

redis.call("RPUSH", job_queue, job)
redis.call("DEL", item_key)
redis.call("ZREM", src, sid)
return 1
"""

REMOVE_PENDING_SMS_LUA_SCRIPT_HASH = hashlib.sha1(
    REMOVE_PENDING_SMS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_remove_pending_sms_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the remove_pending_sms lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(REMOVE_PENDING_SMS_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(REMOVE_PENDING_SMS_LUA_SCRIPT)
        assert (
            correct_hash == REMOVE_PENDING_SMS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {REMOVE_PENDING_SMS_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def remove_pending_sms(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    job_queue: Union[str, bytes],
    sid: Union[str, bytes],
    expected_num_changes: int,
    job: Union[str, bytes],
) -> bool:
    """If the message resource indicated by the given sid is in the pending set and
    is unchanged (as identified by num_changes), it's removed and the given job is
    enqueued.

    If the message resource is not in the pending set, or has changed, nothing happens.

    If the message resource is in an invalid state, it is cleaned up but this returns False
    anyway.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        job_queue (Union[str, bytes]): The job queue, typically b"jobs:hot"
        sid (Union[str, bytes]): The sid to remove
        expected_num_changes (int): The number of changes the item should have in the pending
          set, to confirm it hasn't changed since it was fetched.
        job (Union[str, bytes]): The job to push to the right of the job queue

    Returns:
        bool: True if the sid was removed and the job enqueued, False otherwise

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        REMOVE_PENDING_SMS_LUA_SCRIPT_HASH,
        2,
        src,
        job_queue,
        sid,
        expected_num_changes,
        job,
    )
    if res is redis:
        return None
    if isinstance(res, (str, bytes)):
        return bool(int(res))
    assert isinstance(res, int)
    return bool(res)


async def remove_pending_sms_safe(
    itgs: Itgs,
    src: Union[str, bytes],
    job_queue: Union[str, bytes],
    sid: Union[str, bytes],
    expected_num_changes: int,
    job: Union[str, bytes],
) -> bool:
    """Loads the remove_pending_sms script if necessary and executes remove_pending_sms
    within the standard redis instance on the integrations.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (Union[str, bytes]): The receipt pending set, typically `b"sms:pending"`
        job_queue (Union[str, bytes]): The job queue, typically b"jobs:hot"
        sid (Union[str, bytes]): The sid to remove
        expected_num_changes (int): The number of changes the item should have in the pending
          set, to confirm it hasn't changed since it was fetched.
        job (Union[str, bytes]): The job to push to the right of the job queue

    Returns:
        bool: True if the sid was removed and the job enqueued, False otherwise
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_remove_pending_sms_script_exists(redis, force=force)

    async def func():
        return await remove_pending_sms(
            redis, src, job_queue, sid, expected_num_changes, job
        )

    return await redis_helpers.run_with_prep.run_with_prep(prep, func)
