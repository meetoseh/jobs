from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

GET_STALE_RECEIPTS_LUA_SCRIPT = """
local src = KEYS[1]
local now = tonumber(ARGV[1])
local bump_score_to = tonumber(ARGV[2])
local max_count = tonumber(ARGV[3])

local res = {}
local count = 0
while count < max_count do
    local sids = redis.call('zrange', src, '-inf', now, 'BYSCORE', 'LIMIT', 0, 1)
    if #sids == 0 then
        break
    end
    count = count + 1

    local sid = sids[1]
    local item_key = src .. ':' .. sid
    local item_type = redis.call('type', item_key)['ok']
    if item_type ~= 'hash' then
        if item_type ~= 'none' then
            redis.call('del', item_key)
        end
        redis.call('zrem', src, sid)
    else
        redis.call('hset', item_key, 'failure_job_last_called_at', now)
        redis.call('hincrby', item_key, 'num_failures', 1)
        redis.call('hincrby', item_key, 'num_changes', 1)
        res[#res + 1] = redis.call('hgetall', item_key)
        redis.call('zadd', src, bump_score_to, sid)
    end
end

return res
"""

GET_STALE_RECEIPTS_LUA_SCRIPT_HASH = hashlib.sha1(
    GET_STALE_RECEIPTS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_get_stale_receipts_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the get_stale_receipts lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(GET_STALE_RECEIPTS_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(GET_STALE_RECEIPTS_LUA_SCRIPT)
        assert (
            correct_hash == GET_STALE_RECEIPTS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {GET_STALE_RECEIPTS_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def get_stale_receipts(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    now: float,
    bump_score_to: float,
    max_count: int,
) -> List[dict]:
    """Fetches stale receipts from the given Receipt Pending Set, updating the score of any
    returned items to the given value.

    Specifically, the `src` must be a sorted set sorted by posix time. Each item in the
    sorted set must be a string, and there must be a key at `{src}:{item}` which is a
    hash with the following keys (at minimum):

    - `failure_job_last_called_at`
    - `num_failures`
    - `num_changes`

    This will set `failure_job_last_called_at` to `now` and increment `num_failures` and
    `num_changes` by 1.

    If this encounters an item within the receipt pending set which doesn't have
    a corresponding key, it will remove the item from the set and continue, counting
    towards the `max_count` but not returning the item.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (str, bytes): The Receipt Pending Set, typically `b"sms:pending"`
        now (float): The current posix time
        bump_score_to (float): The score to set the items to
        max_count (int): The maximum number of items to fetch. Items with a lower
            score will be fetched first.

    Returns:
        List[dict]: The list of items fetched, as they are after the update, with the
            lowest original score first. If `redis` is a pipeline, this returns None.
            The items are returned as if by `hgetall`

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        GET_STALE_RECEIPTS_LUA_SCRIPT_HASH, 1, src, now, bump_score_to, max_count
    )
    if res is redis:
        return None
    return res


async def get_stale_receipts_safe(
    itgs: Itgs,
    src: Union[str, bytes],
    now: float,
    bump_score_to: float,
    max_count: int,
) -> List[dict]:
    """Loads the get stale receipts script if necessary and executes get_stale_receipts
    within the standard redis instance on the integrations.

    Args:
        itgs (Itgs): The integrations
        src (str, bytes): The Receipt Pending Set, typically `b"sms:pending"`
        now (float): The current posix time
        bump_score_to (float): The score to set the items to
        max_count (int): The maximum number of items to fetch. Items with a lower
            score will be fetched first.

    Returns:
        List[dict]: The list of items fetched, as they are after the update, with the
            lowest original score first. If `redis` is a pipeline, this returns None.
            The items are returned as if by `hgetall`
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_get_stale_receipts_script_exists(redis, force=force)

    async def func():
        return await get_stale_receipts(redis, src, now, bump_score_to, max_count)

    return await redis_helpers.run_with_prep.run_with_prep(prep, func)
