from dataclasses import dataclass
from typing import Any, Optional, List
import hashlib
import time
import redis.asyncio.client

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT = """
local now = tonumber(ARGV[1])
local count = tonumber(ARGV[2])

local unconfirmed_zset_key = "journey_share_links:views_unconfirmed"
local views_to_log_key = "journey_share_links:views_to_log"
local view_pseudoset_prefix = "journey_share_links:views:"

local stale_at = now - 60 * 30
local stale_view_ids = redis.call(
    "ZRANGE", 
    unconfirmed_zset_key, 
    "-inf", 
    tostring(stale_at), 
    "BYSCORE",
    "LIMIT", 
    0, 
    count
)

local found = 0
local removed = 0
local queued = 0

for _, view_id in ipairs(stale_view_ids) do
    found = found + 1
    local view_pseudoset_key = view_pseudoset_prefix .. view_id
    local link_uid = redis.call("HGET", view_pseudoset_key, "journey_share_link_uid")
    if link_uid == false or link_uid == "" then
        removed = removed + 1

        redis.call("ZREM", unconfirmed_zset_key, view_id)
        redis.call("DEL", view_pseudoset_key)
    else
        queued = queued + 1

        redis.call("RPUSH", views_to_log_key, view_id)
    end
end

local have_more_work = 0
if #stale_view_ids == count then
    have_more_work = 1
end

return {found, removed, queued, have_more_work}
"""

JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT_HASH = hashlib.sha1(
    JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_journey_share_links_sweep_unconfirmed_ensured_at: Optional[float] = None


async def ensure_journey_share_links_sweep_unconfirmed_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the journey_share_links_sweep_unconfirmed lua script is loaded into redis."""
    global _last_journey_share_links_sweep_unconfirmed_ensured_at

    now = time.time()
    if (
        not force
        and _last_journey_share_links_sweep_unconfirmed_ensured_at is not None
        and (now - _last_journey_share_links_sweep_unconfirmed_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(
            JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT
        )
        assert (
            correct_hash == JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT_HASH=}"

    if (
        _last_journey_share_links_sweep_unconfirmed_ensured_at is None
        or _last_journey_share_links_sweep_unconfirmed_ensured_at < now
    ):
        _last_journey_share_links_sweep_unconfirmed_ensured_at = now


@dataclass
class JourneyShareLinksSweepUnconfirmedResponse:
    found: int
    removed: int
    queued: int
    have_more_work: bool


async def journey_share_links_sweep_unconfirmed(
    redis: redis.asyncio.client.Redis, now: float, count: int
) -> Optional[JourneyShareLinksSweepUnconfirmedResponse]:
    """Sweeps through the unconfirmed views sorted set for stale items,
    checking if they are for a valid code or not. For those which were for
    an invalid code, i.e., which were stored for ratelimiting purposes, they
    are removed from the view pseudo-set and unconfirmed views sorted set.
    For those which were for a valid code, they are added to the view to log
    queue to be persisted.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        now (float): the current time in seconds since the unix epoch
        count (int): the maximum number of items to process

    Returns:
        JourneyShareLinksSweepUnconfirmedResponse, None: What happened. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(JOURNEY_SHARE_LINKS_SWEEP_UNCONFIRMED_LUA_SCRIPT_HASH, 0, now, count)  # type: ignore
    if res is redis:
        return None
    return parse_journey_share_links_sweep_unconfirmed_result(res)


async def journey_share_links_sweep_unconfirmed_safe(
    itgs: Itgs, now: float, count: int
) -> JourneyShareLinksSweepUnconfirmedResponse:
    """Works the same as `journey_share_links_sweep_unconfirmed`, except instead
    of passing a redis client (which might be a pipeline), this uses the main
    redis instance associated with the itgs. Because this is never a pipeline,
    this also manages loading the script

    Args:
        itgs (Itgs): the integration to use
        now (float): the current time in seconds since the unix epoch
        count (int): the maximum number of items to process
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_journey_share_links_sweep_unconfirmed_script_exists(
            redis, force=force
        )

    async def _execute():
        return await journey_share_links_sweep_unconfirmed(redis, now, count)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_journey_share_links_sweep_unconfirmed_result(
    res: Any,
) -> JourneyShareLinksSweepUnconfirmedResponse:
    assert isinstance(res, list), res
    assert len(res) == 4, res
    assert all(isinstance(x, int) for x in res), res
    return JourneyShareLinksSweepUnconfirmedResponse(
        found=res[0],
        removed=res[1],
        queued=res[2],
        have_more_work=bool(res[3]),
    )
