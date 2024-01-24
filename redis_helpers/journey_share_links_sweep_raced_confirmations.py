from typing import Any, Optional, List
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT = """
local cursor = ARGV[1]
local count = ARGV[2]
local now = ARGV[3]

local raced_confirmations_hash_key = "journey_share_links:views_to_confirm"
local view_log_purgatory_set_key = "journey_share_links:views_log_purgatory"
local min_confirmed_at = now - 15

local result = {}

local scan_result = redis.call("HSCAN", raced_confirmations_hash_key, cursor, "COUNT", count)
cursor = scan_result[1]
local view_ids = scan_result[2]

for _, view_id in ipairs(view_ids) do
    local sis_member_result = redis.call("SISMEMBER", view_log_purgatory_set_key, view_id)
    if sis_member_result == 0 then
        local info = redis.call("HGET", raced_confirmations_hash_key, view_id)
        local parsed_info = cjson.decode(info)

        if parsed_info.confirmed_at >= min_confirmed_at then
            table.insert(result, info)
        end
    end
end

return {cursor, #view_ids, result}
"""

JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT_HASH = hashlib.sha1(
    JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_journey_share_links_sweep_raced_confirmations_ensured_at: Optional[float] = None


async def ensure_journey_share_links_sweep_raced_confirmations_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the journey_share_links_sweep_raced_confirmations lua script is loaded into redis."""
    global _last_journey_share_links_sweep_raced_confirmations_ensured_at

    now = time.time()
    if (
        not force
        and _last_journey_share_links_sweep_raced_confirmations_ensured_at is not None
        and (now - _last_journey_share_links_sweep_raced_confirmations_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(
            JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT
        )
        assert (
            correct_hash
            == JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT_HASH=}"

    if (
        _last_journey_share_links_sweep_raced_confirmations_ensured_at is None
        or _last_journey_share_links_sweep_raced_confirmations_ensured_at < now
    ):
        _last_journey_share_links_sweep_raced_confirmations_ensured_at = now


@dataclass
class JourneyShareLinksSweepRacedConfirmationsResult:
    cursor: int
    """The new cursor value. Iteration is finished when this is zero."""

    found: int
    """How many items were returned from HSCAN; `found - len(result)` is the number
    that were skipped beause they were in purgatory or very recent. May be zero, which 
    does not indicate that there are no more items to scan.
    """

    result: List[bytes]
    """The values of the keys returned from HSCAN which were not in the log view purgatory.
    May be empty, which does not indicate that there are no more items to scan.
    """


async def journey_share_links_sweep_raced_confirmations(
    redis: redis.asyncio.client.Redis, cursor: int, count: int, now: float
) -> Optional[JourneyShareLinksSweepRacedConfirmationsResult]:
    """Updates the value in the given key to the given value iff
    the key is unset or the value is lower than the current value.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        cursor (int): The iteration state; should start at zero, and then updated
            with the returned value until it is zero to complete the iteration.
        count (int): Approximately how much work to do. May do slightly more or
            less. Corresponds, roughly, to the maximum length of the returned
            list. For more information about when this may be exceeded, see
            COUNT in https://redis.io/commands/scan
        now (float): Current time in seconds since the epoch

    Returns:
        JourneyShareLinksSweepRacedConfirmationsResult, None: None if executed
            within a transaction, since the result is not known until the
            transaction is executed. Otherwise, the result of the operation.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(JOURNEY_SHARE_LINKS_SWEEP_RACED_CONFIRMATIONS_LUA_SCRIPT_HASH, 0, cursor, count, now)  # type: ignore
    if res is redis:
        return None
    return parse_journey_share_links_sweep_raced_confirmations_result(res)


async def journey_share_links_sweep_raced_confirmations_safe(
    itgs: Itgs, cursor: int, count: int, now: float
) -> JourneyShareLinksSweepRacedConfirmationsResult:
    """The same as journey_share_links_sweep_raced_confirmations, but rather
    than taking a redis client, which may be a pipeline, this uses the primary
    redis instance associated with the integrations. Since this is not a pipeline,
    this can manage loading the script and ensuring a return value.

    Args:
        itgs (Itgs): The integration to use
        cursor (int): The iteration state; should start at zero, and then updated
            with the returned value until it is zero to complete the iteration.
        count (int): Approximately how much work to do. May do slightly more or
            less. Corresponds, roughly, to the maximum length of the returned
            list. For more information about when this may be exceeded, see
            COUNT in https://redis.io/commands/scan
        now (float): Current time in seconds since the epoch
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_journey_share_links_sweep_raced_confirmations_script_exists(
            redis, force=force
        )

    async def _execute():
        return await journey_share_links_sweep_raced_confirmations(
            redis, cursor, count, now
        )

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_journey_share_links_sweep_raced_confirmations_result(
    res: Any,
) -> JourneyShareLinksSweepRacedConfirmationsResult:
    """Parses the result of the journey share links sweep raced confirmations script
    into a more useful format. This also guards against some type errors
    """
    assert isinstance(res, list), res
    assert len(res) == 3, res
    assert isinstance(res[0], bytes), res
    assert isinstance(res[1], int), res
    assert isinstance(res[2], list), res

    # it'd be more accurate to check all items, but for performance we'll
    # just check the first has the right type
    assert len(res[2]) == 0 or isinstance(res[2][0], bytes), res

    return JourneyShareLinksSweepRacedConfirmationsResult(
        cursor=int(res[0]),
        found=res[1],
        result=res[2],
    )
