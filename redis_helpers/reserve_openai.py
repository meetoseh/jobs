from typing import Any, Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

# Usage: reserve_openai NOW KEY CAP INTERVAL REFILL MIN_BAL AMT [...KEY CAP INTERVAL REFILL MIN_BAL AMT]
# all values in argv
RESERVE_OPENAI_LUA_SCRIPT = """
local now = tonumber(ARGV[1])
local key_count = math.floor((#ARGV - 1) / 6)

local parsed = {}

for i = 1, key_count do
  local offset = (i - 1) * 6 + 1
  local key = ARGV[offset + 1]
  local cap = tonumber(ARGV[offset + 2])
  local interval = tonumber(ARGV[offset + 3])
  local refill = tonumber(ARGV[offset + 4])
  local min_balance = tonumber(ARGV[offset + 5])
  local amt = tonumber(ARGV[offset + 6])
  table.insert(parsed, {key=key, cap=cap, interval=interval, refill=refill, min_balance=min_balance, amt=amt})
end

local cmds = {}
local longest_wait_for_zero = 0
for i, v in ipairs(parsed) do
    local info = redis.call('HMGET', v.key, 'last_refill_time', 'tokens')

    local og_last_refill_time = now
    if info[1] ~= false then
        og_last_refill_time = tonumber(info[1])
    end

    local og_tokens = v.cap
    if info[2] ~= false then
        og_tokens = tonumber(info[2])
    end

    local last_refill_time = og_last_refill_time
    local tokens = og_tokens

    local num_refills = math.floor((now - last_refill_time) / v.interval)
    tokens = math.min(v.cap, tokens + num_refills * v.refill)
    last_refill_time = last_refill_time + num_refills * v.interval

    if tokens - v.amt < v.min_balance then
        return {-1, i, tokens, last_refill_time}
    end

    if tokens - v.amt < 0 then
        local tokens_required = v.amt - tokens
        local refills_until_zero = math.ceil(tokens_required / v.refill)
        local time_until_zero = refills_until_zero * v.interval
        longest_wait_for_zero = math.max(longest_wait_for_zero, time_until_zero)
    end

    local tokens_until_cap = v.cap - tokens
    local refills_until_cap = math.ceil(tokens_until_cap / v.refill)
    local time_until_cap = refills_until_cap * v.interval
    cmds[i] = {key=v.key, last_refill_time=last_refill_time, tokens=tokens - v.amt, expireat=last_refill_time + time_until_cap + v.interval}
end

for i, v in ipairs(cmds) do
    redis.call('HSET', v.key, 'last_refill_time', v.last_refill_time, 'tokens', v.tokens)
    redis.call('EXPIREAT', v.key, v.expireat)
end

return {1, longest_wait_for_zero}
"""

RESERVE_OPENAI_LUA_SCRIPT_HASH = hashlib.sha1(
    RESERVE_OPENAI_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_reserve_openai_ensured_at: Optional[float] = None


async def ensure_reserve_openai_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the reserve_openai lua script is loaded into redis."""
    global _last_reserve_openai_ensured_at

    now = time.time()
    if (
        not force
        and _last_reserve_openai_ensured_at is not None
        and (now - _last_reserve_openai_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(RESERVE_OPENAI_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(RESERVE_OPENAI_LUA_SCRIPT)
        assert (
            correct_hash == RESERVE_OPENAI_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {RESERVE_OPENAI_LUA_SCRIPT_HASH=}"

    if _last_reserve_openai_ensured_at is None or _last_reserve_openai_ensured_at < now:
        _last_reserve_openai_ensured_at = now


@dataclass
class ReserveOpenAIArg:
    key: str
    """The key corresponding to the hash with `last_refill_time` and `tokens`"""
    cap: int
    """The maximum number that can be held at once"""
    interval: int
    """The interval between refills in seconds"""
    refill: int
    """How many tokens are refilled every interval"""
    min_balance: int
    """The minimum balance allowed; it is often convenient to allow this to be negative if
    you are willing to wait a bit, since it means you can reserve the tokens and then make
    your request after the wait
    """
    amt: int
    """How many tokens you want to consume"""


@dataclass
class ReserveOpenAIResultSuccess:
    type: Literal["success"]
    """
    - `success`: none of the reserves when below the minimum balance, so
      tokens were reserved on all keys
    """
    wait_time: int
    """How long you should wait before making the request, in seconds"""


@dataclass
class ReserveOpenAIResultFailure:
    type: Literal["failure"]
    """
    - `failure`: one of the reserves would have gone below the minimum balance.
      No tokens were reserved.
    """
    key_index: int
    """The index of the key in the input list that would have gone below the minimum balance"""
    tokens: int
    """The number of tokens in the key"""
    last_refill_time: int
    """The time at which the last refill occurred"""


ReserveOpenAIResult = Union[ReserveOpenAIResultSuccess, ReserveOpenAIResultFailure]


async def reserve_openai(
    redis: redis.asyncio.client.Redis,
    now: int,
    args: List[ReserveOpenAIArg],
) -> Optional[ReserveOpenAIResult]:
    """Performs the leaky bucket style ratelimiting that can be used to
    simulate openai's ratelimits (or really, most ratelimits). This is
    optimized for reserving tokens when we're acting as the client, though
    it'd work fine with a min_balance of 0 for acting as the server, assuming
    you didn't want to punish ratelimited requests.

    This requires that you already know and keep in sync on all instances the
    ratelimits. If this is being called against the same key with different
    values the result will be unpredictable.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        now (int): the current time in seconds since the epoch
        args (List[ReserveOpenAIArg]): the reservations to make


    Returns:
        ReserveOpenAIResult, None: If the reservation succeeded, how long
            to wait to ensure all buckets would be at zero tokens after the
            reservation. If the reservation failed, the reservation which
            failed. None if executed within a transaction, since the result is
            not known until the transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    redis_argv: List[Any] = [str(now).encode("ascii")]
    for arg in args:
        redis_argv.extend(
            [
                arg.key.encode("utf-8"),
                str(arg.cap).encode("ascii"),
                str(arg.interval).encode("ascii"),
                str(arg.refill).encode("ascii"),
                str(arg.min_balance).encode("ascii"),
                str(arg.amt).encode("ascii"),
            ]
        )

    res = await redis.evalsha(
        RESERVE_OPENAI_LUA_SCRIPT_HASH,
        0,
        *redis_argv,  # type: ignore
    )
    if res is redis:
        return None
    return parse_reserve_openai_result(res)


async def safe_reserve_openai(
    itgs: Itgs,
    /,
    *,
    now: int,
    args: List[ReserveOpenAIArg],
) -> ReserveOpenAIResult:
    """The same as reserve_openai but on the main redis instance and thus this
    does not need an optional result and can handle ensuring the script exists

    Typically you would use the `openai_ratelimits` helper over calling this
    directly.
    """

    redis = await itgs.redis()

    async def _prepare(force: bool) -> None:
        await ensure_reserve_openai_script_exists(redis, force=force)

    async def _execute():
        return await reserve_openai(redis, now, args)

    result = await run_with_prep(_prepare, _execute)
    assert result is not None
    return result


def parse_reserve_openai_result(res: Any) -> ReserveOpenAIResult:
    """Parses the raw result of the reserve openai lua script"""
    assert isinstance(res, (list, tuple)), res
    assert len(res) >= 2, res

    type_ = int(res[0])
    if type_ == 1:
        return ReserveOpenAIResultSuccess(
            type="success",
            wait_time=int(res[1]),
        )

    if type_ == -1:
        return ReserveOpenAIResultFailure(
            type="failure",
            key_index=int(res[1]),
            tokens=int(res[2]),
            last_refill_time=int(res[3]),
        )

    raise ValueError(f"Unknown type: {type_} in {res}")
