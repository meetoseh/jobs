from dataclasses import dataclass
from typing import Any, Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

MOVE_TO_SET_PURGATORY_LUA_SCRIPT = """
local queue_key = KEYS[1]
local purgatory_set_key = KEYS[2]
local max_to_move = ARGV[1]

local initial_purgatory_size = redis.call("SCARD", purgatory_set_key)
if initial_purgatory_size > 0 then
  return {1, initial_purgatory_size}
end

local to_move = redis.call("LPOP", queue_key, max_to_move)
local num_popped = 0
local num_added = 0
if to_move ~= false then
    for _, val in ipairs(to_move) do
        num_popped = num_popped + 1

        local res = redis.call("SADD", purgatory_set_key, val)
        if res == 1 then
            num_added = num_added + 1
        end
    end
end

return {2, num_popped, num_added}
"""

MOVE_TO_SET_PURGATORY_LUA_SCRIPT_HASH = hashlib.sha1(
    MOVE_TO_SET_PURGATORY_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_move_to_set_purgatory_ensured_at: Optional[float] = None


async def ensure_move_to_set_purgatory_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the move_to_set_purgatory lua script is loaded into redis."""
    global _last_move_to_set_purgatory_ensured_at

    now = time.time()
    if (
        not force
        and _last_move_to_set_purgatory_ensured_at is not None
        and (now - _last_move_to_set_purgatory_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        MOVE_TO_SET_PURGATORY_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(MOVE_TO_SET_PURGATORY_LUA_SCRIPT)
        assert (
            correct_hash == MOVE_TO_SET_PURGATORY_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {MOVE_TO_SET_PURGATORY_LUA_SCRIPT_HASH=}"

    if (
        _last_move_to_set_purgatory_ensured_at is None
        or _last_move_to_set_purgatory_ensured_at < now
    ):
        _last_move_to_set_purgatory_ensured_at = now


@dataclass
class MoveToSetPurgatoryHadItemsInPurgatoryResult:
    category: Literal["had_items_in_purgatory"]
    num_items_in_purgatory: int


@dataclass
class MoveToSetPurgatoryMovedItemsResult:
    category: Literal["moved_items"]
    num_popped: int
    num_added: int


MoveToSetPurgatoryResult = Union[
    MoveToSetPurgatoryHadItemsInPurgatoryResult, MoveToSetPurgatoryMovedItemsResult
]


async def move_to_set_purgatory(
    redis: redis.asyncio.client.Redis,
    queue_key: bytes,
    purgatory_set_key: bytes,
    max_to_move: int,
) -> Optional[MoveToSetPurgatoryResult]:
    """Given a queue key which points to a list and a purgatory set key which
    points to a set, this works as follows:

    - If there are any items in the purgatory set, this does nothing
      and returns how many items are in the purgatory set.
    - Otherwise, this pops up to `max_to_move` items from the queue and
      adds them to the purgatory set. It returns how many items were popped
      and how many were added, which will only differ if there are duplicates
      in the queue.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        queue_key (bytes): The key of the queue to move from
        purgatory_set_key (bytes): The key of the set to move to
        max_to_move (int): The maximum number of items to move

    Returns:
        MoveToSetPurgatoryResult, None: The state and action taken. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(MOVE_TO_SET_PURGATORY_LUA_SCRIPT_HASH, 2, queue_key, purgatory_set_key, max_to_move)  # type: ignore
    if res is redis:
        return None
    return parse_move_to_set_purgatory_result(res)


async def move_to_set_purgatory_safe(
    itgs: Itgs,
    queue_key: bytes,
    purgatory_set_key: bytes,
    max_to_move: int,
) -> MoveToSetPurgatoryResult:
    """Equivalent to move_to_set_purgatory, except operates on the primary
    redis instance associated with the given integrations. Since this is never
    a pipeline, this can also manage ensuring the script exists and retries.
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_move_to_set_purgatory_script_exists(redis, force=force)

    async def _execute():
        return await move_to_set_purgatory(
            redis, queue_key, purgatory_set_key, max_to_move
        )

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_move_to_set_purgatory_result(res: Any) -> MoveToSetPurgatoryResult:
    """Parses the result of the move to set purgatory command
    into the typed result.
    """
    assert isinstance(res, list)
    assert len(res) >= 1
    assert isinstance(res[0], int)

    category = res[0]
    if category == 1:
        assert len(res) == 2
        assert isinstance(res[1], int)
        return MoveToSetPurgatoryHadItemsInPurgatoryResult(
            category="had_items_in_purgatory",
            num_items_in_purgatory=res[1],
        )
    elif category == 2:
        assert len(res) == 3
        assert isinstance(res[1], int)
        assert isinstance(res[2], int)
        return MoveToSetPurgatoryMovedItemsResult(
            category="moved_items",
            num_popped=res[1],
            num_added=res[2],
        )
    raise ValueError(f"Unknown category {category} for {res=}")
