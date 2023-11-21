from typing import Optional, List, Union
import hashlib
import time
import redis.asyncio.client
import redis_helpers.run_with_prep
from itgs import Itgs

LMOVE_USING_PURGATORY_LUA_SCRIPT = """
local src = KEYS[1]
local dst = KEYS[2]

local existing_item = redis.call("LINDEX", dst, 0)
if existing_item ~= false then
  return existing_item
end

return redis.call("LMOVE", src, dst, "LEFT", "RIGHT")
"""

LMOVE_USING_PURGATORY_LUA_SCRIPT_HASH = hashlib.sha1(
    LMOVE_USING_PURGATORY_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_ensured_at: Optional[float] = None


async def ensure_lmove_using_purgatory_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the lmove_using_purgatory lua script is loaded into redis."""
    global _ensured_at

    now = time.time()
    if not force and _ensured_at is not None and (now - _ensured_at < 5):
        return

    loaded: List[bool] = await redis.script_exists(
        LMOVE_USING_PURGATORY_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(LMOVE_USING_PURGATORY_LUA_SCRIPT)
        assert (
            correct_hash == LMOVE_USING_PURGATORY_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {LMOVE_USING_PURGATORY_LUA_SCRIPT_HASH=}"

    if _ensured_at is None or _ensured_at < now:
        _ensured_at = now


async def lmove_using_purgatory(
    redis: redis.asyncio.client.Redis,
    src: Union[str, bytes],
    dst: Union[str, bytes],
) -> Optional[Union[str, bytes]]:
    """Both `src` and `dst` should refer to redis lists, where `src` is acting
    as the main list and `dst` is acting as the purgatory list. If the destination
    is not empty, returns the item at index 0. Otherwise, returns the result of
    `LMOVE src dst LEFT RIGHT`.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (str, bytes): the main list
        dst (str, bytes): the purgatory list

    Returns:
        str, bytes, or None: The item at index 0 of `dst` if it is not empty,
            otherwise the result of `LMOVE src dst LEFT RIGHT`

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(LMOVE_USING_PURGATORY_LUA_SCRIPT_HASH, 2, src, dst)  # type: ignore
    if res is redis:
        return None
    return res


async def lmove_using_purgatory_safe(
    itgs: Itgs,
    src: Union[str, bytes],
    dst: Union[str, bytes],
) -> Union[str, bytes, None]:
    """Loads the lmove_using_purgatory script if necessary and executes lmove_using_purgatory
    within the standard redis instance on the integrations.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        src (str, bytes): the main list
        dst (str, bytes): the purgatory list

    Returns:
        str, bytes, or None: The item at index 0 of `dst` if it is not empty,
            otherwise the result of `LMOVE src dst LEFT RIGHT`
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_lmove_using_purgatory_script_exists(redis, force=force)

    async def func():
        return await lmove_using_purgatory(redis, src, dst)

    res = await redis_helpers.run_with_prep.run_with_prep(prep, func)
    return res
