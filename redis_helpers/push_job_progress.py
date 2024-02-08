from typing import Optional, List, Tuple, cast
import hashlib
import time
import redis.asyncio.client

PUSH_JOB_PROGRESS_LUA_SCRIPT = """
local uid = ARGV[1]
local evt = ARGV[2]

local events_key = "jobs:progress:events:" .. uid
local count_key = "jobs:progress:count:" .. uid

local new_length = redis.call("RPUSH", events_key, evt)
while new_length > 50 do
    redis.call("LPOP", events_key)
    new_length = new_length - 1
end

local new_count = redis.call("INCR", count_key)
local num_subscribers = redis.call("PUBLISH", "ps:jobs:progress:" .. uid, '[' .. evt .. ']')

redis.call("EXPIRE", events_key, 1800)
redis.call("EXPIRE", count_key, 1800)

return {new_count, num_subscribers}
"""

PUSH_JOB_PROGRESS_LUA_SCRIPT_HASH = hashlib.sha1(
    PUSH_JOB_PROGRESS_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_push_job_progress_ensured_at: Optional[float] = None


async def ensure_push_job_progress_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the push_job_progress lua script is loaded into redis."""
    global _last_push_job_progress_ensured_at

    now = time.time()
    if (
        not force
        and _last_push_job_progress_ensured_at is not None
        and (now - _last_push_job_progress_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(PUSH_JOB_PROGRESS_LUA_SCRIPT_HASH)
    if not loaded[0]:
        correct_hash = await redis.script_load(PUSH_JOB_PROGRESS_LUA_SCRIPT)
        assert (
            correct_hash == PUSH_JOB_PROGRESS_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {PUSH_JOB_PROGRESS_LUA_SCRIPT_HASH=}"

    if (
        _last_push_job_progress_ensured_at is None
        or _last_push_job_progress_ensured_at < now
    ):
        _last_push_job_progress_ensured_at = now


async def push_job_progress(
    redis: redis.asyncio.client.Redis, progress_uid: bytes, event: bytes
) -> Optional[Tuple[int, int]]:
    """Pushes the given job progress message to the progress event list with the
    corresponding uid. This will:

    - Push the event to the right of the list `jobs:progress:events:{uid}`
    - Pop from the left of the list `jobs:progress:events:{uid}` until the length is
      50 or less
    - Increment the count at `jobs:progress:count:{uid}`
    - Publish the event, wrapped in square brackets, to the channel `ps:jobs:progress:{uid}`

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        key (str): The key to update
        val (int): The value to update to

    Returns:
        (int, int), None: The result of the increment and publish commands. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(PUSH_JOB_PROGRESS_LUA_SCRIPT_HASH, 0, progress_uid, event)  # type: ignore
    if res is redis:
        return None
    assert isinstance(res, (list, tuple)), res
    assert len(res) == 2, res
    assert isinstance(res[0], int), res
    assert isinstance(res[1], int), res
    return cast(Tuple[int, int], res)
