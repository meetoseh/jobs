import time
from typing import Any, Coroutine, Tuple, cast, Dict, Optional, Protocol
import redis.asyncio.client


_last_checked_at_hash_to_time: Dict[str, float] = dict()


async def ensure_script_exists(
    redis: redis.asyncio.client.Redis,
    /,
    *,
    script: str,
    script_hash: str,
    check_every_s: float = 5.0,
    now: Optional[float] = None,
    force: bool = False,
) -> None:
    """Non-pure function that verifies the given script is available in redis. This will
    do nothing if called within the last `check_every_s` seconds for the same script hash.
    Otherwise, it will check if the script exists with SCRIPT EXISTS, and if not, load it
    with SCRIPT LOAD.

    This assumes that all redis scripts are determined at the start and thus it's not a
    relevant amount of memory to store the script hashes in memory indefinitely.

    Arguments:
        redis (redis.asyncio.client.Redis): The redis client
        script (str): The lua script to load if it's not already loaded
        script_hash (str): The sha1 hash of the script
        check_every_s (float): How often to check if the script is loaded
        now (Optional[float]): The current time; defaults to time.time()
        force (bool): If True, will always check if the script is loaded
    """
    if now is None:
        now = time.time()

    if not force:
        last_checked_at = _last_checked_at_hash_to_time.get(script_hash)
        if last_checked_at is not None and (now - last_checked_at < check_every_s):
            return

    loaded = cast(Tuple[bool], await redis.script_exists(script_hash))
    if not loaded[0]:
        correct_hash = await redis.script_load(script)
        assert correct_hash == script_hash, f"{correct_hash=} != {script_hash=}"

    _last_checked_at_hash_to_time[script_hash] = now


class PartialEnsureScriptExists(Protocol):
    def __call__(
        self,
        redis: redis.asyncio.client.Redis,
        /,
        *,
        check_every_s: float = 5.0,
        force: bool = False,
    ) -> Coroutine[Any, Any, None]:
        """Verifies that this script is held in redis. This is a non-pure function; it will
        do nothing if called within the last `check_every_s` seconds. You can force it to
        check by setting `force=True`, which makes this function essentially pure (though it
        does still store some state).

        Arguments:
            redis (redis.asyncio.client.Redis): The redis client
            check_every_s (float): How often to check if the script is loaded, usually not set
            force (bool): If True, will always check if the script is loaded
        """
        ...


def make_partial_ensure_script_exists(
    *, script_name: str, script: str, script_hash: str
) -> PartialEnsureScriptExists:
    """Helper function to act as partial(ensure_script_exists, script=script, script_hash=script_hash)
    but preserving as much VSCode integration as possible. Right now, this gives the correct
    type signature and while you are writing the arguments you can get some documentation on what
    they are, but hovering the function name will not give you the documentation for the function
    itself (unsure how to fix that).

    With just using partial directly, the type signature inappropriately includes the script
    and script hash and the documentation behaves similarly (with the script and script hash
    included)
    """

    def _my_partial(
        redis: redis.asyncio.client.Redis,
        check_every_s: float = 5.0,
        force: bool = False,
    ):
        return ensure_script_exists(
            redis,
            script=script,
            script_hash=script_hash,
            check_every_s=check_every_s,
            force=force,
        )

    return _my_partial
