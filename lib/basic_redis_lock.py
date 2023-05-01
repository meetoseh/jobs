from typing import Optional
from graceful_death import GracefulDeath
from itgs import Itgs
from contextlib import asynccontextmanager
import asyncio


class LockHeldError(Exception):
    """Raised when a lock is already held by another process"""

    def __init__(self, key):
        super().__init__(f"Lock {key=} is already held by another process")


@asynccontextmanager
async def basic_redis_lock(
    itgs: Itgs, key: str, *, gd: Optional[GracefulDeath] = None, spin: bool = False
) -> None:
    """Uses redis for a very basic lock on the given key, releasing it when done

    Args:
        itgs (Itgs): The integrations to (re)use
        key (str): The redis key to use for the lock
        gd (GracefulDeath, None): If specified, sleeps will be canceled once a term
            signal is received, using this as the detector. Defaults to None, meaning
            term signals are not handled.
        spin (bool): Whether to spin while waiting for the lock to be released,
            or to just raise an exception. Defaults to False. Requires that gd
            be specified to avoid spinning while a term signal is received.
    """
    if spin and gd is None:
        raise ValueError("Cannot spin without gd")

    redis = await itgs.redis()
    while True:
        success = await redis.set(key, "1", nx=True, ex=86400)
        if success:
            break
        if not spin:
            raise LockHeldError(key)
        if gd.received_term_signal:
            return
        await asyncio.sleep(0.1)

    try:
        yield
    finally:
        await redis.delete(key)
