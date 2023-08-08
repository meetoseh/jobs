from typing import Awaitable, Callable
from redis.exceptions import NoScriptError


async def run_with_prep(
    prep: Callable[[bool], Awaitable[None]], func: Callable[[], Awaitable[None]]
) -> None:
    """Runs `prep`, then tries to run `func`, catching NoScriptError and retrying 1
    time. The first call to prep is with `False` and the second run is with `True`.

    Args:
        prep (Callable[[bool], Awaitable[None]]): The function to prepare redis
        func (Callable[[], Awaitable[None]]): The function to run
    """
    await prep(False)
    try:
        await func()
    except NoScriptError:
        await prep(True)
        await func()
