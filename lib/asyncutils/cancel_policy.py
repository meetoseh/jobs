import asyncio
from enum import IntEnum
from typing import Set


class CancelPolicy(IntEnum):
    """The policy to use when canceling tasks."""

    CANCEL_AND_WAIT = 1
    """Cancels any running tasks and waits for them to finish."""

    CANCEL_WITHOUT_WAITING = 2
    """Cancels any running tasks and then returns immediately"""

    WAIT_WITHOUT_CANCELING = 3
    """Waits for any running tasks to finish, but does not cancel them."""


async def handle_cancel_policy(running: Set[asyncio.Task], policy: CancelPolicy):
    """Performs the necessary actions for the given running tasks provided
    the given policy.
    """
    if policy == CancelPolicy.CANCEL_AND_WAIT:
        await handle_cancel_and_wait_policy(running)
    elif policy == CancelPolicy.CANCEL_WITHOUT_WAITING:
        await handle_cancel_without_waiting_policy(running)
    elif policy == CancelPolicy.WAIT_WITHOUT_CANCELING:
        await handle_wait_without_canceling_policy(running)
    else:
        raise ValueError(f"Unknown policy: {policy}")


async def handle_cancel_and_wait_policy(running: Set[asyncio.Task]):
    """Cancels all running tasks and waits for them to finish."""
    if not running:
        return

    for task in running:
        task.cancel()

    await asyncio.wait(
        running,
        return_when=asyncio.ALL_COMPLETED,
    )


async def handle_cancel_without_waiting_policy(running: Set[asyncio.Task]):
    """Cancels all running tasks and then returns immediately."""
    for task in running:
        task.cancel()


async def handle_wait_without_canceling_policy(running: Set[asyncio.Task]):
    """Waits for all running tasks to finish, but does not cancel them."""
    if not running:
        return

    await asyncio.wait(
        running,
        return_when=asyncio.ALL_COMPLETED,
    )
