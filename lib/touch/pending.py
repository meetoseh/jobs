"""Helper functions for success/failure callbacks within the touch subsystem to
interact with the touch:pending sorted set
"""
from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.touch_send_destination_success import (
    ensure_touch_send_destination_success_script_exists,
    touch_send_destination_success,
)
from redis_helpers.touch_send_destination_failed import (
    ensure_touch_send_destination_failed_script_exists,
    touch_send_destination_failed,
)
import time


async def on_touch_destination_success(itgs: Itgs, touch_uid: str) -> None:
    """Should be invoked after a destination succeeds for a given touch. If the
    touch is in the touch:pending sorted set it will be removed and have its
    success callback called, atomically.

    Args:
        itgs (Itgs): the integrations to (re)use
        touch_uid (str): the uid of the touch for which one of the destinations
            succeeded
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_touch_send_destination_success_script_exists(redis, force=force)

    async def func():
        await touch_send_destination_success(
            redis,
            b"touch:pending",
            f"touch:pending:{touch_uid}".encode("utf-8"),
            f"touch:pending:{touch_uid}:remaining".encode("utf-8"),
            b"jobs:hot",
            touch_uid.encode("utf-8"),
            time.time(),
        )

    await run_with_prep(prep, func)


async def on_touch_destination_abandoned_or_permanently_failed(
    itgs: Itgs, touch_uid: str, attempt_uid: str
) -> None:
    """Should be invoked after a destination could not be reached and will no
    longer be retried for a given touch. If the touch is in the touch:pending
    sorted set and has no remaining destinations, implying that every
    destination has been abandoned or permanently failed, it will be removed and
    have its failure callback called. Otherwise, this destination will be
    removed from the touch's remaining destinations. This all happens
    atomically.

    Args:
        itgs (Itgs): the integrations to (re)use
        touch_uid (str): the uid of the touch for which one of the destinations
            was abandoned or permanently failed
        attempt_uid (str): the uid of the email/sms/push that was abandoned or permanently
            failed
    """

    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_touch_send_destination_failed_script_exists(redis, force=force)

    async def func():
        await touch_send_destination_failed(
            redis,
            b"touch:pending",
            f"touch:pending:{touch_uid}".encode("utf-8"),
            f"touch:pending:{touch_uid}:remaining".encode("utf-8"),
            b"jobs:hot",
            touch_uid.encode("utf-8"),
            attempt_uid.encode("utf-8"),
            time.time(),
        )

    await run_with_prep(prep, func)
