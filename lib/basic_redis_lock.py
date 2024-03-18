import secrets
from typing import Union
from error_middleware import handle_warning
from graceful_death import GracefulDeath, graceful_sleep
from itgs import Itgs
from contextlib import asynccontextmanager
import asyncio
import time
import socket
import logging

from redis_helpers.acquire_lock import acquire_lock_safe
from redis_helpers.release_lock import release_lock_safe


class LockHeldError(Exception):
    """Raised when a lock is already held by another process"""

    def __init__(self, key):
        super().__init__(f"Lock {key=} is already held by another process")


@asynccontextmanager
async def basic_redis_lock(
    itgs: Itgs,
    key: Union[str, bytes],
    *,
    gd: GracefulDeath,
    spin: bool = False,
):
    """Uses redis for a very basic lock on the given key, releasing it when done.

    If the lock is currently taken, in order to reduce the need for human intervention
    after SIGKILLs, the following rules are used to determine if the lock can be stolen:

    - If the lock is owned by the same hostname, the lock is set to expire in
      5m, a warning is raised. This needs to be changed if we are using multiple
      processes on the same host or expect to contend for the lock within the
      same process to use a multi-process aware coordinating lock.
    - If the lock is owned by a different hostname and they've held the lock for at least
      1 hour a warning is emitted. If they've held the lock for at least 6 hours, a different
      warning is emitted and a 1 hour expiration is set on the lock.


    Args:
        itgs (Itgs): The integrations to (re)use
        key (str, bytes): The redis key to use for the lock
        gd (GracefulDeath): Sleeps will be canceled once a term
            signal is received, using this as the detector. Furthermore, as soon as
            a term signal is received, we flag the lock to expire in 60s
            regardless of if we receive control back.
        spin (bool): Whether to spin while waiting for the lock to be released,
            or to just raise an exception. Defaults to False. Requires that gd
            be specified to avoid spinning while a term signal is received.
    """
    if spin and gd is None:
        raise ValueError("Cannot spin without gd")
    if isinstance(key, str):
        key = key.encode("utf-8")

    my_hostname = ("jobs-" + socket.gethostname()).encode("utf-8")
    lock_id = secrets.token_urlsafe(16).encode("utf-8")
    while True:
        logging.debug(f"basic_redis_lock: acquiring lock {key=} {lock_id=}")
        lock_result = await acquire_lock_safe(
            itgs, key, my_hostname, int(time.time()), lock_id
        )
        if lock_result.error_type is None:
            logging.debug(f"basic_redis_lock: acquired lock {key=} {lock_id=}")
            break

        if lock_result.error_type == "already_held":
            logging.warning(f"basic_redis_lock: already held lock {key=} {lock_id=}")
            await handle_warning(
                f"{__name__}:already_held",
                f"lock {key=} was acquired without a success response",
            )
            break

        logging.info(
            f"basic_redis_lock: waiting for lock {key=} {lock_id=}: {lock_result=}, {spin=}"
        )

        if not spin:
            raise LockHeldError(key)
        if not await graceful_sleep(gd, 0.1):
            logging.info(
                f"basic_redis_lock: term signal received while waiting for lock {key=}, {lock_id=}"
            )
            return

    signal_aio_event = asyncio.Event()
    cancel_checking_task = asyncio.Event()
    checking_task = asyncio.create_task(
        _check_for_signal(
            itgs, gd, signal_aio_event, cancel_checking_task, key, lock_id
        )
    )
    try:
        yield
    finally:
        logging.debug(
            f"basic_redis_lock: canceling checking task for {key=} {lock_id=}"
        )
        cancel_checking_task.set()
        logging.debug(f"basic_redis_lock: releasing {key=} {lock_id=}")
        await release_lock_safe(itgs, key, lock_id)
        logging.debug(
            f"basic_redis_lock: waiting for checking task to finish for {key=} {lock_id=}"
        )
        await checking_task
        logging.debug(f"basic_redis_lock: finished {key=} {lock_id=}")


async def _check_for_signal(
    itgs: Itgs,
    gd: GracefulDeath,
    signal_aio_event: asyncio.Event,
    cancel_signal: asyncio.Event,
    key: bytes,
    lock_id: bytes,
):
    wait_cancel_task = asyncio.create_task(cancel_signal.wait())
    while True:
        sleep_task = asyncio.create_task(graceful_sleep(gd, 0.2))
        await asyncio.wait(
            [wait_cancel_task, sleep_task], return_when=asyncio.FIRST_COMPLETED
        )

        if cancel_signal.is_set():
            logging.debug(
                f"basic_redis_lock: cancel_signal received for {key=}, {lock_id=}"
            )
            sleep_task.cancel()
            return
        if gd.received_term_signal:
            logging.debug(
                f"basic_redis_lock: polled term signal for {key=}, {lock_id=}"
            )
            wait_cancel_task.cancel()
            break

    signal_aio_event.set()
    logging.debug(
        f"basic_redis_lock: setting expiration in _check_for_signal for {key=}, {lock_id=}"
    )
    await release_lock_safe(itgs, key, lock_id, expire=True)
