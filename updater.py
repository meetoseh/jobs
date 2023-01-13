"""Handles updating when the repository is updated"""
from itgs import Itgs
from error_middleware import handle_warning
import asyncio
import subprocess
import platform
import secrets
import os
import threading

from mp_helper import adapt_threading_event_to_asyncio


async def _listen_forever():
    """Subscribes to the redis channel updates:jobs and upon
    recieving a message, calls /home/ec2-user/update_webapp.sh
    """
    async with Itgs() as itgs:
        await release_update_lock_if_held(itgs)

    while True:
        try:
            async with Itgs() as itgs:
                redis = await itgs.redis()
                pubsub = redis.pubsub()
                await pubsub.subscribe("updates:jobs")
                while (
                    await pubsub.get_message(ignore_subscribe_messages=True, timeout=5)
                ) is None:
                    pass
                break
        except Exception as e:
            await handle_warning("updater:error", "Error in jobs updater loop", e)
            await asyncio.sleep(1)

    async with Itgs() as itgs:
        await acquire_update_lock(itgs)

    do_update()


async def acquire_update_lock(itgs: Itgs):
    our_identifier = secrets.token_urlsafe(16).encode("utf-8")
    local_cache = await itgs.local_cache()

    redis = await itgs.redis()
    while True:
        local_cache.set(b"updater-lock-key", our_identifier, expire=310)
        success = await redis.set(b"updates:jobs:lock", our_identifier, nx=True, ex=300)
        if success:
            break
        await asyncio.sleep(1)


DELETE_IF_MATCH_SCRIPT = """
local key = KEYS[1]
local expected = ARGV[1]

local current = redis.call("GET", key)
if current == expected then
    redis.call("DEL", key)
    return 1
end
return 0
"""


async def release_update_lock_if_held(itgs: Itgs):
    local_cache = await itgs.local_cache()

    our_identifier = local_cache.get(b"updater-lock-key")
    if our_identifier is None:
        return

    redis = await itgs.redis()
    await redis.eval(DELETE_IF_MATCH_SCRIPT, 1, b"updates:jobs:lock", our_identifier)
    local_cache.delete(b"updater-lock-key")


def do_update():
    if platform.platform().lower().startswith("linux"):
        subprocess.Popen(
            "bash /home/ec2-user/update_webapp.sh > /dev/null 2>&1",
            shell=True,
            stdin=None,
            stdout=None,
            stderr=None,
            preexec_fn=os.setpgrp,
        )
    else:
        subprocess.Popen(
            "bash /home/ec2-user/update_webapp.sh",
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            close_fds=True,
        )


async def listen_forever(stop_event: threading.Event):
    """Subscribes to the redis channel updates:jobs and upon
    recieving a message, calls /home/ec2-user/update_webapp.sh
    """
    if os.path.exists("updater.lock"):
        return
    with open("updater.lock", "w") as f:
        f.write(str(os.getpid()))

    asyncio_stop_event = adapt_threading_event_to_asyncio(stop_event)

    try:
        _, running = await asyncio.wait(
            [
                asyncio.create_task(_listen_forever()),
                asyncio.create_task(asyncio_stop_event.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in running:
            t.cancel()
    finally:
        stop_event.set()  # ensures the thread shuts down
        os.unlink("updater.lock")
        print("updater shutdown")


def listen_forever_sync(stop_event: threading.Event):
    """Subscribes to the redis channel updates:jobs and upon
    recieving a message, calls /home/ec2-user/update_webapp.sh
    """
    asyncio.run(listen_forever(stop_event))
