"""Handles updating when the repository is updated"""
from itgs import Itgs
import asyncio
import subprocess
import platform
import os
from graceful_death import GracefulDeath


async def _listen_forever():
    """Subscribes to the redis channel updates:jobs and upon
    recieving a message, calls /home/ec2-user/update_webapp.sh
    """
    gd = GracefulDeath()
    async with Itgs() as itgs:
        redis = await itgs.redis()
        pubsub = redis.pubsub()
        await pubsub.subscribe("updates:jobs")
        while (
            await pubsub.get_message(ignore_subscribe_messages=True, timeout=5)
        ) is None:
            if gd.received_term_signal:
                print("updater shutting down")
                return
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


async def listen_forever():
    """Subscribes to the redis channel updates:jobs and upon
    recieving a message, calls /home/ec2-user/update_webapp.sh
    """
    if os.path.exists("updater.lock"):
        return
    with open("updater.lock", "w"):
        pass
    try:
        await _listen_forever()
    finally:
        os.unlink("updater.lock")


def listen_forever_sync():
    """Subscribes to the redis channel updates:jobs and upon
    recieving a message, calls /home/ec2-user/update_webapp.sh
    """
    asyncio.run(listen_forever())


if __name__ == "__main__":
    listen_forever_sync()
