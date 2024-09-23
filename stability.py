import threading
import asyncio
import logging
import logging.config

import yaml
import psutil

from error_middleware import handle_error
from mp_helper import adapt_threading_event_to_asyncio


def _make_bytes_human_readable(size: float) -> str:
    """Converts a size in bytes to a human-readable string."""
    unit = "B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.2f} {unit}"


def _memory_usage_information() -> str:
    process = psutil.Process()
    info = process.memory_info()

    resident_set_size = info.rss
    virtual_memory_size = info.vms

    return f"allocated space in ram (RSS): {_make_bytes_human_readable(resident_set_size)}, allocated space, total (VMS): {_make_bytes_human_readable(virtual_memory_size)}"


async def _run_forever(stop_event: threading.Event, stopping_event: threading.Event):
    asyncio_event = adapt_threading_event_to_asyncio(stop_event)
    asyncio_stopping_event = adapt_threading_event_to_asyncio(stopping_event)

    try:
        print("stability background thread starting")
        with open("logging.yaml") as f:
            logging_config = yaml.safe_load(f)

        logging.config.dictConfig(logging_config)
        logging.info("stability background thread initialized logging")

        stop_task = asyncio.create_task(asyncio_event.wait())
        stopping_task = asyncio.create_task(asyncio_stopping_event.wait())
        while True:
            if stop_task.done() or stopping_task.done():
                break
            post_memory_usage_timeout = asyncio.create_task(asyncio.sleep(10))
            await asyncio.wait(
                [post_memory_usage_timeout, stop_task, stopping_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not post_memory_usage_timeout.cancel():
                logging.info(f"memory usage: {_memory_usage_information()}")

        if asyncio_stopping_event.is_set():
            print("stability stopped on stopping event, waiting for real stop")
            await asyncio.wait_for(asyncio_event.wait(), timeout=300)

        stop_task.cancel()
        stopping_task.cancel()
    except Exception as e:
        await handle_error(e)
    finally:
        stop_event.set()
        print("stability stopped")


def run_forever_sync(stop_event: threading.Event, stopping_event: threading.Event):
    """Runs forever, typically as a target for a separate thread, logging memory
    usage and warning slack if it starts to grow too large.
    """
    asyncio.run(_run_forever(stop_event, stopping_event))
