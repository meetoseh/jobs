import gc
import os
import sys
import threading
import asyncio
import logging
import logging.config

import yaml
import psutil

from error_middleware import handle_error
from mp_helper import adapt_threading_event_to_asyncio
import pickle


def _make_bytes_human_readable(size: float) -> str:
    """Converts a size in bytes to a human-readable string."""
    unit = "B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.2f} {unit}"


def is_probably_class_instance_like(o) -> bool:
    try:
        is_openai_beta_proxy = hasattr(o, "__get_proxied__")
        if is_openai_beta_proxy:
            return True
    except BaseException:
        ...

    try:
        is_six_lazy_module = hasattr(o, "_resolve")
        if is_six_lazy_module:
            return True
    except BaseException:
        ...

    try:
        return hasattr(o, "__class__")
    except BaseException:
        return True


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
                process = psutil.Process()
                info = process.memory_info()

                resident_set_size = info.rss
                virtual_memory_size = info.vms

                if resident_set_size > 1024 * 1024 * 1024 * 4:
                    # this threshold was chosen 10/1/2024 as an amount that always indicates we are
                    # leaking memory
                    logging.warning(
                        f"DETECTED MEMORY LEAK: {_make_bytes_human_readable(resident_set_size)} allocated space in ram (RSS)"
                    )

                    # looks through leak-{n}.pickle 1-10 and replaces the oldest or the first to not exist
                    best_n = 1
                    best_timestamp = float("inf")
                    for n in range(1, 11):
                        try:
                            # get when the file exists
                            file_info = os.stat(f"leak-{n}.pickle")
                            if file_info.st_mtime < best_timestamp:
                                best_timestamp = file_info.st_mtime
                                best_n = n
                        except FileNotFoundError:
                            best_n = n
                            break

                    out_filename = f"leak-{best_n}.pickle"
                    lookup_by_type = dict()
                    logging.warning("iterating gc objects")
                    for obj in gc.get_objects():
                        size = sys.getsizeof(obj, 0)
                        type_name = f"{type(obj)!r}"
                        type_info = lookup_by_type.get(type_name)
                        if type_info is None:
                            type_info = {"count": 0, "total_size": 0, "largest_size": 0}
                            lookup_by_type[type_name] = type_info
                        type_info["count"] += 1
                        type_info["total_size"] += size
                        type_info["largest_size"] = max(type_info["largest_size"], size)
                    logging.warning(f"writing memory information to {out_filename}")
                    with open(out_filename, "wb") as f:
                        pickle.dump(lookup_by_type, f)

                    if resident_set_size > 1024 * 1024 * 1024 * 7:
                        logging.warning("about to OOM, exiting")
                        raise Exception("about to run out of memory")

                logging.info(
                    f"allocated space in ram (RSS): {_make_bytes_human_readable(resident_set_size)}, allocated space, total (VMS): {_make_bytes_human_readable(virtual_memory_size)}"
                )

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
