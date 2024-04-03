import threading
import asyncio
import multiprocessing
from multiprocessing.synchronize import Event as MPEvent
from typing import Any, Union
from queue import Empty


def adapt_threading_event_to_asyncio(
    threading_event: Union[threading.Event, MPEvent],
) -> asyncio.Event:
    """Converts a threading.Event to an asyncio.Event in the
    current event loop, using a background thread.
    """
    asyncio_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    thread = threading.Thread(
        target=set_asyncio_on_threading_event,
        args=(loop, threading_event, asyncio_event),
    )
    thread.start()
    return asyncio_event


def set_asyncio_on_threading_event(
    loop: asyncio.BaseEventLoop,
    threading_event: Union[threading.Event, MPEvent],
    asyncio_event: asyncio.Event,
):
    """Sets the asyncio.Event when the threading.Event is set, threadsafe"""
    threading_event.wait()
    loop.call_soon_threadsafe(asyncio_event.set)


async def adapt_mp_queue_pop_to_asyncio(queue: multiprocessing.Queue) -> Any:
    loop = asyncio.get_running_loop()
    result = loop.create_future()

    canceled_event = threading.Event()
    thread = threading.Thread(
        target=set_asyncio_on_queue_pop,
        args=(loop, queue, result, canceled_event),
    )
    thread.start()

    try:
        return await result
    except asyncio.CancelledError:
        canceled_event.set()
        thread.join()
        raise


def set_asyncio_on_queue_pop(
    loop: asyncio.BaseEventLoop,
    queue: multiprocessing.Queue,
    fut: asyncio.Future,
    canceled_event: threading.Event,
):
    while True:
        try:
            item = queue.get(timeout=0.1)
            loop.call_soon_threadsafe(
                fut.set_result,
                item,
            )
        except Empty:
            if canceled_event.is_set():
                loop.call_soon_threadsafe(fut.cancel)
                break
