import threading
import asyncio


def adapt_threading_event_to_asyncio(
    threading_event: threading.Event,
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
    threading_event: threading.Event,
    asyncio_event: asyncio.Event,
):
    """Sets the asyncio.Event when the threading.Event is set, threadsafe"""
    threading_event.wait()
    loop.call_soon_threadsafe(asyncio_event.set)
