import io
import traceback
from typing import Dict, Optional
from collections import deque
import time
import os
import socket
import logging as logger


async def handle_error(exc: BaseException, *, extra_info: Optional[str] = None) -> None:
    """Handles a generic error"""
    logger.error("Posting error to slack", exc_info=exc)

    message = "\n".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)[-5:]
    )
    message = f"{socket.gethostname()}\n\n```\n{message}\n```"

    if extra_info is not None:
        message += f"\n\n{extra_info}"

    from itgs import Itgs

    try:
        async with Itgs() as itgs:
            slack = await itgs.slack()
            await slack.send_web_error_message(message, "an error occurred in jobs")
    except:
        logger.exception("Failed to send slack message for error")


async def handle_contextless_error(*, extra_info: Optional[str] = None) -> None:
    """Handles an error that was found programmatically, i.e., which didn't cause an
    actual exception object to be raised. This will produce a stack trace and include
    the extra information.
    """
    full_exc = io.StringIO()
    full_exc.write(f"{extra_info=}\n")
    traceback.print_stack(file=full_exc)
    logger.error(full_exc.getvalue())

    current_traceback = traceback.extract_stack()[-5:]
    message = "\n".join(traceback.format_list(current_traceback))
    message = f"{socket.gethostname()}\n\n```\n{message}\n```"

    if extra_info is not None:
        message += f"\n\n{extra_info}"

    from itgs import Itgs

    async with Itgs() as itgs:
        slack = await itgs.slack()
        await slack.send_web_error_message(
            message, "a contextless error occurred in jobs"
        )


RECENT_WARNINGS: Dict[str, deque] = dict()  # deque[float] is not available on prod
"""Maps from a warning identifier to a deque of timestamps of when the warning was sent."""

WARNING_RATELIMIT_INTERVAL = 60 * 60
"""The interval in seconds we keep track of warnings for"""

MAX_WARNINGS_PER_INTERVAL = 5
"""The maximum number of warnings to send per interval for a particular identifier"""


async def handle_warning(
    identifier: str, text: str, exc: Optional[Exception] = None, is_urgent: bool = False
) -> None:
    """Sends a warning to slack, with basic ratelimiting

    Args:
        identifier (str): An identifier for ratelimiting the warning
        text (str): The text to send
        exc (Exception, None): If an exception occurred, formatted and added to
          the text appropriately
        is_urgent (bool): If true, the message is sent to the #oseh-bot channel instead
          of the #web-errors channel
    """

    if exc is not None:
        text += (
            "\n\n```"
            + "\n".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)[-5:]
            )
            + "```"
        )

    logger.warning(f"{identifier}: {text}")

    if identifier not in RECENT_WARNINGS:
        RECENT_WARNINGS[identifier] = deque()

    recent_warnings = RECENT_WARNINGS[identifier]
    now = time.time()

    while recent_warnings and recent_warnings[0] < now - WARNING_RATELIMIT_INTERVAL:
        recent_warnings.popleft()

    if len(recent_warnings) >= MAX_WARNINGS_PER_INTERVAL:
        logger.debug(f"warning suppressed (ratelimit): {identifier}")
        return

    recent_warnings.append(now)
    total_warnings = len(recent_warnings)

    message = f"WARNING: `{identifier}` (warning {total_warnings}/{MAX_WARNINGS_PER_INTERVAL} per {WARNING_RATELIMIT_INTERVAL} seconds for {socket.gethostname()} - pid {os.getpid()})\n\n{text}"
    preview = f"WARNING: {identifier}"

    from itgs import Itgs

    try:
        async with Itgs() as itgs:
            slack = await itgs.slack()
            if is_urgent:
                logger.debug("sending warning to #ops")
                await slack.send_oseh_bot_message(message, preview=preview)
            else:
                logger.debug("sending warning to #web-errors")
                await slack.send_web_error_message(message, preview=preview)
    except:
        logger.exception("Failed to send slack message for warning")
