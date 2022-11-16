from itgs import Itgs
import traceback
import logging
from typing import Dict, Optional
from collections import deque
import time
import os


async def handle_error(exc: Exception) -> None:
    """Handles a generic error"""
    logging.error("Posting error to slack", exc_info=exc)
    message = "\n".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)[-5:]
    )
    message = f"```\n{message}\n```"
    async with Itgs() as itgs:
        slack = await itgs.slack()
        await slack.send_web_error_message(message, "an error occurred in jobs")


RECENT_WARNINGS: Dict[str, deque] = dict()  # deque[float] is not available on prod
"""Maps from a warning identifier to a deque of timestamps of when the warning was sent."""

WARNING_RATELIMIT_INTERVAL = 60 * 60
"""The interval in seconds we keep track of warnings for"""

MAX_WARNINGS_PER_INTERVAL = 5
"""The maximum number of warnings to send per interval for a particular identifier"""


async def handle_warning(
    identifier: str, text: str, exc: Optional[Exception] = None
) -> None:
    """Sends a warning to slack, with basic ratelimiting"""

    if identifier not in RECENT_WARNINGS:
        RECENT_WARNINGS[identifier] = deque()

    recent_warnings = RECENT_WARNINGS[identifier]
    now = time.time()

    while recent_warnings and recent_warnings[0] < now - WARNING_RATELIMIT_INTERVAL:
        recent_warnings.popleft()

    if len(recent_warnings) >= MAX_WARNINGS_PER_INTERVAL:
        return

    if exc is not None:
        text += (
            "\n\n```"
            + "\n".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)[-5:]
            )
            + "```"
        )

    recent_warnings.append(now)
    total_warnings = len(recent_warnings)

    async with Itgs() as itgs:
        slack = await itgs.slack()
        await slack.send_web_error_message(
            f"WARNING: `{identifier}` (warning {total_warnings}/{MAX_WARNINGS_PER_INTERVAL} per {WARNING_RATELIMIT_INTERVAL} seconds for {os.getpid()})\n\n{text}",
            preview=f"WARNING: {identifier}",
        )
