from typing import Optional, Union
from lib.push.message_attempt_info import (
    MessageAttemptFailureInfo,
    MessageAttemptToCheck,
    MessageAttemptToSend,
)
import logging
import time
from itgs import Itgs
from lib.push.send import abandon_send_push, retry_send_push
from lib.push.check import abandon_check_push, retry_check_push


async def retry_or_abandon_standard(
    itgs: Itgs,
    attempt: Union[MessageAttemptToCheck, MessageAttemptToSend],
    failure_info: MessageAttemptFailureInfo,
    *,
    now: Optional[float] = None,
) -> bool:
    """If the given failure is retryable, this will either retry or abandon the
    attempt. If the failure is not retryable, this does nothing and returns False.

    This updates statistics as appropriate and logs the action taken.

    Args:
        itgs (Itgs): the integrations to (re)use
        attempt (Union[MessageAttemptToCheck, MessageAttemptToSend]): the attempt to retry or abandon
        failure_info (MessageAttemptFailureInfo): the failure information
        now (float, optional): If specified we use this as the current time for deciding
            whether to abandon the attempt. Defaults to None, in which case the current
            time is used.

    Returns:
        bool: True if the attempt was retried, False if it was abandoned
    """
    if not failure_info.retryable:
        return False

    if now is None:
        now = time.time()

    if failure_info.action == "send":
        send_attempt: MessageAttemptToSend = attempt

        if (
            send_attempt.retry >= 3
            or (now - send_attempt.initially_queued_at) > 60 * 60 * 12
        ):
            logging.info(
                f"Abandoning send attempt {send_attempt.uid} after {send_attempt.retry} retries"
            )
            await abandon_send_push(itgs, attempt=send_attempt)
            return False
        else:
            logging.info(
                f"Retrying send attempt {send_attempt.uid} after {send_attempt.retry} retries"
            )
            await retry_send_push(itgs, attempt=send_attempt)
            return True
    else:
        check_attempt: MessageAttemptToCheck = attempt

        if (
            check_attempt.retry >= 3
            or (now - check_attempt.attempt_initially_queued_at) > 60 * 60 * 12
        ):
            logging.info(
                f"Abandoning check attempt {check_attempt.uid} after {check_attempt.retry} retries"
            )
            await abandon_check_push(itgs, attempt=check_attempt)
            return False
        else:
            logging.info(
                f"Retrying check attempt {check_attempt.uid} after {check_attempt.retry} retries"
            )
            await retry_check_push(itgs, attempt=check_attempt)
            return True
