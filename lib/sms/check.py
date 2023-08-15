from itgs import Itgs
from typing import Optional
from lib.sms.sms_info import PendingSMS


async def retry_pending(itgs: Itgs, *, sms: PendingSMS, now: Optional[float]) -> bool:
    """If the provided sms is still the latest version of the sms with that uid,
    this will send the sid to the recovery queue and return True, otherwise it
    will return False. This should only be called by a failure callback on a
    retryable error, which failed on the `pending` action.

    This will increment events as necessary.

    Args:
        itgs (Itgs): the integrations to (re)use
        sms (PendingSMS): the SMS to retry
        now (Optional[float], optional): the current time. Defaults to to time.time()

    Returns:
        bool: True if the sid was sent to the recovery queue, False otherwise. If
          False, the failure callback should proceed as if it was never called
          (rolling back any changes it already made).
    """


async def abandon_pending(itgs: Itgs, *, sms: PendingSMS, now: Optional[float]) -> bool:
    """If the provided sms is still the latest version of the sms with that uid,
    this will abandon the sms and return True, otherwise it will return False.
    This should only be called by a failure callback on a retryable error, which
    failed on the `pending` action.

    This will increment events as necessary.

    Args:
        itgs (Itgs): the integrations to (re)use
        sms (PendingSMS): the SMS to abandon
        now (Optional[float], optional): the current time. Defaults to to time.time()

    Returns:
        bool: True if the sms was abandoned, False otherwise. If False, the failure
          callback should proceed as if it was never called (rolling back any changes
          it already made).
    """
