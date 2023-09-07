import dataclasses
import logging
from typing import Optional, Union, cast

from itgs import Itgs
from lib.email.email_info import EmailAttempt, EmailFailureInfo, EmailPending
import time

from lib.email.send import abandon_send, retry_send


@dataclasses.dataclass
class EmailRetryOrAbandonStandardResult:
    wanted_to_retry: bool
    succeeded: Optional[bool]

    @property
    def should_ignore(self):
        return self.succeeded is False


async def retry_or_abandon_standard(
    itgs: Itgs,
    email: Union[EmailAttempt, EmailPending],
    failure_info: EmailFailureInfo,
    *,
    now: Optional[float] = None,
) -> EmailRetryOrAbandonStandardResult:
    """Uses the standard logic for determining if the given email should be abandoned
    or retried, and then attempts to do so.

    Args:
        itgs (Itgs): the integrations to (re)use
        email (EmailAttempt, EmailPending)
        failure_info (EmailFailureInfo): what went wrong
        now (float, None): If specified use this as the current time, otherwise use
            the current time.

    Returns:
        EmailRetryOrAbandonStandardResult: the result of the operation
    """
    if now is None:
        now = time.time()

    if not failure_info.retryable:
        return EmailRetryOrAbandonStandardResult(wanted_to_retry=False, succeeded=None)

    if failure_info.step not in ("template", "send"):
        raise NotImplementedError(
            f"Unknown step for retryable failure: {failure_info.step}"
        )

    attempt = cast(EmailAttempt, email)

    if attempt.retry > 3 or (now - email.initially_queued_at) > 60 * 60 * 12:
        logging.info(
            f"Abandoning email {email.uid} after {email.retry} retries during {failure_info.step} step"
        )
        await abandon_send(itgs, email=email, now=now)
        return EmailRetryOrAbandonStandardResult(wanted_to_retry=False, succeeded=True)

    logging.info(
        f"Retrying email {email.uid} {failure_info.step} (this will be attempt {email.retry + 1})"
    )
    await retry_send(itgs, email=email, now=now)
    return EmailRetryOrAbandonStandardResult(wanted_to_retry=True, succeeded=None)
