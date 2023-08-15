from lib.sms.send import retry_send, abandon_send
from lib.sms.check import retry_pending, abandon_pending
import dataclasses
from typing import Optional, Union
from itgs import Itgs
from lib.sms.sms_info import SMSToSend, PendingSMS, SMSFailureInfo
import logging
import time


@dataclasses.dataclass
class SMSRetryOrAbandonStandardResult:
    wanted_to_retry: bool
    succeeded: Optional[bool]

    @property
    def should_ignore(self):
        return self.succeeded is False


async def retry_or_abandon_standard(
    itgs: Itgs,
    sms: Union[SMSToSend, PendingSMS],
    failure_info: SMSFailureInfo,
    *,
    now: Optional[float] = None,
) -> SMSRetryOrAbandonStandardResult:
    """Uses the standard logic for determining if the given sms should be abandoned
    or retried, and then attempts to do so. If the sms failed at the pending step it's
    possible that we became aware of a more recent status between the failure callback
    being queued and now, in which case this failure callback should undo anything its
    already done and proceed as if it wasn't called.

    Args:
        itgs (Itgs): the integrations to (re)use
        sms (SMSToSend, PendingSMS): the state of the sms
        failure_info (SMSFailureInfo): what went wrong
        now (float, None): If specified use this as the current time, otherwise use
            the current time.

    Returns:
        SMSRetryOrAbandonStandardResult: the result of the operation
    """
    if now is None:
        now = time.time()

    if not failure_info.retryable:
        return SMSRetryOrAbandonStandardResult(wanted_to_retry=False, succeeded=None)

    if failure_info.action == "send":
        assert isinstance(sms, SMSToSend)

        if sms.retry >= 3 or (now - sms.initially_queued_at) > 60 * 60 * 12:
            logging.info(
                f"Abandoning sms {sms.uid} after {sms.retry} retries during send step"
            )
            await abandon_send(itgs, sms=sms, now=now)
            return SMSRetryOrAbandonStandardResult(
                wanted_to_retry=False, succeeded=True
            )

        logging.info(
            f"Retrying sms {sms.uid} send (this will be attempt {sms.retry + 1})"
        )
        await retry_send(itgs, sms=sms, now=now)
        return SMSRetryOrAbandonStandardResult(wanted_to_retry=True, succeeded=True)

    assert failure_info.action == "pending"
    assert isinstance(sms, PendingSMS)

    if sms.num_failures >= 4 or (now - sms.send_initially_queued_at) > 60 * 60 * 12:
        succeeded = await abandon_pending(itgs, sms=sms, now=now)
        if succeeded:
            logging.info(
                f"Abandoned sms {sms.uid} after {sms.num_failures - 1} retries during pending step"
            )
        return SMSRetryOrAbandonStandardResult(
            wanted_to_retry=False, retry_succeeded=succeeded
        )

    succeeded = await retry_pending(itgs, sms=sms, now=now)
    if succeeded:
        logging.info(
            f"Retrying sms {sms.uid} poll (this will be attempt {sms.num_failures})"
        )
    return SMSRetryOrAbandonStandardResult(
        wanted_to_retry=True, retry_succeeded=succeeded
    )
