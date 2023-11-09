import json
import secrets
from error_middleware import handle_warning
from lib.daily_reminders.registration_stats import (
    DailyReminderRegistrationStatsPreparer,
)
from lib.sms.send import retry_send, abandon_send
from lib.sms.check import retry_pending, abandon_pending
import dataclasses
from typing import Optional, Union
from itgs import Itgs
from lib.sms.sms_info import SMSToSend, PendingSMS, SMSFailureInfo
import logging
import time
import pytz
import unix_dates


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
            wanted_to_retry=False, succeeded=succeeded
        )

    succeeded = await retry_pending(itgs, sms=sms, now=now)
    if succeeded:
        logging.info(
            f"Retrying sms {sms.uid} poll (this will be attempt {sms.num_failures})"
        )
    return SMSRetryOrAbandonStandardResult(wanted_to_retry=True, succeeded=succeeded)


async def suppress_phone_with_reason(
    itgs: Itgs, phone_number: str, *, reason: str, reason_details: dict, now: float
) -> None:
    """Suppresses the given phone number which will prevent it  from being
    contacted, unregistering daily reminders as appropriate and updating stats
    immediately

    It's usually better to call `maybe_suppress_phone_due_to_failure` which will
    consider if the failure warrants suppression and format the reason and
    reason details for you.

    Args:
        itgs (Itgs): the integrations to (re)use
        phone_number (str): the phone number to suppress in E.164 format
        reason (str): the general category
        reason_details (dict): the specific details
        now (float): the canonical system time for stats
    """
    conn = await itgs.conn()
    cursor = conn.cursor()

    new_spn_uid = f"oseh_spn_{secrets.token_urlsafe(16)}"
    response = await cursor.executemany3(
        (
            (
                "INSERT INTO suppressed_phone_numbers ("
                " uid, phone_number, reason, reason_details, created_at"
                ") "
                "SELECT"
                " ?, ?, ?, ?, ? "
                "WHERE"
                " NOT EXISTS ("
                "  SELECT 1 FROM suppressed_phone_numbers AS spn"
                "  WHERE spn.phone_number = ?"
                " )",
                (
                    new_spn_uid,
                    phone_number,
                    reason,
                    json.dumps(reason_details),
                    now,
                    phone_number,
                ),
            ),
            (
                "DELETE FROM user_daily_reminders "
                "WHERE"
                " user_daily_reminders.channel = 'sms'"
                " AND EXISTS ("
                "  SELECT 1 FROM user_phone_numbers"
                "  WHERE"
                "   user_daily_reminders.user_id = user_phone_numbers.user_id"
                "   AND user_phone_numbers.phone_number = ?"
                "   AND NOT EXISTS ("
                "    SELECT 1 FROM user_phone_numbers AS upn"
                "    WHERE"
                "     upn.id <> user_phone_numbers.id"
                "     AND upn.user_id = user_daily_reminders.user_id"
                "     AND upn.verified"
                "     AND upn.receives_notifications"
                "     AND NOT EXISTS ("
                "      SELECT 1 FROM suppressed_phone_numbers AS spn"
                "      WHERE spn.phone_number = upn.phone_number"
                "     )"
                "   )"
                " )",
                (phone_number,),
            ),
        )
    )

    if response[0].rows_affected is not None and response[0].rows_affected > 1:
        await handle_warning(
            f"{__name__}:suppressed_multiple",
            f"Expected `{response[0].rows_affected=}` to be 0 or 1",
        )

    if response[0].rows_affected is None or response[0].rows_affected == 0:
        logging.info(f"Phone number {phone_number} was already suppressed")
    else:
        logging.info(f"Suppressed phone number {phone_number} due to {reason}")

    if response[1].rows_affected is not None and response[1].rows_affected > 0:
        logging.info(
            f"Unregistered {response[1].rows_affected} daily reminders for {phone_number} "
            "due to suppression"
        )

        stats = DailyReminderRegistrationStatsPreparer()
        stats.incr_unsubscribed(
            unix_dates.unix_timestamp_to_unix_date(
                now, tz=pytz.timezone("America/Los_Angeles")
            ),
            "sms",
            "unreachable",
            amt=response[1].rows_affected,
        )
        await stats.store(itgs)


INVALID_DESTINATION_ERRORS = frozenset(
    (
        "21408",
        "21606",
        "21610",
        "21614",
        "30004",
        "30005",
        "30006",
        "30007",
        "30010",
        "30011",
        "63032",
        "63033",
    )
)
"""Errors that imply something is wrong with the recipient rather than
us or twilio
"""


async def maybe_suppress_phone_due_to_failure(
    itgs: Itgs,
    sms: Union[SMSToSend, PendingSMS],
    failure_info: SMSFailureInfo,
) -> bool:
    """Suppresses the phone number in the given SMS if the failure warrants it.

    Args:
        itgs (Itgs): the integrations to (re)use
        sms (SMSToSend, PendingSMS): the state of the sms when it failed
        failure_info (SMSFailureInfo): what went wrong

    Returns:
        bool: True if the failure warranted suppression, False otherwise
    """
    if failure_info.identifier != "ApplicationErrorOther":
        return False

    if failure_info.subidentifier not in INVALID_DESTINATION_ERRORS:
        return False

    await suppress_phone_with_reason(
        itgs,
        sms.phone_number,
        reason="Unreachable",
        reason_details={
            "identifier": failure_info.identifier,
            "subidentifier": failure_info.subidentifier,
            "extra": failure_info.extra,
        },
        now=time.time(),
    )
