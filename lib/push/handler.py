import json
from typing import Optional, Union
from error_middleware import handle_warning
from lib.contact_methods.contact_method_stats import ContactMethodStatsPreparer
from lib.daily_reminders.registration_stats import (
    DailyReminderRegistrationStatsPreparer,
)
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
import secrets
import pytz
from lib.redis_stats_preparer import RedisStatsPreparer
from lib.shared.clean_for_slack import clean_for_slack
import lib.push.token_stats
import unix_dates


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


async def delete_push_token(
    itgs: Itgs, *, token: str, now: float, cml_reason: str, token_stats_event: str
) -> None:
    """Removes the given stored expo push token, updating user daily reminders
    and stats as appropriate. This should be called when DeviceNotRegistered is
    encountered to avoid sending push notifications to a device that has
    uninstalled the app.

    This is a low-level method; it's generally more appropriate to use
    `maybe_delete_push_token_due_to_failure` which will decide whether to delete
    the push token and format the necessary contact method log entry.

    Args:
        itgs (Itgs): the integrations to (re)use
        token (str): the token to remove
        now (float): the canonical current time the deletion occurred
        cml_reason (str): the reason to use for the contact method log entry
            deleting the push token, if it's needed
        token_stats_event (str): if a push token is deleted, which event
            to increment in the push token stats. Usually one of
            `deleted_due_to_unrecognized_ticket` or
            `deleted_due_to_unrecognized_receipt`
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    cml_uid = f"oseh_cml_{secrets.token_urlsafe(16)}"
    now = time.time()
    unix_date = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )

    response = await cursor.executemany3(
        (
            (
                """
                DELETE FROM user_daily_reminders
                WHERE
                    EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.token = ?
                        AND upt.user_id = user_daily_reminders.user_id
                        AND NOT EXISTS (
                            SELECT 1 FROM user_push_tokens AS upt2
                            WHERE upt.id <> upt2.id
                            AND upt2.user_id = user_daily_reminders.user_id
                            AND upt2.receives_notifications
                        )
                    )
                """,
                (token,),
            ),
            (
                "INSERT INTO contact_method_log ("
                " uid, user_id, channel, identifier, action, reason, created_at"
                ") "
                "SELECT"
                " ?, user_push_tokens.user_id, 'push', user_push_tokens.token, 'delete', ?, ? "
                "FROM user_push_tokens "
                "WHERE"
                " user_push_tokens.token = ?",
                (
                    cml_uid,
                    cml_reason,
                    now,
                    token,
                ),
            ),
            (
                "DELETE FROM user_push_tokens WHERE token=?",
                (token,),
            ),
        )
    )

    def debug_info():
        return (
            "Odd response from deleting push token "
            f"`attempt.push_token={clean_for_slack(token)}`:\n\n"
            f"```\nresponse={clean_for_slack(repr(response))}\n```"
        )

    affected = [r.rows_affected is not None and r.rows_affected > 0 for r in response]
    if any(a and r.rows_affected != 1 for (a, r) in zip(affected, response)):
        await handle_warning(f"{__name__}:multiple_rows_affected", debug_info())

    (deleted_reminder, logged_delete, deleted_token) = affected

    if deleted_reminder and not deleted_token:
        await handle_warning(f"{__name__}:reminder_deleted_but_not_token", debug_info())

    if logged_delete is not deleted_token:
        await handle_warning(f"{__name__}:log_mismatch", debug_info())

    stats = RedisStatsPreparer()

    if deleted_reminder:
        logging.debug("Deleted reminder associated with push token")
        stats.merge_with(
            DailyReminderRegistrationStatsPreparer().incr_unsubscribed(
                unix_date, "push", "unreachable"
            )
        )

    if deleted_token:
        logging.debug("Deleted push token")
        ContactMethodStatsPreparer(stats).incr_deleted(
            unix_date, "push", "device_not_registered"
        )

        await lib.push.token_stats.increment_event(
            itgs, event=token_stats_event, now=now
        )
    else:
        logging.debug("The push token had already been deleted")

    await stats.store(itgs)


async def maybe_delete_push_token_due_to_failure(
    itgs: Itgs,
    *,
    attempt: Union[MessageAttemptToCheck, MessageAttemptToSend],
    failure_info: MessageAttemptFailureInfo,
    file: str,
) -> bool:
    """Deletes the push token associated with the given attempt, if
    appropriate.

    Args:
        itgs (Itgs): the integrations to (re)use
        attempt (Union[MessageAttemptToCheck, MessageAttemptToSend]): the attempt to check
        failure_info (MessageAttemptFailureInfo): the failure information
        file (str): the file in which the failure handler is located, for the
            contact method log entry

    Returns:
        bool: True if the failure warranted deleting the push token, false
            otherwise
    """
    if failure_info.identifier != "DeviceNotRegistered":
        return

    cml_reason = json.dumps(
        {
            "repo": "jobs",
            "file": file,
            "reason": failure_info.identifier,
            "context": {"extra": failure_info.extra},
        }
    )

    if failure_info.action == "send":
        token_stats_event = "deleted_due_to_unrecognized_ticket"
    else:
        token_stats_event = "deleted_due_to_unrecognized_receipt"

    await delete_push_token(
        itgs,
        token=attempt.push_token,
        now=time.time(),
        cml_reason=cml_reason,
        token_stats_event=token_stats_event,
    )
    return True
