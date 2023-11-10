from typing import List, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.emails.events import EmailEvent
from lib.daily_reminders.registration_stats import (
    DailyReminderRegistrationStatsPreparer,
)
import secrets
import time
import socket
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, event_obj: dict):
    """Invoked when an email not in the receipt pending set experiences a bounce
    or complaint notification. This _usually_ is only called with complaint
    notifications, since a user can successfully receive an email and then later
    complain about it / report it as spam.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        event (dict): the jsonified EmailEvent that was received
    """
    event = EmailEvent.parse_obj(event_obj)
    logging.warning(f"abandoned email {event.message_id} -> {event.notification.type}")

    if event.notification.type == "Delivery":
        return

    failure_extra: Optional[str] = None
    emails: List[str] = []
    if event.notification.type == "Bounce":
        failure_extra = (
            f"{event.notification.reason.primary}/{event.notification.reason.secondary}"
        )
        emails = event.notification.bounced_recipients
    elif event.notification.type == "Complaint":
        failure_extra = event.notification.feedback_type
        emails = event.notification.complained_recipients
    else:
        logging.warning(f"unknown notification type {event}")
        return

    hostname = socket.gethostname()

    now = time.time()
    slack = await itgs.slack()
    slack_suppressed = False

    for i, email in enumerate(emails):
        logging.info(f"Suppressing email {email} due to {event.notification.type}")
        await suppress_email(
            itgs,
            type_=event.notification.type,
            email=email,
            failure_extra=failure_extra,
            now=now,
        )

        if not slack_suppressed and i < 3:
            try:
                await slack.send_ops_message(
                    f"{hostname} Suppressed email {email} due to {event.notification.type}",
                    preview=f"Suppressed {email}",
                )
            except Exception:
                logging.exception("failed to send ops message about suppressing email")
                slack_suppressed = True

    if not slack_suppressed and len(emails) > 3:
        await slack.send_ops_message(
            f"{hostname} Suppressed {len(emails)-3} additional emails due to {event.notification.type}",
            preview=f"Suppressed many emails",
        )


async def suppress_email(
    itgs: Itgs, *, type_: str, email: str, failure_extra: Optional[str], now: float
):
    """Stores a failure in email_failures and a suppression in suppressed_emails
    for the given email, and updates email_verified on any user that has that
    email address to 0.

    Args:
        itgs (Itgs): the integrations to (re)use
        type_ (str): the type of failure, e.g., Bounce or Complaint. See email_failures
            table for more details.
        email (str): the email address that failed
        failure_extra (Optional[str]): extra information about the failure, e.g., the
            reason for the bounce.
    """
    conn = await itgs.conn()
    cursor = conn.cursor()
    response = await cursor.executemany3(
        (
            (
                "INSERT INTO email_failures ("
                " uid, email_address, failure_type, failure_extra, created_at"
                ") "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    f"oseh_ef_{secrets.token_urlsafe(16)}",
                    email,
                    type_,
                    failure_extra,
                    now,
                ),
            ),
            (
                "INSERT INTO suppressed_emails ("
                " uid, email_address, reason, created_at"
                ") "
                "SELECT"
                " ?, ?, ?, ?"
                "WHERE"
                " NOT EXISTS ("
                "  SELECT 1 FROM suppressed_emails WHERE email_address = ? COLLATE NOCASE"
                " )",
                (
                    f"oseh_se_{secrets.token_urlsafe(16)}",
                    email,
                    type_,
                    now,
                    email,
                ),
            ),
            (
                "DELETE FROM user_daily_reminders "
                "WHERE"
                " channel = 'email'"
                " AND EXISTS ("
                "  SELECT 1 FROM user_email_addresses"
                "  WHERE"
                "   user_email_addresses.user_id = user_daily_reminders.user_id"
                "   AND user_email_addresses.email = ? COLLATE NOCASE"
                "   AND NOT EXISTS ("
                "    SELECT 1 FROM user_email_addresses AS uea"
                "    WHERE"
                "     uea.id <> user_email_addresses.id"
                "     AND uea.user_id = user_daily_reminders.user_id"
                "     AND uea.receives_notifications"
                "     AND uea.verified"
                "     AND NOT EXISTS ("
                "      SELECT 1 FROM suppressed_emails"
                "      WHERE suppressed_emails.email_address = uea.email COLLATE NOCASE"
                "     )"
                "   )"
                " )",
                (email,),
            ),
        )
    )

    if response[0].rows_affected != 1:
        await handle_warning(
            f"{__name__}:email_failure_not_logged",
            f"Expected `{response[0].rows_affected=}` to be 1",
        )

    if response[1].rows_affected is None or response[1].rows_affected == 0:
        logging.info(f"Email {email=} was already suppressed")
    elif response[1].rows_affected != 1:
        await handle_warning(
            f"{__name__}:suppressed_multiple",
            f"Expected `{response[1].rows_affected=}` to be 1",
        )

    if response[2].rows_affected is not None and response[2].rows_affected > 0:
        logging.info(
            f"Unsubscribed {response[2].rows_affected} users from email reminders while suppressing {email=}"
        )

        stats = DailyReminderRegistrationStatsPreparer()
        stats.incr_unsubscribed(
            unix_dates.unix_timestamp_to_unix_date(
                now, tz=pytz.timezone("America/Los_Angeles")
            ),
            "email",
            "unreachable",
            amt=response[2].rows_affected,
        )
        await stats.store(itgs)
