from typing import List, Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.email.events import EmailEvent
import secrets
import time
import socket

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
    for the given email.

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
    await cursor.executemany3(
        (
            (
                """
                INSERT INTO email_failures (
                    uid, email_address, failure_type, failure_extra, created_at
                )
                """,
                (
                    f"oseh_ef_{secrets.token_urlsafe(16)}",
                    email,
                    type_,
                    failure_extra,
                    now,
                ),
            ),
            (
                """
                INSERT INTO suppressed_emails (
                    uid, email_address, reason, created_at
                )
                SELECT
                    ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM suppressed_emails WHERE email_address = ?
                )
                """,
                (
                    f"oseh_se_{secrets.token_urlsafe(16)}",
                    email,
                    type_,
                    now,
                    email,
                ),
            ),
        )
    )
