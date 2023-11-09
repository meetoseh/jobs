import time
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.emails.email_info import decode_data_for_failure_job
from lib.emails.handler import retry_or_abandon_standard
from runners.emails.abandoned_email_callback import suppress_email
import socket

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str, uid: str):
    """The email failure handler for sign in with oseh. Stores the failure
    in the siwo email log table.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded data from the emails system
        uid (str): The uid assigned to the log entry within `siwo_email_log`
    """
    now = time.time()
    email, info = decode_data_for_failure_job(data_raw)

    result = await retry_or_abandon_standard(itgs, email, info, now=now)
    if result.should_ignore:
        return

    logging.info(
        f"Sign in with Oseh Failed to send to {email.email} ({email.subject}); {info.error_identifier}"
        f"; retryable? {info.retryable}, retried? {result.wanted_to_retry}\n{email=}, {info=}"
    )

    if info.error_identifier in ("Bounce", "Complaint"):
        logging.info(
            f"Since this is a {info.error_identifier}, suppressing the corresponding email address {email.email}"
        )
        await suppress_email(
            itgs,
            type_=info.error_identifier,
            email=email.email,
            failure_extra=info.extra,
            now=now,
        )
        try:
            slack = await itgs.slack()
            await slack.send_ops_message(
                f"{socket.gethostname()} Sign in with Oseh - suppressed {email.email} due to {info.error_identifier} ({info.extra})",
                preview=f"Suppressed {email.email}",
            )
        except:
            logging.exception("Failed to send slack message")

    conn = await itgs.conn()
    cursor = conn.cursor()

    response = await cursor.execute(
        """
        UPDATE siwo_email_log
        SET failed_at=?, failure_data_raw=?
        WHERE uid=?
        """,
        (now, data_raw, uid),
    )

    if response.rows_affected != 1:
        await handle_warning(
            f"{__name__}:log_not_updated",
            f"Sign in with Oseh Email Failure Handler updated {repr(response.rows_affected)} rows (expected 1)",
        )
