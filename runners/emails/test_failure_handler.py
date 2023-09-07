import time
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.email.email_info import decode_data_for_failure_job
from lib.email.handler import retry_or_abandon_standard
from runners.emails.abandoned_email_callback import suppress_email
import socket

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str):
    """A barebones failure handler for emails. This should generally not be
    used as it's very low-level, i.e., it never ties the failure to a particular
    user.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded data from the emails system
    """
    now = time.time()
    email, info = decode_data_for_failure_job(data_raw)

    result = await retry_or_abandon_standard(itgs, email, info, now=now)
    if result.should_ignore:
        return

    logging.info(
        f"Failed to send {email.message_id} ({email.subject}); {info.error_identifier}"
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
        slack = await itgs.slack()
        await slack.send_ops_message(
            f"{socket.gethostname()} Suppressed {email.email} due to {info.error_identifier} ({info.extra})",
            preview=f"Suppressed {email.email}",
        )
