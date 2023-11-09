from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.emails.email_info import decode_data_for_success_job
import time

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, data_raw: str, uid: str):
    """The email failure handler for sign in with oseh. Stores the success
    in the siwo email log table.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        data_raw (str): The encoded email and success information
        uid (str): The uid assigned to the log entry within `siwo_email_log`
    """
    email, info = decode_data_for_success_job(data_raw)
    now = time.time()
    logging.info(
        f"Sign in with Oseh email {email.message_id} ({email.subject}) was successfully "
        f"delivered to {email.email} using {email.template}:\n{email=}, {info=}"
    )

    conn = await itgs.conn()
    cursor = conn.cursor()
    response = await cursor.execute(
        "UPDATE siwo_email_log SET succeeded_at=? WHERE uid=?",
        (now, uid),
    )

    if response.rows_affected != 1:
        await handle_warning(
            f"{__name__}:log_not_updated",
            f"Sign in with Oseh Email Success Handler updated {repr(response.rows_affected)} rows (expected 1)",
        )
