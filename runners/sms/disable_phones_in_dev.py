"""Disables all but the test phone number in development; call once per day
to keep the number of phones down
"""
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import os
import secrets
import time
from lib.contact_methods.contact_method_stats import ContactMethodStatsPreparer
from lib.daily_reminders.registration_stats import (
    DailyReminderRegistrationStatsPreparer,
)
from lib.redis_stats_preparer import RedisStatsPreparer
import unix_dates
import pytz
from jobs import JobCategory
import json
import logging


category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""

TEST_NUMBER = "+15555555555"


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Disables every phone number except `TEST_NUMBER`, keeping statistics accurate
    and updating `user_daily_reminders`, to avoid constantly texting ourselves!

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    if os.environ["ENVIRONMENT"] != "dev":
        return

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    tz = pytz.timezone("America/Los_Angeles")
    cml_reason = json.dumps({"repo": "jobs", "file": __name__})

    while True:
        response = await cursor.execute(
            "SELECT upn.uid FROM user_phone_numbers AS upn "
            "WHERE "
            " upn.phone_number <> ?"
            " AND upn.verified"
            " AND upn.receives_notifications "
            "ORDER BY upn.uid ASC "
            "LIMIT 100",
            (TEST_NUMBER,),
        )

        if not response.results:
            break

        for (row_upn_uid,) in response.results:
            now = time.time()
            unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)
            cml_uid = f"oseh_cml_{secrets.token_urlsafe(16)}"

            response = await cursor.executemany3(
                (
                    (
                        "INSERT INTO contact_method_log ("
                        " uid, user_id, channel, identifier, action, reason, created_at"
                        ") "
                        "SELECT"
                        " ?, users.id, 'phone', user_phone_numbers.phone_number, 'disable_notifs', ?, ? "
                        "FROM users, user_phone_numbers "
                        "WHERE"
                        " user_phone_numbers.uid = ?"
                        " AND user_phone_numbers.user_id = users.id"
                        " AND user_phone_numbers.phone_number <> ?"
                        " AND user_phone_numbers.verified"
                        " AND user_phone_numbers.receives_notifications",
                        (
                            cml_uid,
                            cml_reason,
                            now,
                            row_upn_uid,
                            TEST_NUMBER,
                        ),
                    ),
                    (
                        "UPDATE user_phone_numbers "
                        "SET receives_notifications = 0 "
                        "WHERE"
                        " uid = ?"
                        " AND phone_number <> ?"
                        " AND verified"
                        " AND receives_notifications",
                        (row_upn_uid, TEST_NUMBER),
                    ),
                    (
                        "DELETE FROM user_daily_reminders "
                        "WHERE"
                        " channel = 'sms'"
                        " AND EXISTS ("
                        "  SELECT 1 FROM user_phone_numbers"
                        "  WHERE"
                        "   user_phone_numbers.user_id = user_daily_reminders.user_id"
                        "   AND user_phone_numbers.uid = ?"
                        " )"
                        " AND NOT EXISTS ("
                        "  SELECT 1 FROM user_phone_numbers"
                        "  WHERE"
                        "   user_phone_numbers.user_id = user_daily_reminders.user_id"
                        "   AND user_phone_numbers.verified"
                        "   AND user_phone_numbers.receives_notifications"
                        "   AND NOT EXISTS ("
                        "    SELECT 1 FROM suppressed_phone_numbers"
                        "    WHERE suppressed_phone_numbers.phone_number = user_phone_numbers.phone_number"
                        "   )"
                        " )",
                        (row_upn_uid,),
                    ),
                )
            )

            def debug_info():
                return (
                    "Odd response from disabling phone number "
                    f"`row_upn_uid={row_upn_uid}`:\n\n"
                    f"```\nresponse={response}\n```"
                )

            affected = [
                r.rows_affected is not None and r.rows_affected > 0 for r in response
            ]
            if any(a and r.rows_affected != 1 for (a, r) in zip(affected, response)):
                await handle_warning(f"{__name__}:multiple_rows_affected", debug_info())

            (logged_disable, disabled, deleted_reminder) = affected
            if logged_disable is not disabled:
                await handle_warning(f"{__name__}:log_mismatch", debug_info())

            stats = RedisStatsPreparer()

            if disabled:
                logging.info(f"Disabled phone number {row_upn_uid}")
                ContactMethodStatsPreparer(stats).incr_disabled(
                    unix_date, channel="phone", reason="dev_auto_disable"
                )

            if deleted_reminder:
                logging.info(
                    f"Deleted sms user daily reminders for user associated with {row_upn_uid}"
                )
                stats.merge_with(
                    DailyReminderRegistrationStatsPreparer().incr_unsubscribed(
                        unix_date, "sms", "unreachable"
                    )
                )

            await stats.store(itgs)


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sms.disable_phones_in_dev")

    asyncio.run(main())
