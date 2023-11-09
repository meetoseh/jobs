import json
import time
from typing import List, Literal, Optional
from error_middleware import handle_contextless_error, handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import secrets
from lib.contact_methods.contact_method_stats import ContactMethodStatsPreparer
from lib.daily_reminders.registration_stats import (
    DailyReminderRegistrationStatsPreparer,
)
import lib.push.token_stats
from lib.redis_stats_preparer import RedisStatsPreparer
import redis_helpers.run_with_prep
import socket
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST
MAX_CONCURRENT_PUSH_TOKENS_PER_USER = 10
"""The maximum number of push tokens a user can have before inserting
a new one deletes the oldest one. Note that if you lower this value,
existing users will not be limited to this amount actively, but rather
they will kept at their current amount
"""

WITH_OLDEST_PUSH_TOKENS = (
    "WITH oldest_push_tokens ("
    " user_id, token"
    ") AS ("
    " SELECT"
    "  upt.user_id,"
    "  upt.token"
    " FROM user_push_tokens AS upt"
    " WHERE"
    "  NOT EXISTS ("
    "   SELECT 1 FROM user_push_tokens as upt2"
    "   WHERE"
    "    upt2.user_id = upt.user_id"
    "    AND ("
    "     upt2.last_seen_at > upt.last_seen_at"
    "     OR ("
    "      upt2.last_seen_at = upt.last_seen_at"
    "      AND upt2.id > upt.id"
    "     )"
    "    )"
    "  )"
    ") "
)


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    user_sub: str,
    expo_push_token: str,
    platform: Literal["ios", "android", "generic"],
):
    """Handles a user sending a push token to the server. The expo push token should
    be sanity checked prior to this to have a reasonable size, but otherwise this assumes
    it's untrusted.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): the sub of the user who sent the token
        expo_push_token (str): the expo push token to store
        platform ('ios', 'android', or 'generic'): the platform of the token,
            which makes it easier to identify which token is which when debugging
    """
    logging.debug(
        f"Processing {platform} push token from {user_sub}: {expo_push_token}"
    )
    conn = await itgs.conn()
    cursor = conn.cursor("none")
    now = time.time()

    new_uid = f"oseh_upt_{secrets.token_urlsafe(16)}"
    new_udr_uid = f"oseh_udr_{secrets.token_urlsafe(16)}"

    reassign_delete_cml_uid = f"oseh_cml_{secrets.token_urlsafe(16)}"
    reassign_create_cml_uid = f"oseh_cml_{secrets.token_urlsafe(16)}"
    reassign_excessive_delete_uid = f"oseh_cml_{secrets.token_urlsafe(16)}"
    excessive_delete_cml_uid = f"oseh_cml_{secrets.token_urlsafe(16)}"
    new_create_cml_uid = f"oseh_cml_{secrets.token_urlsafe(16)}"

    cml_reason = json.dumps(
        {
            "repo": "jobs",
            "file": __name__,
        }
    )

    response = await cursor.executemany3(
        (
            # LOGGING
            # If we're going to reassign, log that we're deleting a push token for
            # the other user
            (
                "INSERT INTO contact_method_log ("
                " uid, user_id, channel, identifier, action, reason, created_at"
                ") "
                "SELECT"
                " ?,"
                " from_users.id,"
                " 'push',"
                " ?,"
                " 'delete',"
                " json_insert(?, '$.reason', 'reassign', '$.context.to', to_users.sub),"
                " ? "
                "FROM users AS to_users, users AS from_users "
                "WHERE"
                " to_users.sub = ?"
                " AND EXISTS ("
                "  SELECT 1 FROM user_push_tokens AS upt"
                "  WHERE"
                "   upt.user_id = from_users.id"
                "   AND upt.user_id <> to_users.id"
                "   AND upt.token = ?"
                " )",
                (
                    reassign_delete_cml_uid,
                    expo_push_token,
                    cml_reason,
                    now,
                    user_sub,
                    expo_push_token,
                ),
            ),
            # If we're going to reassign, log that we're creating a push token for
            # the new user
            (
                "INSERT INTO contact_method_log ("
                " uid, user_id, channel, identifier, action, reason, created_at"
                ") "
                "SELECT"
                " ?,"
                " to_users.id,"
                " 'push',"
                " ?,"
                " 'create_unverified',"
                " json_insert(?, '$.reason', 'reassign', '$.context.from', from_users.sub),"
                " ? "
                "FROM users AS to_users, users AS from_users "
                "WHERE"
                " to_users.sub = ?"
                " AND EXISTS ("
                "  SELECT 1 FROM user_push_tokens AS upt"
                "  WHERE"
                "   upt.user_id = from_users.id"
                "   AND upt.user_id <> to_users.id"
                "   AND upt.token = ?"
                " )",
                (
                    reassign_create_cml_uid,
                    expo_push_token,
                    cml_reason,
                    now,
                    user_sub,
                    expo_push_token,
                ),
            ),
            # If we're going to reassign, and the new user will have too many push tokens,
            # log that we're deleting the oldest push token for the new user
            (
                WITH_OLDEST_PUSH_TOKENS + "INSERT INTO contact_method_log ("
                " uid, user_id, channel, identifier, action, reason, created_at"
                ") "
                "SELECT"
                " ?,"
                " to_users.id,"
                " 'push',"
                " to_oldest_push_tokens.token,"
                " 'delete',"
                " json_insert(?, '$.reason', 'reassign_excessive', '$.context.from', from_users.sub),"
                " ? "
                "FROM users AS to_users, users AS from_users, oldest_push_tokens AS to_oldest_push_tokens "
                "WHERE"
                " to_users.sub = ?"
                " AND to_users.id = to_oldest_push_tokens.user_id"
                " AND EXISTS ("
                "  SELECT 1 FROM user_push_tokens AS upt"
                "  WHERE"
                "   upt.user_id = from_users.id"
                "   AND upt.user_id <> to_users.id"
                "   AND upt.token = ?"
                " )"
                " AND ("
                "  SELECT COUNT(*) FROM user_push_tokens AS upt"
                "  WHERE"
                "   upt.user_id = to_users.id"
                " ) >= ?",
                (
                    reassign_excessive_delete_uid,
                    cml_reason,
                    now,
                    user_sub,
                    expo_push_token,
                    MAX_CONCURRENT_PUSH_TOKENS_PER_USER,
                ),
            ),
            # If we're going to create a new token, and the user will have too many
            # push tokens, log that we're deleting the oldest push token for the user
            (
                WITH_OLDEST_PUSH_TOKENS + "INSERT INTO contact_method_log ("
                " uid, user_id, channel, identifier, action, reason, created_at"
                ") "
                "SELECT"
                " ?,"
                " users.id,"
                " 'push',"
                " oldest_push_tokens.token,"
                " 'delete',"
                " json_insert(?, '$.reason', 'excessive'),"
                " ? "
                "FROM users, oldest_push_tokens "
                "WHERE"
                " users.sub = ?"
                " AND oldest_push_tokens.user_id = users.id"
                " AND NOT EXISTS ("
                "  SELECT 1 FROM user_push_tokens"
                "  WHERE"
                "   user_push_tokens.token = ?"
                " )"
                " AND ("
                "  SELECT COUNT(*) FROM user_push_tokens AS upt"
                "  WHERE"
                "   upt.user_id = users.id"
                " ) >= ?",
                (
                    excessive_delete_cml_uid,
                    cml_reason,
                    now,
                    user_sub,
                    expo_push_token,
                    MAX_CONCURRENT_PUSH_TOKENS_PER_USER,
                ),
            ),
            # If we're going to create a new token log that we're creating it
            (
                "INSERT INTO contact_method_log ("
                " uid, user_id, channel, identifier, action, reason, created_at"
                ") "
                "SELECT"
                " ?,"
                " users.id,"
                " 'push',"
                " ?,"
                " 'create_unverified',"
                " json_insert(?, '$.reason', 'create'),"
                " ? "
                "FROM users "
                "WHERE"
                " users.sub = ?"
                " AND NOT EXISTS ("
                "  SELECT 1 FROM user_push_tokens"
                "  WHERE"
                "   user_push_tokens.token = ?"
                " )",
                (
                    new_create_cml_uid,
                    expo_push_token,
                    cml_reason,
                    now,
                    user_sub,
                    expo_push_token,
                ),
            ),
            #
            # ACTION
            # Refresh if it already exists and is for this user
            (
                "UPDATE user_push_tokens "
                "SET last_seen_at = ?, updated_at = ? "
                "WHERE"
                " EXISTS ("
                "  SELECT 1 FROM users"
                "  WHERE"
                "   users.id = user_push_tokens.user_id"
                "   AND users.sub = ?"
                " )"
                " AND user_push_tokens.token = ?",
                (now, now, user_sub, expo_push_token),
            ),
            # If we're going to reassign and that causes the other user not to
            # have any push tokens anymore, delete their push daily reminder
            (
                "DELETE FROM user_daily_reminders "
                "WHERE"
                " user_daily_reminders.channel = 'push'"
                " AND EXISTS ("
                "  SELECT 1 FROM users"
                "  WHERE"
                "   users.id = user_daily_reminders.user_id"
                "   AND users.sub != ?"
                " )"
                " AND EXISTS ("
                "  SELECT 1 FROM user_push_tokens AS upt"
                "  WHERE"
                "   upt.user_id = user_daily_reminders.user_id"
                "   AND upt.token = ?"
                " )"
                " AND NOT EXISTS ("
                "  SELECT 1 FROM user_push_tokens AS upt"
                "  WHERE"
                "   upt.user_id = user_daily_reminders.user_id"
                "   AND upt.token != ?"
                " )",
                (user_sub, expo_push_token, expo_push_token),
            ),
            # If we're about to reassign and it will cause excessively many push tokens
            # for this new user, delete the oldest one
            (
                "DELETE FROM user_push_tokens "
                "WHERE"
                " EXISTS ("
                "  SELECT 1 FROM contact_method_log"
                "  WHERE"
                "   contact_method_log.uid = ?"
                "   AND user_push_tokens.token = contact_method_log.identifier"
                " )",
                (reassign_excessive_delete_uid,),
            ),
            # Reassign if it already exists for a different user
            (
                "UPDATE user_push_tokens "
                "SET user_id = users.id, last_seen_at = ?, updated_at = ? "
                "FROM users "
                "WHERE"
                " users.sub = ?"
                " AND user_push_tokens.user_id != users.id"
                " AND user_push_tokens.token = ?",
                (now, now, user_sub, expo_push_token),
            ),
            # Delete excessive tokens if we're about to add a new one
            (
                "DELETE FROM user_push_tokens "
                "WHERE"
                " EXISTS ("
                "  SELECT 1 FROM contact_method_log"
                "  WHERE"
                "   contact_method_log.uid = ?"
                "   AND user_push_tokens.token = contact_method_log.identifier"
                " )",
                (excessive_delete_cml_uid,),
            ),
            # Add a new token if it doesn't exist
            (
                """
                INSERT INTO user_push_tokens (
                    uid,
                    user_id,
                    platform,
                    token,
                    receives_notifications,
                    created_at,
                    updated_at,
                    last_seen_at,
                    last_confirmed_at
                )
                SELECT
                    ?, users.id, ?, ?, ?, ?, ?, ?, NULL
                FROM users
                WHERE
                    users.sub = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.token = ?
                    )
                """,
                (
                    new_uid,
                    platform,
                    expo_push_token,
                    1,
                    now,
                    now,
                    now,
                    user_sub,
                    expo_push_token,
                ),
            ),
            # Register for daily push notifications if they are supposed to be
            # registered now (full check, including settings)
            (
                "INSERT INTO user_daily_reminders ("
                " uid, user_id, channel, start_time, end_time, day_of_week_mask, created_at"
                ") "
                "SELECT"
                " ?,"
                " users.id,"
                " 'push',"
                " CASE"
                "  WHEN settings.id IS NULL THEN 21600"
                "  WHEN json_extract(settings.time_range, '$.type') = 'preset' THEN"
                "   CASE json_extract(settings.time_range, '$.preset')"
                "    WHEN 'afternoon' THEN 46800"
                "    WHEN 'evening' THEN 61200"
                "    ELSE 21600"
                "   END"
                "  WHEN json_extract(settings.time_range, '$.type') = 'explicit' THEN"
                "   json_extract(settings.time_range, '$.start')"
                "  ELSE 21600"
                " END,"
                " CASE"
                "  WHEN settings.id IS NULL THEN 39600"
                "  WHEN json_extract(settings.time_range, '$.type') = 'preset' THEN"
                "   CASE json_extract(settings.time_range, '$.preset')"
                "    WHEN 'afternoon' THEN 57600"
                "    WHEN 'evening' THEN 68400"
                "    ELSE 39600"
                "   END"
                "  WHEN json_extract(settings.time_range, '$.type') = 'explicit' THEN"
                "   json_extract(settings.time_range, '$.end')"
                "  ELSE 39600"
                " END,"
                " COALESCE(settings.day_of_week_mask, 127),"
                " ? "
                "FROM users "
                "LEFT OUTER JOIN user_daily_reminder_settings AS settings "
                "ON settings.id = ("
                " SELECT s.id FROM user_daily_reminder_settings AS s"
                " WHERE"
                "  s.user_id = users.id"
                "  AND (s.channel = 'push' OR s.day_of_week_mask <> 0)"
                " ORDER BY"
                "  s.channel = 'push' DESC,"
                "  CASE json_extract(s.time_range, '$.type')"
                "   WHEN 'explicit' THEN 0"
                "   WHEN 'preset' THEN 1"
                "   ELSE 2"
                "  END ASC,"
                "  (s.day_of_week_mask & 1 > 0) + (s.day_of_week_mask & 2 > 0) + (s.day_of_week_mask & 4 > 0) + (s.day_of_week_mask & 8 > 0) + (s.day_of_week_mask & 16 > 0) + (s.day_of_week_mask & 32 > 0) + (s.day_of_week_mask & 64 > 0) ASC,"
                "  CASE s.channel"
                "   WHEN 'push' THEN 0"
                "   WHEN 'sms' THEN 1"
                "   WHEN 'email' THEN 2"
                "   ELSE 3"
                "  END ASC"
                "  LIMIT 1"
                ") "
                "WHERE"
                " users.sub = ?"
                " AND EXISTS ("
                "  SELECT 1 FROM user_push_tokens AS upt"
                "  WHERE upt.token = ? AND upt.user_id = users.id"
                " )"
                " AND NOT EXISTS ("
                "  SELECT 1 FROM user_daily_reminders AS udr"
                "  WHERE udr.user_id = users.id AND udr.channel = 'push'"
                " )"
                " AND (settings.day_of_week_mask IS NULL OR settings.day_of_week_mask <> 0)",
                (new_udr_uid, now, user_sub, expo_push_token),
            ),
        ),
        transaction=True,
    )

    affected = [r.rows_affected is not None and r.rows_affected > 0 for r in response]

    if any(a and r.rows_affected != 1 for (a, r) in zip(affected, response)):
        await handle_warning(
            f"{__name__}:multiple_rows_affected",
            f"Expected 0 or 1 row affected per query, but got multiple\n\n```\n{affected=}\n```\n",
        )

    await _sanity_warnings(itgs, *affected)

    (
        reassign_delete_logged,
        reassign_create_logged,
        reassign_excessive_logged,
        excessive_delete_logged,
        create_logged,
        refreshed,
        reassign_deleted_reminder,
        reassign_deleted_excessive,
        reassigned,
        excessive_deleted,
        created,
        created_reminder,
    ) = affected

    unix_date = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )
    action_taken: Optional[Literal["refresh", "reassign", "create"]] = None
    excess_deleted: int = int(reassign_deleted_excessive) + int(excessive_deleted)

    stats = RedisStatsPreparer()

    if reassign_delete_logged:
        ContactMethodStatsPreparer(stats).incr_deleted(
            unix_date, channel="push", reason="reassigned"
        )

    if reassign_create_logged or create_logged:
        ContactMethodStatsPreparer(stats).incr_created(
            unix_date,
            channel="push",
            verified=False,
            enabled=True,
            reason="app",
            amt=int(reassign_create_logged) + int(create_logged),
        )

    if reassign_excessive_logged or excessive_delete_logged:
        ContactMethodStatsPreparer(stats).incr_deleted(
            unix_date,
            channel="push",
            reason="excessive",
            amt=int(reassign_excessive_logged) + int(excessive_delete_logged),
        )

    if reassign_deleted_reminder:
        stats.merge_with(
            DailyReminderRegistrationStatsPreparer().incr_unsubscribed(
                unix_date, "push", "unreachable"
            )
        )

    if created_reminder:
        stats.merge_with(
            DailyReminderRegistrationStatsPreparer().incr_subscribed(
                unix_date, "push", "push_token_added"
            )
        )

    if refreshed:
        action_taken = "refresh"
    elif reassigned:
        action_taken = "reassign"
    elif created:
        action_taken = "create"

    if action_taken is None:
        await handle_contextless_error(
            extra_info=f"no action taken for expo push token? {expo_push_token=}, {user_sub=}"
        )

    redis = await itgs.redis()

    async def prep(force: bool):
        await lib.push.token_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await stats.write_earliest(pipe)
            await stats.write_increments(pipe)
            if action_taken == "refresh":
                await lib.push.token_stats.attempt_increment_event(
                    pipe, event="refreshed", now=now
                )
            elif action_taken == "reassign":
                await lib.push.token_stats.attempt_increment_event(
                    pipe, event="reassigned", now=now
                )
            elif action_taken == "create":
                await lib.push.token_stats.attempt_increment_event(
                    pipe, event="created", now=now
                )

            if excess_deleted > 0:
                await lib.push.token_stats.attempt_increment_event(
                    pipe,
                    event="deleted_due_to_token_limit",
                    now=now,
                    amount=excess_deleted,
                )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)
    logging.info(
        f"Handled {platform=} {expo_push_token=} for {user_sub=}: {action_taken=}, {excess_deleted=}"
    )


async def _sanity_warnings(
    itgs: Itgs,
    reassign_delete_logged,
    reassign_create_logged,
    reassign_excessive_logged,
    excessive_delete_logged,
    create_logged,
    refreshed,
    reassign_deleted_reminder,
    reassign_deleted_excessive,
    reassigned,
    excessive_deleted,
    created,
    created_reminder,
) -> None:
    """Emits a warning if the result of the main query is unexpected"""
    issues: List[str] = []

    if reassign_delete_logged is not reassigned:
        issues.append("reassign_delete_logged is not reassigned")

    if reassign_create_logged is not reassigned:
        issues.append("reassign_create_logged is not reassigned")

    if reassign_excessive_logged and not reassigned:
        issues.append("reassign_excessive_logged is True but reassigned is False")

    if reassign_excessive_logged is not reassign_deleted_excessive:
        issues.append("reassign_excessive_logged is not reassign_deleted_excessive")

    if excessive_delete_logged is not excessive_deleted:
        issues.append("excessive_delete_logged is not excessive_deleted")

    if create_logged is not created:
        issues.append("create_logged is not created")

    if not reassigned and reassign_deleted_reminder:
        issues.append("reassign_deleted_reminderis True but reassigned is False")

    if not created and not reassigned and created_reminder:
        issues.append(
            "created_reminder is True but created is False and reassigned is False"
        )

    if int(reassigned) + int(created) + int(refreshed) != 1:
        issues.append("not exactly one of reassigned, created, refreshed is True")

    if not issues:
        return

    msg = (
        f"{socket.gethostname()} Unexpected result from create_push_tokens query:\n\n"
        f"```\n"
        f"{reassign_delete_logged=}\n"
        f"{reassign_create_logged=}\n"
        f"{reassign_excessive_logged=}\n"
        f"{excessive_delete_logged=}\n"
        f"{create_logged=}\n"
        f"{refreshed=}\n"
        f"{reassign_deleted_reminder=}\n"
        f"{reassign_deleted_excessive=}\n"
        f"{reassigned=}\n"
        f"{excessive_deleted=}\n"
        f"{created=}\n"
        f"{created_reminder=}\n"
        f"```\n"
        "Issues:\n"
        " •  " + "\n •  ".join(issues)
    )
    logging.warning(msg)

    try:
        slack = await itgs.slack()
        await slack.send_web_error_message(msg)
    except:
        logging.exception("Failed to send slack message")


if __name__ == "__main__":
    import asyncio

    async def main():
        user_sub = input("User sub: ")
        expo_push_token = input("Expo push token: ")
        platform = input("Platform: ")
        if platform not in ("ios", "android", "generic"):
            print("Invalid platform (want: ios, android, generic)")
            return
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.push.create_push_token",
                user_sub=user_sub,
                expo_push_token=expo_push_token,
                platform=platform,
            )

    asyncio.run(main())
