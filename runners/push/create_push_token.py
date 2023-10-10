import time
from typing import Literal, Optional
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import secrets
from lib.daily_reminders.registration_stats import (
    DailyReminderRegistrationStatsPreparer,
)
import lib.push.token_stats
import redis_helpers.run_with_prep
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST
MAX_CONCURRENT_PUSH_TOKENS_PER_USER = 10


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
    response = await cursor.executemany3(
        (
            # Refresh if it already exists and is for this user
            (
                """
                UPDATE user_push_tokens
                SET last_seen_at = ?, updated_at = ?
                WHERE
                    EXISTS (
                        SELECT 1 FROM users
                        WHERE users.id = user_push_tokens.user_id
                          AND users.sub = ?
                    )
                    AND user_push_tokens.token = ?
                """,
                (now, now, user_sub, expo_push_token),
            ),
            # If we're going to reassign and that causes the other user not to
            # have any push tokens anymore, delete their push daily reminder
            (
                """
                DELETE FROM user_daily_reminders
                WHERE
                    user_daily_reminders.channel = 'push'
                    AND EXISTS (
                        SELECT 1 FROM users
                        WHERE users.id = user_daily_reminders.user_id
                          AND users.sub != ?
                    )
                    AND EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.user_id = user_daily_reminders.user_id
                          AND upt.token = ?
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.user_id = user_daily_reminders.user_id
                          AND upt.token != ?
                    )
                """,
                (user_sub, expo_push_token, expo_push_token),
            ),
            # If we're going to reassign and it will be the first push token
            # for this user, register them for daily push notifications
            (
                """
                INSERT INTO user_daily_reminders (
                    uid, user_id, channel, start_time, end_time, day_of_week_mask, created_at
                )
                SELECT
                    ?, users.id, 'push', 28800, 39600, 127, ?
                FROM users
                WHERE
                    users.sub = ?
                    AND EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.user_id != users.id
                          AND upt.token = ?
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.user_id = users.id
                    )
                """,
                (
                    new_udr_uid,
                    now,
                    user_sub,
                    expo_push_token,
                ),
            ),
            # Reassign if it already exists for a different user
            (
                """
                UPDATE user_push_tokens
                SET user_id = users.id, last_seen_at = ?, updated_at = ?
                FROM users
                WHERE
                    users.sub = ?
                    AND user_push_tokens.user_id != users.id
                    AND user_push_tokens.token = ?
                """,
                (now, now, user_sub, expo_push_token),
            ),
            # Delete excessive tokens if we're about to add a new one
            (
                """
                DELETE FROM user_push_tokens
                WHERE
                    EXISTS (
                        SELECT 1 FROM users
                        WHERE users.id = user_push_tokens.user_id
                        AND users.sub = ?
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.token = ?
                    )
                    AND (
                        SELECT COUNT(*) FROM user_push_tokens AS upt
                        WHERE
                            EXISTS (
                                SELECT 1 FROM users
                                WHERE users.id = upt.user_id
                                AND users.sub = ?
                            )
                            AND (
                                upt.last_seen_at > user_push_tokens.last_seen_at
                                OR (
                                    upt.last_seen_at = user_push_tokens.last_seen_at
                                    AND upt.id > user_push_tokens.id
                                )
                            )
                    ) >= ?
                """,
                (
                    user_sub,
                    expo_push_token,
                    user_sub,
                    MAX_CONCURRENT_PUSH_TOKENS_PER_USER - 1,
                ),
            ),
            # Add a new token if it doesn't exist
            (
                """
                INSERT INTO user_push_tokens (
                    uid,
                    user_id,
                    platform,
                    token,
                    created_at,
                    updated_at,
                    last_seen_at,
                    last_confirmed_at
                )
                SELECT
                    ?, users.id, ?, ?, ?, ?, ?, NULL
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
                    now,
                    now,
                    now,
                    user_sub,
                    expo_push_token,
                ),
            ),
            # Register for daily push notifications if we created the only token for the user
            (
                """
                INSERT INTO user_daily_reminders (
                    uid, user_id, channel, start_time, end_time, day_of_week_mask, created_at
                )
                SELECT
                    ?, users.id, 'push', 28800, 39600, 127, ?
                FROM users
                WHERE
                    users.sub = ?
                    AND EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.uid = ? AND upt.user_id = users.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM user_daily_reminders AS udr
                        WHERE udr.user_id = users.id AND udr.channel = 'push'
                    )
                """,
                (new_udr_uid, now, user_sub, new_uid),
            ),
        ),
        transaction=True,
    )

    refresh_response = response[0]
    reminder_deleted_during_reassign_response = response[1]
    reminder_created_during_reassign_response = response[2]
    reassign_response = response[3]
    excess_delete_response = response[4]
    create_response = response[5]
    daily_reminder_response = response[6]

    action_taken: Optional[Literal["refresh", "reassign", "create"]] = None
    excess_deleted: int = 0

    if (
        refresh_response.rows_affected is not None
        and refresh_response.rows_affected > 0
    ):
        if refresh_response.rows_affected != 1:
            await handle_contextless_error(
                extra_info=f"refreshed more than one expo push token? {refresh_response.rows_affected=}, {expo_push_token=}, {user_sub=}"
            )
        if (
            reassign_response.rows_affected is not None
            and reassign_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"refreshed and reassigned expo push token? {expo_push_token=}, {user_sub=}"
            )
        if (
            excess_delete_response.rows_affected is not None
            and excess_delete_response.rows_affected > 0
        ):
            excess_deleted = excess_delete_response.rows_affected
            await handle_contextless_error(
                extra_info=f"refreshed and deleted excess expo push tokens? {expo_push_token=}, {user_sub=}"
            )
        if (
            create_response.rows_affected is not None
            and create_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"refreshed and created new expo push token? {expo_push_token=}, {user_sub=}"
            )
        if (
            daily_reminder_response.rows_affected is not None
            and daily_reminder_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"refreshed and created daily reminder? {expo_push_token=}, {user_sub=}"
            )
        if (
            reminder_deleted_during_reassign_response.rows_affected is not None
            and reminder_deleted_during_reassign_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"refreshed and deleted {reminder_deleted_during_reassign_response.rows_affected=} daily reminders during reassign? {expo_push_token=}, {user_sub=}"
            )
        if (
            reminder_created_during_reassign_response.rows_affected is not None
            and reminder_created_during_reassign_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"refreshed and created {reminder_created_during_reassign_response.rows_affected=} daily reminders during reassign? {expo_push_token=}, {user_sub=}"
            )

        action_taken = "refresh"
    elif (
        reassign_response.rows_affected is not None
        and reassign_response.rows_affected > 0
    ):
        if reassign_response.rows_affected != 1:
            await handle_contextless_error(
                extra_info=f"reassigned more than one expo push token? {reassign_response.rows_affected=}, {expo_push_token=}, {user_sub=}"
            )
        if (
            excess_delete_response.rows_affected is not None
            and excess_delete_response.rows_affected > 0
        ):
            excess_deleted = excess_delete_response.rows_affected
            await handle_contextless_error(
                extra_info=f"reassigned and deleted excess expo push tokens? {expo_push_token=}, {user_sub=}"
            )
        if (
            create_response.rows_affected is not None
            and create_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"reassigned and created new expo push token? {expo_push_token=}, {user_sub=}"
            )
        if (
            daily_reminder_response.rows_affected is not None
            and daily_reminder_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"reassigned and created daily reminder? {expo_push_token=}, {user_sub=}"
            )

        action_taken = "reassign"

        if (
            reminder_deleted_during_reassign_response.rows_affected is not None
            and reminder_deleted_during_reassign_response.rows_affected > 0
        ):
            if reminder_deleted_during_reassign_response.rows_affected != 1:
                await handle_contextless_error(
                    extra_info=f"deleted {reminder_deleted_during_reassign_response.rows_affected=} daily reminders during reassign? {expo_push_token=}, {user_sub=}"
                )

            await (
                DailyReminderRegistrationStatsPreparer()
                .incr_unsubscribed(
                    unix_dates.unix_timestamp_to_unix_date(
                        now, tz=pytz.timezone("America/Los_Angeles")
                    ),
                    "push",
                    "unreachable",
                )
                .store(itgs)
            )

        if (
            reminder_created_during_reassign_response.rows_affected is not None
            and reminder_created_during_reassign_response.rows_affected > 0
        ):
            if reminder_created_during_reassign_response.rows_affected != 1:
                await handle_contextless_error(
                    extra_info=f"created {reminder_created_during_reassign_response.rows_affected=} daily reminders during reassign? {expo_push_token=}, {user_sub=}"
                )

            await (
                DailyReminderRegistrationStatsPreparer()
                .incr_subscribed(
                    unix_dates.unix_timestamp_to_unix_date(
                        now, tz=pytz.timezone("America/Los_Angeles")
                    ),
                    "push",
                    "push_token_reassigned",
                )
                .store(itgs)
            )
    elif (
        create_response.rows_affected is not None and create_response.rows_affected > 0
    ):
        if create_response.rows_affected != 1:
            await handle_contextless_error(
                extra_info=f"created more than one expo push token? {create_response.rows_affected=}, {expo_push_token=}, {user_sub=}"
            )
        if (
            reminder_deleted_during_reassign_response.rows_affected is not None
            and reminder_deleted_during_reassign_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"created and deleted {reminder_deleted_during_reassign_response.rows_affected=} daily reminders during reassign? {expo_push_token=}, {user_sub=}"
            )
        if (
            reminder_created_during_reassign_response.rows_affected is not None
            and reminder_created_during_reassign_response.rows_affected > 0
        ):
            await handle_contextless_error(
                extra_info=f"created token and created {reminder_created_during_reassign_response.rows_affected=} daily reminders during reassign? {expo_push_token=}, {user_sub=}"
            )
        if (
            daily_reminder_response.rows_affected is not None
            and daily_reminder_response.rows_affected > 0
        ):
            if daily_reminder_response.rows_affected != 1:
                await handle_contextless_error(
                    extra_info=f"created more than one daily reminder? {daily_reminder_response.rows_affected=}, {expo_push_token=}, {user_sub=}"
                )
            await (
                DailyReminderRegistrationStatsPreparer()
                .incr_subscribed(
                    unix_dates.unix_timestamp_to_unix_date(
                        now, tz=pytz.timezone("America/Los_Angeles")
                    ),
                    "push",
                    "push_token_added",
                )
                .store(itgs)
            )
        excess_deleted = (
            excess_delete_response.rows_affected
            if excess_delete_response.rows_affected is not None
            else 0
        )
        action_taken = "create"

    if action_taken is None:
        await handle_contextless_error(
            extra_info=f"no action taken for expo push token? {expo_push_token=}, {user_sub=}"
        )
        return

    redis = await itgs.redis()

    async def prep(force: bool):
        await lib.push.token_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
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
