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
                        WHERE upt.token = ? AND upt.user_id = users.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM user_push_tokens AS upt
                        WHERE upt.token != ? AND upt.user_id = users.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM user_daily_reminders AS udr
                        WHERE udr.user_id = users.id AND udr.channel = 'push'
                    )
                """,
                (new_udr_uid, now, user_sub, expo_push_token, expo_push_token),
            ),
        ),
        transaction=True,
    )

    action_taken: Optional[Literal["refresh", "reassign", "create"]] = None
    excess_deleted: int = 0

    if response[0].rows_affected is not None and response[0].rows_affected > 0:
        if response[0].rows_affected != 1:
            await handle_contextless_error(
                extra_info=f"refreshed more than one expo push token? {response[0].rows_affected=}, {expo_push_token=}, {user_sub=}"
            )
        if response[1].rows_affected is not None and response[1].rows_affected > 0:
            await handle_contextless_error(
                extra_info=f"refreshed and reassigned expo push token? {expo_push_token=}, {user_sub=}"
            )
        if response[2].rows_affected is not None and response[2].rows_affected > 0:
            excess_deleted = response[2].rows_affected
            await handle_contextless_error(
                extra_info=f"refreshed and deleted excess expo push tokens? {expo_push_token=}, {user_sub=}"
            )
        if response[3].rows_affected is not None and response[3].rows_affected > 0:
            await handle_contextless_error(
                extra_info=f"refreshed and created new expo push token? {expo_push_token=}, {user_sub=}"
            )
        if response[4].rows_affected is not None and response[4].rows_affected > 0:
            await handle_contextless_error(
                extra_info=f"refreshed and created daily reminder? {expo_push_token=}, {user_sub=}"
            )

        action_taken = "refresh"
    elif response[1].rows_affected is not None and response[1].rows_affected > 0:
        if response[1].rows_affected != 1:
            await handle_contextless_error(
                extra_info=f"reassigned more than one expo push token? {response[1].rows_affected=}, {expo_push_token=}, {user_sub=}"
            )
        if response[2].rows_affected is not None and response[2].rows_affected > 0:
            excess_deleted = response[2].rows_affected
            await handle_contextless_error(
                extra_info=f"reassigned and deleted excess expo push tokens? {expo_push_token=}, {user_sub=}"
            )
        if response[3].rows_affected is not None and response[3].rows_affected > 0:
            await handle_contextless_error(
                extra_info=f"reassigned and created new expo push token? {expo_push_token=}, {user_sub=}"
            )
        if response[4].rows_affected is not None and response[4].rows_affected > 0:
            await handle_contextless_error(
                extra_info=f"reassigned and created daily reminder? {expo_push_token=}, {user_sub=}"
            )

        action_taken = "reassign"
    elif response[3].rows_affected is not None and response[3].rows_affected > 0:
        if response[3].rows_affected != 1:
            await handle_contextless_error(
                extra_info=f"created more than one expo push token? {response[3].rows_affected=}, {expo_push_token=}, {user_sub=}"
            )
        if response[4].rows_affected is not None and response[4].rows_affected > 0:
            if response[4].rows_affected != 1:
                await handle_contextless_error(
                    extra_info=f"created more than one daily reminder? {response[4].rows_affected=}, {expo_push_token=}, {user_sub=}"
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
            response[2].rows_affected if response[2].rows_affected is not None else 0
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
