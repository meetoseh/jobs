"""Sends notifications to users about a new daily event, if they have opted to receive them."""
import json
from typing import Optional
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
from redis.exceptions import NoScriptError
from error_middleware import handle_error, handle_warning
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from twilio.base.exceptions import TwilioException
import logging
import time
import secrets
import socket
import hashlib
import string
import asyncio
import os

category = JobCategory.HIGH_RESOURCE_COST
# this is easily weavable with other jobs and doesn't take much memory/cpu, however,
# since for now jobs are always run one-at-a-time per instance, we don't want this to
# take up the low resource cost instances


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Checks if we've notified users about the current daily event, and if we have
    not, sends out notifications to users who have opted to receive them.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    now = time.time()
    response = await cursor.execute(
        """
        SELECT uid FROM daily_events
        WHERE
            available_at <= ?
            AND NOT EXISTS (
                SELECT 1 FROM daily_events AS de
                WHERE
                  de.available_at <= ?
                  AND de.available_at > daily_events.available_at
            )
        """,
        (now, now),
    )
    if not response.results:
        return

    current_daily_event_uid: str = response.results[0][0]

    redis = await itgs.redis()
    last_notified_daily_event_uid = await redis.get(b"daily_event:last_notified:uid")
    if isinstance(last_notified_daily_event_uid, bytes):
        last_notified_daily_event_uid = last_notified_daily_event_uid.decode("utf-8")

    if last_notified_daily_event_uid == current_daily_event_uid:
        return

    lock_key = b"daily_event:notifications:lock"
    acquired_lock_until = int(time.time() + 60)
    lock_uid = f"{socket.gethostname()}-{secrets.token_urlsafe(16)}".encode("utf-8")
    got_lock = await redis.set(lock_key, lock_uid, exat=acquired_lock_until, nx=True)
    if not got_lock:
        return

    async def refresh_lock(min_remaining: float = 30) -> bool:
        """Refreshes the lock if it has less than min_remaining seconds remaining."""
        nonlocal acquired_lock_until
        remaining = acquired_lock_until - time.time()
        if remaining < min_remaining:
            acquired_lock_until = int(time.time() + max(60, min_remaining * 2))

        if not await expire_if_match(itgs, lock_key, lock_uid, acquired_lock_until):
            await handle_warning(
                f"{__name__}:lock_lost",
                "Lost lock while sending daily event notifications",
            )
            return False
        return True

    logging.info(f"Sending daily event notifications for {current_daily_event_uid=}")
    twilio = await itgs.twilio()
    twilio_from = os.environ["OSEH_TWILIO_MESSAGE_SERVICE_SID"]
    last_uns_uid: Optional[str] = None
    our_src = "jobs.runners.notifications.send_daily_event_notifications"
    reason = json.dumps({"src": our_src, "daily_event_uid": current_daily_event_uid})
    base_url = os.environ["ROOT_FRONTEND_URL"] + "/n/"
    callback_url: Optional[str] = (
        os.environ["ROOT_BACKEND_URL"] + "/api/1/notifications/complete"
        if os.environ["ENVIRONMENT"] != "dev"
        else None
    )
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1) as executor:
        while True:
            if gd.received_term_signal:
                logging.info(
                    "send_daily_event_notifications: received term signal before completion"
                )
                await redis.delete(lock_key)
                return
            if not await refresh_lock():
                return

            # we have a specific partial index on user_notifications for this query
            response = await cursor.execute(
                """
                SELECT
                    user_notification_settings.uid,
                    users.sub,
                    users.phone_number
                FROM user_notification_settings
                JOIN users ON users.id = user_notification_settings.user_id
                WHERE
                    user_notification_settings.daily_event_enabled = 1
                    AND user_notification_settings.channel = 'sms'
                    AND users.phone_number IS NOT NULL
                    AND (? IS NULL OR user_notification_settings.uid > ?)
                    AND NOT EXISTS (
                        SELECT 1 FROM user_notifications
                        WHERE
                            user_notifications.user_id = users.id
                            AND user_notifications.channel = 'sms'
                            AND json_extract(user_notifications.reason, '$.src') = ?
                            AND json_extract(user_notifications.reason, '$.daily_event_uid') = ?
                    )
                ORDER BY user_notification_settings.uid
                LIMIT 10
                """,
                (last_uns_uid, last_uns_uid, our_src, current_daily_event_uid),
            )
            if not response.results:
                break

            user_notifications_to_send = response.results
            for _, sub, phone_number in user_notifications_to_send:
                if not await refresh_lock():
                    return
                if gd.received_term_signal:
                    logging.info(
                        "send_daily_event_notifications: received term signal before completion"
                    )
                    await redis.delete(lock_key)
                    return
                un_uid = f"oseh_un_{secrets.token_urlsafe(16)}"
                tracking_code = generate_code()
                body = f"Take a mindful moment with the journeys that just came out. {base_url}{tracking_code}"
                channel_extra_unencoded = {
                    "pn": phone_number,
                    "provider": "twilio",
                    "from": twilio_from,
                    "__is_optimistic": True,
                }
                now = time.time()
                response = await cursor.execute(
                    """
                    INSERT INTO user_notifications (
                        uid, user_id, tracking_code, channel, channel_extra, status,
                        contents, contents_s3_file_id, reason, created_at
                    )
                    SELECT
                        ?, users.id, ?, ?, ?, ?, ?, NULL, ?, ?
                    FROM users WHERE users.sub=?
                    """,
                    (
                        un_uid,
                        tracking_code,
                        "sms",
                        json.dumps(channel_extra_unencoded, sort_keys=True),
                        "pending",
                        json.dumps({"body": body}, sort_keys=True),
                        reason,
                        now,
                        sub,
                    ),
                    raise_on_error=False,
                )

                if response.error:
                    logging.error(
                        f"Failed to insert user notification: {response.error}"
                    )
                    continue

                if response.rows_affected is None or response.rows_affected < 1:
                    logging.error(
                        "Failed to insert user notification: no rows affected"
                    )
                    continue

                try:
                    response = await loop.run_in_executor(
                        executor,
                        partial(
                            twilio.messages.create,
                            body=body,
                            to=phone_number,
                            from_=twilio_from,
                            status_callback=callback_url,
                        ),
                    )
                except TwilioException as e:
                    await handle_error(
                        e,
                        extra_info=f"Failed to send SMS to {sub=}, {phone_number=}",
                    )
                    await asyncio.sleep(5)
                    continue

                del channel_extra_unencoded["__is_optimistic"]
                channel_extra_unencoded["message_sid"] = response.sid
                channel_extra_unencoded["requested_callback"] = callback_url is not None
                actual_status = response.status

                logging.info(
                    f"Sent SMS to {sub=}, {phone_number=}: {body=}; {response.sid=}, {response.status=}"
                )

                channel_extra = json.dumps(channel_extra_unencoded, sort_keys=True)
                response = await cursor.execute(
                    "UPDATE user_notifications SET channel_extra = ?, status = ? WHERE uid=? AND status='pending'",
                    (channel_extra, actual_status, un_uid),
                )
                if response.rows_affected is None or response.rows_affected < 1:
                    # raced the status, can still update channel_extra
                    await cursor.execute(
                        "UPDATE user_notifications SET channel_extra = ? WHERE uid=?",
                        (channel_extra, un_uid),
                    )

                await asyncio.sleep(1)

            last_uns_uid = user_notifications_to_send[-1][0]

    await redis.set(
        b"daily_event:last_notified:uid", current_daily_event_uid.encode("utf-8")
    )
    await redis.delete(lock_key)

    logging.info("send_daily_event_notifications: completed successfully")


EXPIRE_AT_IF_MATCH_SCRIPT = """
local key = KEYS[1]
local expected_value = ARGV[1]
local expire_at = tonumber(ARGV[2])

local value = redis.call("GET", key)
if value == expected_value then
    redis.call("EXPIREAT", key, expire_at)
    return 1
else
    return 0
end
"""

EXPIRE_AT_IF_MATCH_SCRIPT_SHA1 = hashlib.sha1(
    EXPIRE_AT_IF_MATCH_SCRIPT.encode("utf-8")
).hexdigest()


async def expire_if_match(
    itgs: Itgs,
    key: bytes,
    value: bytes,
    expire_at: int,
) -> bool:
    """Attempts to expire a key if it has the given value."""
    redis = await itgs.redis()

    try:
        result = await redis.evalsha(
            EXPIRE_AT_IF_MATCH_SCRIPT_SHA1,
            1,
            key,
            value,
            expire_at,
        )
    except NoScriptError:
        real_sha = await redis.script_load(EXPIRE_AT_IF_MATCH_SCRIPT)
        assert (
            real_sha == EXPIRE_AT_IF_MATCH_SCRIPT_SHA1
        ), f"{real_sha=} != {EXPIRE_AT_IF_MATCH_SCRIPT_SHA1=}"
        result = await redis.evalsha(
            EXPIRE_AT_IF_MATCH_SCRIPT_SHA1,
            1,
            key,
            value,
            expire_at,
        )

    return bool(result)


# RFC 3986 2.3. Unreserved Characters
# Updated by:
# - RFC 6874: Proposed Standard - no apparent relevant change
# - RFC 7320 aka BCP 190: Best Current Practice - no apparent relevant change
# - RFC 8820: Best Current Practice - no apparent relevant change
CODE_ALPHABET = string.ascii_letters + string.digits + "-._~"


def generate_code() -> str:
    """Generates a random url-safe code. This has
    66^5 ~= 1.25 billion possible values and thus collisions are
    highly unlikely.

    Note that the returned result may not be a valid base64
    string.
    """
    return "".join([secrets.choice(CODE_ALPHABET) for _ in range(5)])
