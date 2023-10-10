from typing import Union
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.daily_reminders.registration_stats import (
    DailyReminderRegistrationStatsPreparer,
)
from lib.push.message_attempt_info import (
    MessageAttemptFailureInfo,
    MessageAttemptToCheck,
    MessageAttemptToSend,
    decode_data_for_failure_job,
)
from lib.push.handler import retry_or_abandon_standard
from lib.touch.pending import on_touch_destination_abandoned_or_permanently_failed
from lib.touch.touch_info import (
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchDebugLogInsertFields,
    UserTouchDebugLogEventSendAbandon,
    UserTouchDebugLogEventSendRetry,
    UserTouchDebugLogEventSendUnretryable,
)
import lib.push.token_stats
from runners.touch.send import create_user_touch_debug_log_uid
import time
import unix_dates
import pytz


category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    attempt_uid: str,
    touch_uid: str,
    user_sub: str,
    data_raw: str,
):
    """Handles failure in a push notification sent via the touch send job.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        attempt_uid (str): the uid of the send_attempt event within the user_touch_debug_log
        touch_uid (str): the uid of the touch the email was for, for lib.touch.pending
        user_sub (str): the sub of the user we were trying to contact
        data_raw (str): The encoded failure information, provided by the push module
    """
    attempt, failure_info = decode_data_for_failure_job(data_raw)
    now = time.time()
    logging.info(
        f"Touch push notification {attempt.uid} failed during {failure_info.action}: {failure_info}"
    )
    retried = await retry_or_abandon_standard(itgs, attempt, failure_info)

    redis = await itgs.redis()
    if not failure_info.retryable:
        await redis.rpush(
            b"touch:to_log",
            TouchLogUserTouchDebugLogInsert(
                table="user_touch_debug_log",
                action="insert",
                fields=TouchLogUserTouchDebugLogInsertFields(
                    uid=create_user_touch_debug_log_uid(),
                    user_sub=user_sub,
                    event=UserTouchDebugLogEventSendUnretryable(
                        type="send_unretryable",
                        parent=attempt_uid,
                        destination=attempt.push_token,
                        info=failure_info,
                    ),
                    created_at=now,
                ),
                queued_at=now,
            )
            .json()
            .encode("utf-8"),
        )
        await _handle_if_token_is_bad(
            itgs,
            gd,
            attempt_uid=attempt_uid,
            user_sub=user_sub,
            attempt=attempt,
            failure_info=failure_info,
            now=now,
        )
        await on_touch_destination_abandoned_or_permanently_failed(
            itgs, touch_uid=touch_uid, attempt_uid=attempt.uid
        )
        return

    if retried:
        await redis.rpush(
            b"touch:to_log",
            TouchLogUserTouchDebugLogInsert(
                table="user_touch_debug_log",
                action="insert",
                fields=TouchLogUserTouchDebugLogInsertFields(
                    uid=create_user_touch_debug_log_uid(),
                    user_sub=user_sub,
                    event=UserTouchDebugLogEventSendRetry(
                        type="send_retry",
                        parent=attempt_uid,
                        destination=attempt.push_token,
                        info=failure_info,
                    ),
                    created_at=now,
                ),
                queued_at=now,
            )
            .json()
            .encode("utf-8"),
        )
        return

    await redis.rpush(
        b"touch:to_log",
        TouchLogUserTouchDebugLogInsert(
            table="user_touch_debug_log",
            action="insert",
            fields=TouchLogUserTouchDebugLogInsertFields(
                uid=create_user_touch_debug_log_uid(),
                user_sub=user_sub,
                event=UserTouchDebugLogEventSendAbandon(
                    type="send_abandon",
                    parent=attempt_uid,
                    destination=attempt.push_token,
                    info=failure_info,
                ),
                created_at=now,
            ),
            queued_at=now,
        )
        .json()
        .encode("utf-8"),
    )
    await on_touch_destination_abandoned_or_permanently_failed(
        itgs, touch_uid=touch_uid, attempt_uid=attempt.uid
    )


async def _handle_if_token_is_bad(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    attempt_uid: str,
    user_sub: str,
    attempt: Union[MessageAttemptToSend, MessageAttemptToCheck],
    failure_info: MessageAttemptFailureInfo,
    now: float,
) -> None:
    if failure_info.identifier != "DeviceNotRegistered":
        return

    # This path is unlikely enough and important enough that it
    # seems prudent to directly communicate with the database
    logging.warning(
        f"Detected push token is invalid for {user_sub=}; {attempt.push_token=}"
    )

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.executemany3(
        (
            (
                """
            DELETE FROM user_daily_reminders
            WHERE
                EXISTS (
                    SELECT 1 FROM user_push_tokens AS upt
                    WHERE upt.token = ?
                      AND upt.user_id = user_daily_reminders.user_id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM user_push_tokens AS upt
                    WHERE upt.token != ?
                      AND upt.user_id = user_daily_reminders.user_id
                )
            """,
                (attempt.push_token, attempt.push_token),
            ),
            (
                "DELETE FROM user_push_tokens WHERE token=?",
                (attempt.push_token,),
            ),
        )
    )

    if response[0].rows_affected is not None and response[0].rows_affected > 0:
        logging.debug(
            f"Deleted {response[0].rows_affected} daily reminders for users with this token"
        )
        await (
            DailyReminderRegistrationStatsPreparer()
            .incr_unsubscribed(
                unix_dates.unix_timestamp_to_unix_date(
                    now, tz=pytz.timezone("America/Los_Angeles")
                ),
                "push",
                "unreachable",
                amt=response[0].rows_affected,
            )
            .store(itgs)
        )

    if response[1].rows_affected is None or response[1].rows_affected < 1:
        logging.debug("The push token had already been deleted")
        return

    if failure_info.action == "send":
        await lib.push.token_stats.increment_event(
            itgs, event="deleted_due_to_unrecognized_ticket", now=now
        )
    else:
        await lib.push.token_stats.increment_event(
            itgs, event="deleted_due_to_unrecognized_receipt", now=now
        )
