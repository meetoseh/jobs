"""SMS failure handler for the touch system"""
import time
from typing import Union
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.sms.sms_info import (
    PendingSMS,
    SMSFailureInfo,
    SMSToSend,
    decode_data_for_failure_job,
)
from lib.sms.handler import retry_or_abandon_standard
from lib.touch.pending import on_touch_destination_abandoned_or_permanently_failed
from lib.touch.touch_info import (
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchDebugLogInsertFields,
    UserTouchDebugLogEventSendAbandon,
    UserTouchDebugLogEventSendRetry,
    UserTouchDebugLogEventSendUnretryable,
)
from runners.touch.send import create_user_touch_debug_log_uid

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
    """Handles failure from an sms sent via the touch send job.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        attempt_uid (str): the uid of the send_attempt event within the user_touch_debug_log
        touch_uid (str): the uid of the touch the email was for, for lib.touch.pending
        user_sub (str): the sub of the user we were trying to contact
        data_raw (str): The encoded failure information
    """
    now = time.time()
    attempt, failure_info = decode_data_for_failure_job(data_raw)
    result = await retry_or_abandon_standard(itgs, attempt, failure_info)
    if result.should_ignore:
        return

    logging.info(
        f"Touch sms {attempt.uid} failed during {failure_info.action}: {failure_info}"
    )
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
                        destination=attempt.phone_number,
                        info=failure_info,
                    ),
                    created_at=now,
                ),
                queued_at=now,
            )
            .json()
            .encode("utf-8"),
        )
        await _handle_if_phone_is_bad(
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

    if result.wanted_to_retry:
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
                        destination=attempt.phone_number,
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
                    destination=attempt.phone_number,
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


INVALID_DESTINATION_ERRORS = frozenset(
    (
        "21408",
        "21606",
        "21610",
        "21614",
        "30004",
        "30005",
        "30006",
        "30007",
        "30010",
        "30011",
        "63032",
        "63033",
    )
)
"""Errors that imply something is wrong with the recipient rather than
us or twilio
"""


async def _handle_if_phone_is_bad(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    attempt_uid: str,
    user_sub: str,
    attempt: Union[SMSToSend, PendingSMS],
    failure_info: SMSFailureInfo,
    now: float,
) -> None:
    if failure_info.identifier != "ApplicationErrorOther":
        return

    if failure_info.subidentifier not in INVALID_DESTINATION_ERRORS:
        return

    conn = await itgs.conn()
    cursor = conn.cursor()

    response = await cursor.execute(
        "UPDATE users SET phone_number_verified = 0 WHERE phone_number = ? AND phone_number_verified != 0",
        (attempt.phone_number,),
    )

    if response.rows_affected is None or response.rows_affected < 1:
        logging.debug(
            "User has already been deleted or their phone number has already changed/unverified"
        )
        return

    logging.warning(
        f"Detected {attempt.phone_number=} is invalid for {user_sub=}; "
        "marked unverified"
    )

    slack = await itgs.slack()
    await slack.send_oseh_bot_message(
        f"Detected {attempt.phone_number=} is invalid for {user_sub=}; "
        f"marked unverified.\n- {failure_info.subidentifier=}\n- {response.rows_affected=}",
        preview="Phone number found invalid",
    )
