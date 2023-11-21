"""Touch system push notification success handler"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.push.message_attempt_info import decode_data_for_success_job
import time
from lib.touch.pending import on_touch_destination_success

from lib.touch.touch_info import (
    TouchLogUserPushTokenUpdate,
    TouchLogUserPushTokenUpdateFields,
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchDebugLogInsertFields,
    TouchLogUserTouchInsert,
    TouchLogUserTouchPushInsertFields,
    TouchLogUserTouchPushInsertMessage,
    UserTouchDebugLogEventSendSuccess,
)
from runners.touch.send import create_user_touch_debug_log_uid

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    attempt_uid: str,
    touch_uid: str,
    touch_point_uid: str,
    user_sub: str,
    data_raw: str,
):
    """The touch system push notification success handler, which stores the
    success in user_touches and user_touch_debug_log.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        attempt_uid (str): the uid of the send_attempt event within the user_touch_debug_log
        touch_uid (str): the uid of the touch the email was for, for lib.touch.pending
        touch_point_uid (str): the uid of the touch point used to send the message
        user_sub (str): the sub of the user we were trying to contact
        data_raw (str): The encoded message attempt and success information, provided
            by the push module
    """
    attempt, success_info = decode_data_for_success_job(data_raw)
    now = time.time()
    logging.info(
        f"Touch push notification {attempt.uid} confirmed successful "
        f"after {now - attempt.initially_queued_at:.2f}s:\n"
        f"  {attempt=}\n"
        f"  {success_info=}"
    )

    redis = await itgs.redis()
    await redis.rpush(
        b"touch:to_log",  # type: ignore
        TouchLogUserTouchDebugLogInsert(
            table="user_touch_debug_log",
            action="insert",
            fields=TouchLogUserTouchDebugLogInsertFields(
                uid=create_user_touch_debug_log_uid(),
                user_sub=user_sub,
                event=UserTouchDebugLogEventSendSuccess(
                    type="send_success",
                    parent=attempt_uid,
                    destination=attempt.push_token,
                ),
                created_at=success_info.receipt_checked_at,
            ),
            queued_at=now,
        )
        .model_dump_json()
        .encode("utf-8"),
        TouchLogUserTouchInsert(
            table="user_touches",
            action="insert",
            fields=TouchLogUserTouchPushInsertFields(
                uid=touch_uid,
                user_sub=user_sub,
                channel="push",
                touch_point_uid=touch_point_uid,
                destination=attempt.push_token,
                message=TouchLogUserTouchPushInsertMessage(
                    title=attempt.contents.title,
                    body=attempt.contents.body,
                    channel_id=attempt.contents.channel_id,
                ),
                created_at=success_info.receipt_checked_at,
            ),
            queued_at=now,
        )
        .model_dump_json()
        .encode("utf-8"),
        TouchLogUserPushTokenUpdate(
            table="user_push_tokens",
            action="update",
            fields=TouchLogUserPushTokenUpdateFields(
                token=attempt.push_token,
                last_confirmed_at=success_info.receipt_checked_at,
            ),
            queued_at=now,
        )
        .model_dump_json()
        .encode("utf-8"),
    )
    await on_touch_destination_success(itgs, touch_uid=touch_uid)
