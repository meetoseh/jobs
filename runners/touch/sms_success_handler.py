"""The SMS success handler for the touch system"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.sms.sms_info import decode_data_for_success_job
import time
from lib.touch.pending import on_touch_destination_success

from lib.touch.touch_info import (
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchDebugLogInsertFields,
    TouchLogUserTouchInsert,
    TouchLogUserTouchSMSInsertFields,
    TouchLogUserTouchSMSMessage,
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
    """The touch system sms notification success handler, which stores the
    success in user_touches and user_touch_debug_log.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        attempt_uid (str): the uid of the send_attempt event within the user_touch_debug_log
        touch_uid (str): the uid of the touch the email was for, for lib.touch.pending
        touch_point_uid (str): the uid of the touch point used to send the message
        user_sub (str): the sub of the user we were trying to contact
        data_raw (str): The encoded sms attempt and success information, provided
            by the sms module
    """
    sms, success_info = decode_data_for_success_job(data_raw)
    now = time.time()
    logging.info(
        f"SMS {sms.uid} confirmed successful "
        f"after {now - sms.initially_queued_at:.2f}s:\n"
        f"  {sms=}\n"
        f"  {success_info=}"
    )

    redis = await itgs.redis()
    await redis.rpush(
        b"touch:to_log",
        TouchLogUserTouchDebugLogInsert(
            table="user_touch_debug_log",
            action="insert",
            fields=TouchLogUserTouchDebugLogInsertFields(
                uid=create_user_touch_debug_log_uid(),
                user_sub=user_sub,
                event=UserTouchDebugLogEventSendSuccess(
                    type="send_success",
                    parent=attempt_uid,
                    destination=sms.phone_number,
                ),
                created_at=success_info.message_resource_succeeded_at,
            ),
            queued_at=now,
        )
        .json()
        .encode("utf-8"),
        TouchLogUserTouchInsert(
            table="user_touches",
            action="insert",
            fields=TouchLogUserTouchSMSInsertFields(
                uid=touch_uid,
                user_sub=user_sub,
                channel="sms",
                touch_point_uid=touch_point_uid,
                destination=sms.phone_number,
                message=TouchLogUserTouchSMSMessage(
                    body=sms.body,
                ),
                created_at=success_info.message_resource_succeeded_at,
            ),
            queued_at=now,
        )
        .json()
        .encode("utf-8"),
    )
    await on_touch_destination_success(itgs, touch_uid=touch_uid)
