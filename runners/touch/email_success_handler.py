"""Touch system email notification success handler"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.emails.email_info import decode_data_for_success_job
from lib.touch.pending import on_touch_destination_success
from lib.touch.touch_info import (
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchDebugLogInsertFields,
    TouchLogUserTouchEmailInsertFields,
    TouchLogUserTouchEmailMessage,
    TouchLogUserTouchInsert,
    UserTouchDebugLogEventSendSuccess,
)
from runners.touch.send import create_user_touch_debug_log_uid
import time

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
    """The touch system email notification success handler, which stores the
    success in user_touches and user_touch_debug_log.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        attempt_uid (str): the uid of the send_attempt event within the user_touch_debug_log
        touch_uid (str): the uid of the touch the email was for, for lib.touch.pending
        touch_point_uid (str): the uid of the touch point used to send the message
        user_sub (str): the sub of the user we were trying to contact
        data_raw (str): The encoded message attempt and success information, provided
            by the email module
    """
    now = time.time()
    email, info = decode_data_for_success_job(data_raw)
    logging.info(
        f"Email {email.message_id} ({email.subject}) was successfully delivered to {email.email} "
        f"using {email.template}:\n{email=}, {info=}"
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
                    destination=email.email,
                ),
                created_at=info.delivery_received_at,
            ),
            queued_at=now,
        )
        .model_dump_json()
        .encode("utf-8"),
        TouchLogUserTouchInsert(
            table="user_touches",
            action="insert",
            fields=TouchLogUserTouchEmailInsertFields(
                uid=touch_uid,
                user_sub=user_sub,
                channel="email",
                touch_point_uid=touch_point_uid,
                destination=email.email,
                message=TouchLogUserTouchEmailMessage(
                    subject=email.subject,
                    template=email.template,
                    template_parameters=email.template_parameters,
                ),
                created_at=info.delivery_received_at,
            ),
            queued_at=now,
        )
        .model_dump_json()
        .encode("utf-8"),
    )
    await on_touch_destination_success(itgs, touch_uid=touch_uid)
