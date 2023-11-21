"""SMS failure handler for the touch system"""
import time
from typing import Union
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.shared.clean_for_slack import clean_for_non_code_slack, clean_for_slack
from lib.shared.describe_user import enqueue_send_described_user_slack_message
from lib.sms.sms_info import (
    PendingSMS,
    SMSFailureInfo,
    SMSToSend,
    decode_data_for_failure_job,
)
from lib.sms.handler import (
    retry_or_abandon_standard,
    maybe_suppress_phone_due_to_failure,
)
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
            b"touch:to_log",  # type: ignore
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
            .model_dump_json()
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
            b"touch:to_log",  # type: ignore
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
            .model_dump_json()
            .encode("utf-8"),
        )
        return

    await redis.rpush(
        b"touch:to_log",  # type: ignore
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
        .model_dump_json()
        .encode("utf-8"),
    )
    await on_touch_destination_abandoned_or_permanently_failed(
        itgs, touch_uid=touch_uid, attempt_uid=attempt.uid
    )


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
    warranted_suppression = await maybe_suppress_phone_due_to_failure(
        itgs, attempt, failure_info
    )
    if not warranted_suppression:
        return

    await enqueue_send_described_user_slack_message(
        itgs,
        message=(
            f"{{name}}'s phone number {clean_for_non_code_slack(repr(attempt.phone_number))} is unreachable; suppressed"
            f"\n\n```\nattempt={clean_for_slack(repr(attempt))}\n```"
            f"\n\n```\nfailure_info={clean_for_slack(repr(failure_info))}\n```"
        ),
        sub=user_sub,
        channel="oseh_bot",
    )
