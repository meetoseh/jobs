from typing import Union
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.push.message_attempt_info import (
    MessageAttemptFailureInfo,
    MessageAttemptToCheck,
    MessageAttemptToSend,
    decode_data_for_failure_job,
)
from lib.push.handler import (
    maybe_delete_push_token_due_to_failure,
    retry_or_abandon_standard,
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
import time


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
                        destination=attempt.push_token,
                        info=failure_info,
                    ),
                    created_at=now,
                ),
                queued_at=now,
            )
            .model_dump_json()
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
                        destination=attempt.push_token,
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
                    destination=attempt.push_token,
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
    warranted_delete = await maybe_delete_push_token_due_to_failure(
        itgs, attempt=attempt, failure_info=failure_info, file=__name__
    )

    if not warranted_delete:
        return

    logging.info(
        f"Deleting push token {attempt.push_token} for user {user_sub} due to failure"
    )
