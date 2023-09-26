import time
from typing import Union
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.email.email_info import (
    EmailAttempt,
    EmailFailureInfo,
    EmailPending,
    decode_data_for_failure_job,
)
from lib.email.handler import retry_or_abandon_standard
from lib.touch.pending import on_touch_destination_abandoned_or_permanently_failed
from lib.touch.touch_info import (
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchDebugLogInsertFields,
    UserTouchDebugLogEventSendAbandon,
    UserTouchDebugLogEventSendRetry,
    UserTouchDebugLogEventSendUnretryable,
)
from runners.emails.abandoned_email_callback import suppress_email
import socket

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
    """Handles failure in an email notification sent via the touch send job.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        attempt_uid (str): the uid of the send_attempt event within the user_touch_debug_log
        touch_uid (str): the uid of the touch the email was for, for lib.touch.pending
        user_sub (str): the sub of the user we were trying to contact
        data_raw (str): The encoded failure information, provided by the push module
    """
    now = time.time()
    email, info = decode_data_for_failure_job(data_raw)

    result = await retry_or_abandon_standard(itgs, email, info, now=now)
    if result.should_ignore:
        return

    logging.info(
        f"Failed to send to {email.email} ({email.subject}); {info.error_identifier}"
        f"; retryable? {info.retryable}, retried? {result.wanted_to_retry}\n{email=}, {info=}"
    )

    redis = await itgs.redis()
    if not info.retryable:
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
                        destination=email.email,
                        info=info,
                    ),
                    created_at=now,
                ),
                queued_at=now,
            )
            .json()
            .encode("utf-8"),
        )
        await _handle_if_email_is_bad(
            itgs,
            gd,
            attempt_uid=attempt_uid,
            user_sub=user_sub,
            attempt=email,
            failure_info=info,
            now=now,
        )
        await on_touch_destination_abandoned_or_permanently_failed(
            itgs, touch_uid=touch_uid, attempt_uid=email.uid
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
                        destination=email.email,
                        info=info,
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
                    destination=email.email,
                    info=info,
                ),
                created_at=now,
            ),
            queued_at=now,
        )
        .json()
        .encode("utf-8"),
    )
    await on_touch_destination_abandoned_or_permanently_failed(
        itgs, touch_uid=touch_uid, attempt_uid=email.uid
    )


async def _handle_if_email_is_bad(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    attempt_uid: str,
    user_sub: str,
    attempt: Union[EmailAttempt, EmailPending],
    failure_info: EmailFailureInfo,
    now: float,
) -> None:
    if failure_info.error_identifier not in ("Bounce", "Complaint"):
        return

    logging.info(
        f"Since this is a {failure_info.error_identifier}, suppressing the corresponding email address {attempt.email}"
    )
    await suppress_email(
        itgs,
        type_=failure_info.error_identifier,
        email=attempt.email,
        failure_extra=failure_info.extra,
        now=now,
    )
    slack = await itgs.slack()
    await slack.send_oseh_bot_message(
        f"{socket.gethostname()} Suppressed {attempt.email} due to {failure_info.error_identifier} ({failure_info.extra})",
        preview=f"Suppressed {attempt.email}",
    )
