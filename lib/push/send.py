"""A low-level api for sending push notifications to devices. Caution is required
when using this library directly:

1. Users can have multiple devices
2. This doesn't itself handle tracking that we sent the notification

This provides a good framework for reliable push notifications, since it handles
communicating with the expo api (batching requests, checking receipts), etc, and
provides an easy mechanism to implement retries and tracking. Furthermore, an
explanation plus statistics for the entire flow are available in
/admin/notifs_dashboard
"""

from typing import Optional
from itgs import Itgs
from lib.shared.job_callback import JobCallback
from lib.push.message_attempt_info import MessageAttemptToSend, MessageContents
import secrets
import time
import lib.push.ticket_stats
import redis_helpers.run_with_prep


def create_message_attempt_uid() -> str:
    """Creates a new random message attempt uid appropriate for send_push"""
    return f"oseh_pma_{secrets.token_urlsafe(16)}"


async def send_push(
    itgs: Itgs,
    *,
    push_token: str,
    contents: MessageContents,
    success_job: JobCallback,
    failure_job: JobCallback,
    uid: Optional[str] = None,
    now: Optional[float] = None,
) -> str:
    """Sends a push notification to the device identified by the given expo
    push token. This should only be used for the first attempt, not retries.

    Args:
        itgs (Itgs): The integrations to (re)use
        push_token (str): The expo push token to send to
        contents (MessageContents): The contents of the message to send
        success_job (JobCallback): The job to run if the message is successfully
            sent. This will be passed `data_raw` which should be decoded using
            `lib.push.message_attempt_info.decode_data_for_success_job`, in
            additional to any specified kwargs.
        failure_job (JobCallback): The job to run if the message fails to send.
            This will be passed `data_raw` which should be decoded using
            `lib.push.message_attempt_info.decode_data_for_failure_job`, in
            additional to any specified kwargs.
        uid (str, None): If specified, should be a uid created as if by
            `create_message_attempt_uid`. If not specified, a new uid will
            be created.
        now (float, None): If specified, takes the place of `time.time()`;
            generally used to get more consistent timestamps if this is
            happening at "the same time" as something else.

    Returns:
        str: The uid of the created message attempt. It can be helpful to
            log this for debugging purposes, though typically if it needs
            to be stored then you'll want to handle storing it before queueing
            the job to avoid racing, which means you passed it in.
    """
    if uid is None:
        uid = create_message_attempt_uid()
    if now is None:
        now = time.time()

    entry = (
        MessageAttemptToSend(
            aud="send",
            uid=uid,
            initially_queued_at=now,
            retry=0,
            last_queued_at=now,
            push_token=push_token,
            contents=contents,
            failure_job=failure_job,
            success_job=success_job,
        )
        .model_dump_json()
        .encode("utf-8")
    )

    redis = await itgs.redis()

    async def prep(force: bool):
        await lib.push.ticket_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await lib.push.ticket_stats.attempt_increment_event(
                pipe, event="queued", now=now
            )
            await pipe.rpush(b"push:message_attempts:to_send", entry)  # type: ignore
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)
    return uid


async def retry_send_push(
    itgs: Itgs, *, attempt: MessageAttemptToSend, now: Optional[float] = None
) -> None:
    """Retries the given message attempt. Failure jobs which receive a
    MessageAttemptToSend on a retryable error should either call this to
    retry or `abandon_send_push` to abandon the message.

    Args:
        itgs (Itgs): The integrations to (re)use
        attempt (MessageAttemptToSend): The message attempt to retry
        now (float, None): If specified, takes the place of `time.time()`;
            generally used to get more consistent timestamps if this is
            happening at "the same time" as something else.
    """
    if now is None:
        now = time.time()

    entry = (
        MessageAttemptToSend(
            aud="send",
            uid=attempt.uid,
            initially_queued_at=attempt.initially_queued_at,
            retry=attempt.retry + 1,
            last_queued_at=now,
            push_token=attempt.push_token,
            contents=attempt.contents,
            failure_job=attempt.failure_job,
            success_job=attempt.success_job,
        )
        .model_dump_json()
        .encode("utf-8")
    )

    redis = await itgs.redis()

    async def prep(force: bool):
        await lib.push.ticket_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await lib.push.ticket_stats.attempt_increment_event(
                pipe, event="retried", now=attempt.initially_queued_at
            )
            await pipe.rpush(b"push:message_attempts:to_send", entry)  # type: ignore
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def abandon_send_push(itgs: Itgs, *, attempt: MessageAttemptToSend):
    """Abandons the given retryable message attempt. Failure jobs which receive
    a MessageAttemptToSend on a retryable error should call this to abandon
    the message. This MUST NOT be called for a non-retryable error.

    Args:
        itgs (Itgs): The integrations to (re)use
        attempt (MessageAttemptToSend): The message attempt to abandon
    """
    await lib.push.ticket_stats.increment_event(
        itgs, event="abandoned", now=attempt.initially_queued_at
    )
