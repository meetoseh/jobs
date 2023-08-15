import secrets
from typing import Optional
from lib.shared.job_callback import JobCallback
from itgs import Itgs
import time
from lib.sms.sms_info import SMSToSend
import lib.sms.send_stats
import redis_helpers.run_with_prep


def create_sms_uid() -> str:
    """Generates a new uid appropriate for an SMS being added to to_send. It is often
    convenient to fix the uid prior to inserting into the to send queue, to ensure the
    callback jobs are ready before they are called
    """
    return f"oseh_sms_{secrets.token_urlsafe(16)}"


async def send_sms(
    itgs: Itgs,
    *,
    phone_number: str,
    body: str,
    success_job: JobCallback,
    failure_job: JobCallback,
    uid: Optional[str] = None,
    now: Optional[float] = None,
) -> str:
    """Queues sending an SMS. This creates a Message Resource on Twilio, and
    then once it reaches a possibly-terminal (e.g., `sent`) or terminal (e.g.,
    `delivered`) state it calls the success_callback with one additional
    keyword argument: `data_raw` which is decoded via `decode_data_for_success_job`.

    If any step fails, such as it taking a long time to receive status updates
    from Twilio on the message, then the failure_callback is called with
    one keyword argument: `data_raw` which is decoded via `decode_data_for_failure_job`.

    If the failure is retryable, the failure callback is responsible for deciding
    whether to retry (via `retry_send` or `retry_pending` as appropriate based on
    the step the sms failed at) or abandon (via `abandon_send` and `abandon_pending`,
    respectively). The `_pending` functions are in `lib.sms.check`.

    Args:
        itgs (Itgs): the integrations to (re)use
        phone_number (str): the phone number to send the SMS to, in E.164 format
        body (str): the body of the SMS
        success_job (JobCallback): the callback to call on success
        failure_job (JobCallback): the callback to call on failure. Be careful
          interpreting the number of failures, as the callback is pre-increment for
          `send` and post-increment for `check`.
        uid (Optional[str], optional): the uid to use for the SMS. Defaults to a new
          random uid
        now (Optional[float], optional): the current time. Defaults to to time.time()

    Returns:
        str: The UID of the SMS queued to be sent
    """
    if now is None:
        now = time.time()

    if uid is None:
        uid = create_sms_uid()

    entry = (
        SMSToSend(
            aud="send",
            uid=uid,
            initially_queued_at=now,
            retry=0,
            last_queued_at=now,
            phone_number=phone_number,
            body=body,
            failure_job=failure_job,
            success_job=success_job,
        )
        .json()
        .encode("utf-8")
    )

    redis = await itgs.redis()

    async def prep(force: bool):
        await lib.sms.send_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await lib.sms.send_stats.attempt_increment_event(
                pipe, event="queued", now=now
            )
            await pipe.rpush(b"sms:to_send", entry)
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)
    return uid


async def retry_send(
    itgs: Itgs, *, sms: SMSToSend, now: Optional[float] = None
) -> None:
    """Returns the given SMS to the to send queue with an incremented number
    of retries. This should only be called by a failure callback on a retryable
    error, which failed on the `send` action.

    This will increment events as necessary.

    Args:
        itgs (Itgs): the integrations to (re)use
        sms (SMSToSend): the SMS to retry
        now (Optional[float], optional): the current time. Defaults to to time.time()
    """
    if now is None:
        now = time.time()

    entry = (
        SMSToSend(
            aud="send",
            uid=sms.uid,
            initially_queued_at=sms.initially_queued_at,
            retry=sms.retry + 1,
            last_queued_at=now,
            phone_number=sms.phone_number,
            body=sms.body,
            failure_job=sms.failure_job,
            success_job=sms.success_job,
        )
        .json()
        .encode("utf-8")
    )

    redis = await itgs.redis()

    async def prep(force: bool):
        await lib.sms.send_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await lib.sms.send_stats.attempt_increment_event(
                pipe, event="retried", now=sms.initially_queued_at
            )
            await pipe.rpush(b"sms:to_send", entry)
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def abandon_send(
    itgs: Itgs, *, sms: SMSToSend, now: Optional[float] = None
) -> None:
    """Abandons the given SMS that was in the to send queue. This should only
    be called by a failure callback on a retryable error, which failed on the
    `send` action.

    This will increment events as necessary.

    Args:
        itgs (Itgs): the integrations to (re)use
        sms (SMSToSend): the SMS to abandon
        now (Optional[float], optional): the current time. Defaults to to time.time()
    """
    await lib.sms.send_stats.increment_event(
        itgs, event="abandoned", now=sms.initially_queued_at
    )
