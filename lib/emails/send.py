import asyncio
import json
import secrets
from typing import Optional
from pydantic import BaseModel, Field
import pytz
from lib.shared.job_callback import JobCallback
from lib.emails.email_info import EmailAttempt
from itgs import Itgs
import time
import redis_helpers.run_with_prep
from redis_helpers.set_if_lower import ensure_set_if_lower_script_exists, set_if_lower
import unix_dates


def create_email_uid() -> str:
    """Generates a new uid appropriate for an email being added to to_send. It is often
    convenient to fix the uid prior to inserting into the to send queue, to ensure the
    callback jobs are ready before they are called
    """
    return f"oseh_em_{secrets.token_urlsafe(16)}"


class EmailMessageContents(BaseModel):
    """Convenience class for containing the contents of an email, unused
    here
    """

    subject: str = Field(description="Email subject line")
    template: str = Field(description="Email template slug")
    template_parameters: dict = Field(
        description="Email template parameters, as a dict"
    )


async def send_email(
    itgs: Itgs,
    *,
    email: str,
    subject: str,
    template: str,
    template_parameters: dict,
    success_job: JobCallback,
    failure_job: JobCallback,
    uid: Optional[str] = None,
    now: Optional[float] = None,
) -> str:
    """Queues sending an email. This will create the email html/plaintext using the
    given template on email-templates applied with the given arguments, then sends
    the email using the
    [Amazon SESV2 API](https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_SendEmail.html).
    Assuming everything goes well, we eventually receive a
    [delivery notification](https://docs.aws.amazon.com/ses/latest/dg/notification-contents.html).
    At that point, this calls the success_callback with one additional keyword
    argument: `data_raw` which is decoded via `decode_data_for_success_job`.

    If the templating fails, or we receive a bounce or complaint notification
    prior to a delivery notification for this message, or we don't receive a
    notification at all for an excessive period of time, the failure callback is
    queued with one additional keyword argument: `data_raw`, which is decoded
    via `decode_data_for_failure_job`.

    If we receive a bounce or complaint notification after we have already queued
    the success callback, a generic failure callback is queued; see
    `runners/emails/abandoned_email_callback.py`.

    Args:
        itgs (Itgs): the integrations to (re)use
        email (str): the email address of the recipient
        subject (str): the subject line of the email
        template (str): the slug of the template to use
        template_parameters (dict): the template arguments to use
        success_job (JobCallback): the callback to call on success
        failure_job (JobCallback): the callback to call on failure. Be careful
          interpreting the number of failures, as the callback is pre-increment for
          `send` and post-increment for `check`.
        uid (Optional[str], optional): the uid to use for the email. Defaults to a new
          random uid
        now (Optional[float], optional): the current time. Defaults to to time.time()

    Returns:
        str: The UID of the email queued to be sent
    """
    if now is None:
        now = time.time()

    if uid is None:
        uid = create_email_uid()

    entry = (
        EmailAttempt(
            aud="send",
            uid=uid,
            email=email,
            subject=subject,
            template=template,
            template_parameters=template_parameters,
            initially_queued_at=now,
            retry=0,
            last_queued_at=now,
            failure_job=failure_job,
            success_job=success_job,
        )
        .model_dump_json()
        .encode("utf-8")
    )

    today = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )

    redis = await itgs.redis()
    key = f"stats:email_send:daily:{today}".encode("ascii")

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_send:daily:earliest", today)
            await pipe.hincrby(key, b"queued", 1)  # type: ignore
            await pipe.rpush(b"email:to_send", entry)  # type: ignore
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)
    return uid


async def retry_send(
    itgs: Itgs, *, email: EmailAttempt, now: Optional[float] = None
) -> None:
    """Returns the given email to the to send queue with an incremented number
    of retries. This should only be called by a failure callback on a retryable
    error, which failed on the `send` action.

    This will increment events as necessary.

    Args:
        itgs (Itgs): the integrations to (re)use
        email (EmailAttempt): the email to retry
        now (Optional[float], optional): the current time. Defaults to to time.time()
    """
    if now is None:
        now = time.time()

    today = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )
    key = f"stats:email_send:daily:{today}".encode("ascii")

    entry = (
        EmailAttempt(
            aud="send",
            uid=email.uid,
            email=email.email,
            subject=email.subject,
            template=email.template,
            template_parameters=email.template_parameters,
            initially_queued_at=email.initially_queued_at,
            retry=email.retry + 1,
            last_queued_at=now,
            failure_job=email.failure_job,
            success_job=email.success_job,
        )
        .model_dump_json()
        .encode("utf-8")
    )

    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_send:daily:earliest", today)
            await pipe.hincrby(key, b"retried", 1)  # type: ignore
            await pipe.rpush(b"email:to_send", entry)  # type: ignore
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def abandon_send(
    itgs: Itgs, *, email: EmailAttempt, now: Optional[float] = None
) -> None:
    """Abandons the given email that was in the to send queue. This should only
    be called by a failure callback on a retryable error, which failed on the
    `send` action.

    This will increment events as necessary.

    Args:
        itgs (Itgs): the integrations to (re)use
        email (EmailAttempt): the email to abandon
        now (Optional[float], optional): the current time. Defaults to to time.time()
    """
    if now is None:
        now = time.time()

    today = unix_dates.unix_timestamp_to_unix_date(
        now, tz=pytz.timezone("America/Los_Angeles")
    )
    key = f"stats:email_send:daily:{today}".encode("ascii")

    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_send:daily:earliest", today)
            await pipe.hincrby(key, b"abandoned", 1)  # type: ignore
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


if __name__ == "__main__":

    async def main():
        email = input("email: ")
        subject = input("subject: ")
        template = input("template: ")
        template_parameters_raw = input("template_parameters (as json, one line): ")
        template_parameters = json.loads(template_parameters_raw)
        success_job = JobCallback(
            name="runners.emails.test_success_handler", kwargs=dict()
        )
        failure_job = JobCallback(
            name="runners.emails.test_failure_handler", kwargs=dict()
        )

        async with Itgs() as itgs:
            await send_email(
                itgs,
                email=email,
                subject=subject,
                template=template,
                template_parameters=template_parameters,
                success_job=success_job,
                failure_job=failure_job,
            )

        print("email send queued")

    asyncio.run(main())
