"""Hands sending emails from the To Send queue"""

import time
from typing import Dict, Literal, Optional
from error_middleware import handle_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import aioboto3
import aiohttp
import dataclasses
import os
import asyncio
import botocore.exceptions
import lib.emails.auth
from lib.basic_redis_lock import basic_redis_lock
from lib.emails.email_info import (
    EmailAttempt,
    EmailFailureInfo,
    EmailFailureInfoErrorIdentifier,
    EmailPending,
    encode_data_for_failure_job,
)
from lib.shared.clean_for_slack import clean_for_slack
import redis_helpers.run_with_prep
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
import unix_dates
import pytz
import email.utils

category = JobCategory.LOW_RESOURCE_COST
tz = pytz.timezone("America/Los_Angeles")

MAX_JOB_TIME_SECONDS = 50
EMAIL_SOURCE = email.utils.formataddr(
    ("Oseh", "hi@" + os.environ["ROOT_FRONTEND_URL"][len("https://") :])
)
EMAIL_REPLY_TO = email.utils.formataddr(("Customer Support", "hi@oseh.com"))
SUPPRESSED = os.environ["ENVIRONMENT"] == "dev"


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls messages off the email To Send queue, moves them to the
    email Send Purgatory, then templates them and sends them via Amazon SESV2.

    See: https://oseh.io/admin/email_dashboard

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(itgs, b"email:send_job:lock", gd=gd, spin=False):
        redis = await itgs.redis()
        await redis.hset(  # type: ignore
            b"stats:email_send:send_job", b"started_at", str(started_at).encode("ascii")  # type: ignore
        )

        num_in_purgatory = await redis.llen(b"email:send_purgatory")  # type: ignore
        if num_in_purgatory > 0:
            slack = await itgs.slack()
            await slack.send_web_error_message(
                f"**Email Send Job** - recovering {num_in_purgatory=} emails from purgatory",
                "Email Send Job - recovering from purgatory",
            )

            next_to_send_raw = await redis.lindex(b"email:send_purgatory", 0)  # type: ignore
            if next_to_send_raw is None:
                raise Exception(
                    f"Email Send Job - {num_in_purgatory=} but LINDEX 0 is None"
                )
        else:
            next_to_send_raw = await redis.lmove(
                b"email:to_send", b"email:send_purgatory", "LEFT", "RIGHT"  # type: ignore
            )
            num_in_purgatory = 1

        if next_to_send_raw is None:
            logging.info("Email Send Job - no messages to send")
            finished_at = time.time()
            await redis.hset(
                b"stats:email_send:send_job",  # type: ignore
                mapping={
                    b"finished_at": finished_at,
                    b"running_time": finished_at - started_at,
                    b"stop_reason": b"list_exhausted",
                    b"attempted": 0,
                    b"templated": 0,
                    b"accepted": 0,
                    b"failed_permanently": 0,
                    b"failed_transiently": 0,
                },
            )
            return

        logging.debug("Email Send Job - have work to do")

        session = aioboto3.Session()
        client = aiohttp.ClientSession(
            os.environ["ROOT_EMAIL_TEMPLATE_URL"],
            headers={
                "user-agent": "oseh via python aiohttp (+https://www.oseh.com)",
                "accept-encoding": "gzip",
            },
            auto_decompress=True,
        )

        async def advance_next_to_send_raw():
            nonlocal next_to_send_raw, num_in_purgatory
            assert num_in_purgatory > 0

            async with redis.pipeline() as pipe:
                pipe.multi()
                await pipe.lpop(b"email:send_purgatory")  # type: ignore
                num_in_purgatory -= 1

                if num_in_purgatory == 0:
                    await pipe.lmove(
                        b"email:to_send", b"email:send_purgatory", "LEFT", "RIGHT"  # type: ignore
                    )
                else:
                    await pipe.lindex(b"email:send_purgatory", 0)  # type: ignore
                result = await pipe.execute()

            next_to_send_raw = result[1]
            if num_in_purgatory == 0 and next_to_send_raw is not None:
                num_in_purgatory = 1

        run_stats = RunStats(
            attempted=0,
            templated=0,
            accepted=0,
            failed_permanently=0,
            failed_transiently=0,
        )
        stop_reason: Optional[str] = None
        want_failure_sleep: bool = False
        num_failures = 0
        jwts_by_slug: Dict[str, str] = dict()

        async with session.client("sesv2") as sesv2, client as conn:  # type: ignore
            while True:
                if next_to_send_raw is None:
                    logging.debug("Email Send Job - no more messages to send, stopping")
                    stop_reason = "list_exhausted"
                    break

                if gd.received_term_signal:
                    logging.debug("Email Send Job - signal received, stopping")
                    stop_reason = "signal"
                    break

                time_running_so_far = time.time() - started_at

                sleep_time = 0
                if want_failure_sleep:
                    num_failures += 1
                    sleep_time = 2 ** min(num_failures, 5)
                    logging.debug(
                        f"Email Send Job - sleeping {sleep_time}s before next request"
                    )

                if time_running_so_far + sleep_time >= MAX_JOB_TIME_SECONDS:
                    logging.info("Email Send Job - time limit reached, stopping")
                    stop_reason = "time_exhausted"
                    break

                if sleep_time > 0:
                    want_failure_sleep = False
                    await asyncio.sleep(sleep_time)

                try:
                    next_to_send = EmailAttempt.model_validate_json(next_to_send_raw)
                except Exception as exc:
                    await handle_error(
                        exc,
                        extra_info=f"Email Send Job - email\n\n```\n{clean_for_slack(next_to_send_raw)}\n```\nn is malformed",
                    )
                    await advance_next_to_send_raw()
                    continue

                if await is_in_suppression_list(itgs, next_to_send.email):
                    logging.debug(
                        f"Email Send Job - email {next_to_send.email} is in suppression list, skipping"
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="template",
                        identifier="Suppressed",
                        retryable=False,
                        extra=None,
                        events={
                            b"attempted": None,
                            b"failed_permanently": b"template:Suppressed",
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.failed_permanently += 1
                    await advance_next_to_send_raw()
                    continue

                jwt = jwts_by_slug.get(next_to_send.template)
                if jwt is None:
                    jwt = await lib.emails.auth.create_jwt(
                        itgs, next_to_send.template, duration=MAX_JOB_TIME_SECONDS + 10
                    )
                    jwts_by_slug[next_to_send.template] = jwt

                try:
                    html_response, plain_response = await asyncio.gather(
                        conn.post(
                            f"/api/3/templates/{next_to_send.template}",
                            json=next_to_send.template_parameters,
                            headers={
                                "accept": "text/html; charset=utf-8",
                                "content-type": "application/json; charset=utf-8",
                                "authorization": f"Bearer {jwt}",
                            },
                        ),
                        conn.post(
                            f"/api/3/templates/{next_to_send.template}",
                            json=next_to_send.template_parameters,
                            headers={
                                "accept": "text/plain; charset=utf-8",
                                "content-type": "application/json; charset=utf-8",
                                "authorization": f"Bearer {jwt}",
                            },
                        ),
                    )
                except Exception as e:
                    logging.warning(
                        "Email Send Job - failed to connect to email-templates",
                        exc_info=True,
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="template",
                        identifier="TemplateNetworkError",
                        retryable=True,
                        extra=str(e),
                        events={
                            b"attempted": None,
                            b"failed_transiently": b"template:NetworkError",
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.failed_transiently += 1
                    want_failure_sleep = True
                    await advance_next_to_send_raw()
                    continue

                logging.debug(
                    f"Received HTTP status codes {html_response.status=} {plain_response.status=}"
                )

                html_not_desired_for_template = html_response.status == 404
                plain_not_desired_for_template = plain_response.status == 404

                if html_not_desired_for_template and plain_not_desired_for_template:
                    html_response.close()
                    plain_response.close()
                    logging.warning(
                        "Email Send Job - email-templates returned 404 for both html and plain variants for "
                        f"{next_to_send=}: this likely means the template does not exist! Either way, we cannot "
                        "send this email.",
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="template",
                        identifier="TemplateClientError",
                        retryable=False,
                        extra=f"{html_response.status=} {plain_response.status=}",
                        events={
                            b"attempted": None,
                            b"failed_permanently": b"template:404",
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.failed_permanently += 1
                    await advance_next_to_send_raw()
                    continue

                worst_status = max(
                    [
                        s
                        for s in (html_response.status, plain_response.status)
                        if s != 404
                    ]
                )
                if worst_status >= 500:
                    html_response.close()
                    plain_response.close()
                    logging.warning(
                        f"Email Send Job - email-templates returned ServerError {html_response.status=} {plain_response.status=} "
                        f"to {next_to_send=}"
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="template",
                        identifier="TemplateServerError",
                        retryable=True,
                        extra=f"{html_response.status=} {plain_response.status=}",
                        events={
                            b"attempted": None,
                            b"failed_transiently": f"template:{worst_status}".encode(
                                "ascii"
                            ),
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.failed_transiently += 1
                    if any(r.status == 503 for r in [html_response, plain_response]):
                        want_failure_sleep = True
                    await advance_next_to_send_raw()
                    continue

                if worst_status >= 400:
                    html_response.close()
                    plain_response.close()
                    logging.warning(
                        f"Email Send Job - email-templates returned ClientError {html_response.status=} {plain_response.status=} "
                        f"to {next_to_send=}"
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="template",
                        identifier="TemplateClientError",
                        retryable=False,
                        extra=f"{html_response.status=} {plain_response.status=}",
                        events={
                            b"attempted": None,
                            b"failed_permanently": f"template:{worst_status}".encode(
                                "ascii"
                            ),
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.failed_permanently += 1
                    await advance_next_to_send_raw()
                    continue

                if html_not_desired_for_template:
                    html_response.close()

                if plain_not_desired_for_template:
                    plain_response.close()

                try:
                    html_read_task = (
                        None
                        if html_not_desired_for_template
                        else asyncio.create_task(html_response.read())
                    )
                    plain_read_task = (
                        None
                        if plain_not_desired_for_template
                        else asyncio.create_task(plain_response.read())
                    )
                    read_tasks = [
                        t for t in (html_read_task, plain_read_task) if t is not None
                    ]
                    await asyncio.wait(read_tasks, return_when=asyncio.ALL_COMPLETED)
                    template_html_bytes = (
                        html_read_task.result() if html_read_task is not None else None
                    )
                    template_plain_bytes = (
                        plain_read_task.result()
                        if plain_read_task is not None
                        else None
                    )
                    if not html_not_desired_for_template:
                        html_response.close()
                    if not plain_not_desired_for_template:
                        plain_response.close()

                    template_html = (
                        template_html_bytes.decode("utf-8")
                        if template_html_bytes is not None
                        else None
                    )
                    template_plain = (
                        template_plain_bytes.decode("utf-8")
                        if template_plain_bytes is not None
                        else None
                    )
                except Exception as e:
                    logging.warning(
                        "Email Send Job - failed to read email-templates response",
                        exc_info=True,
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="template",
                        identifier="TemplateInternalError",
                        retryable=False,
                        extra=str(e),
                        events={
                            b"attempted": None,
                            b"failed_permanently": b"template:InternalError",
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.failed_permanently += 1
                    await advance_next_to_send_raw()
                    continue

                if SUPPRESSED:
                    logging.debug(
                        "Suppressing sending emails in dev environment, faking ses client error"
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="send",
                        identifier="SESClientError",
                        retryable=False,
                        extra="Email sending is suppressed when running in the dev environment",
                        events={
                            b"attempted": None,
                            b"templated": None,
                            b"failed_permanently": f"ses:SuppressedInDevEnvironment".encode(
                                "utf-8"
                            ),
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.templated += 1
                    run_stats.failed_permanently += 1
                    await advance_next_to_send_raw()
                    continue

                try:
                    result = await sesv2.send_email(
                        FromEmailAddress=EMAIL_SOURCE,
                        Destination={
                            "ToAddresses": [next_to_send.email],
                            "CcAddresses": [],
                            "BccAddresses": [],
                        },
                        ReplyToAddresses=[EMAIL_REPLY_TO],
                        Content={
                            "Simple": {
                                "Subject": {
                                    "Data": next_to_send.subject,
                                },
                                "Body": {
                                    **(
                                        {
                                            "Html": {
                                                "Data": template_html,
                                                "Charset": "UTF-8",
                                            }
                                        }
                                        if template_html is not None
                                        else {}
                                    ),
                                    **(
                                        {
                                            "Text": {
                                                "Data": template_plain,
                                                "Charset": "UTF-8",
                                            }
                                        }
                                        if template_plain is not None
                                        else {}
                                    ),
                                },
                            }
                        },
                    )
                    message_id = result["MessageId"]
                    assert isinstance(message_id, str)
                    assert message_id != ""
                except (
                    botocore.exceptions.NoCredentialsError,
                    botocore.exceptions.CredentialRetrievalError,
                ):
                    logging.warning(
                        f"Email Send Job - failed to send email to {next_to_send.email} due to an issue locating AWS "
                        "credentials. This usually resolves itself, but requires we recreate the SESV2 client. "
                        "Stopping the job early and leaving this email in purgatory to allow this to happen.",
                        exc_info=True,
                    )
                    stop_reason = "credentials"
                    break
                except botocore.exceptions.ConnectionError:
                    logging.warning(
                        f"Email Send Job - failed to send email to {next_to_send.email} due to a connection error.",
                        exc_info=True,
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="send",
                        identifier="SESNetworkError",
                        retryable=True,
                        extra=None,
                        events={
                            b"attempted": None,
                            b"templated": None,
                            b"failed_transiently": b"ses:ConnectionError",
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.templated += 1
                    run_stats.failed_transiently += 1
                    want_failure_sleep = True
                    await advance_next_to_send_raw()
                    continue
                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == "TooManyRequestsException":
                        logging.warning(
                            f"Email Send Job - failed to send email to {next_to_send.email} due to a rate limit error.",
                            exc_info=True,
                        )
                        await fail_job_and_update_stats(
                            itgs,
                            email=next_to_send,
                            action="send",
                            identifier="SESTooManyRequestsException",
                            retryable=True,
                            extra=None,
                            events={
                                b"attempted": None,
                                b"templated": None,
                                b"failed_transiently": b"ses:TooManyRequestsException",
                            },
                            now=time.time(),
                        )
                        run_stats.attempted += 1
                        run_stats.templated += 1
                        run_stats.failed_transiently += 1
                        want_failure_sleep = True
                        await advance_next_to_send_raw()
                        continue
                    logging.warning(
                        f"Email Send Job - failed to send email to {next_to_send.email} due to a client error.",
                        exc_info=True,
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="send",
                        identifier="SESClientError",
                        retryable=False,
                        extra=str(e),
                        events={
                            b"attempted": None,
                            b"templated": None,
                            b"failed_permanently": f"ses:{type(e).__name__}".encode(
                                "utf-8"
                            ),
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.templated += 1
                    run_stats.failed_permanently += 1
                    await advance_next_to_send_raw()
                    continue
                except Exception as e:
                    logging.warning(
                        f"Email Send Job - failed to send email to {next_to_send.email} due to an unknown error.",
                        exc_info=True,
                    )
                    await fail_job_and_update_stats(
                        itgs,
                        email=next_to_send,
                        action="send",
                        identifier="SESInternalError",
                        retryable=False,
                        extra=str(e),
                        events={
                            b"attempted": None,
                            b"templated": None,
                            b"failed_permanently": f"ses:{type(e).__name__}".encode(
                                "utf-8"
                            ),
                        },
                        now=time.time(),
                    )
                    run_stats.attempted += 1
                    run_stats.templated += 1
                    run_stats.failed_permanently += 1
                    await advance_next_to_send_raw()
                    continue

                logging.debug(
                    f"Email Send Job - sent email {message_id} to {next_to_send.email} with template {next_to_send.template}"
                )
                await add_to_pending_and_update_stats(
                    itgs, email=next_to_send, message_id=message_id, now=time.time()
                )
                run_stats.attempted += 1
                run_stats.templated += 1
                run_stats.accepted += 1
                await advance_next_to_send_raw()

        finished_at = time.time()
        logging.info(
            f"Email Send Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Attempted: {run_stats.attempted}\n"
            f"- Templated: {run_stats.templated}\n"
            f"- Accepted: {run_stats.accepted}\n"
            f"- Failed Permanently: {run_stats.failed_permanently}\n"
            f"- Failed Transiently: {run_stats.failed_transiently}"
        )
        await redis.hset(
            b"stats:email_send:send_job",  # type: ignore
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"stop_reason": stop_reason.encode("utf-8"),
                b"attempted": run_stats.attempted,
                b"templated": run_stats.templated,
                b"accepted": run_stats.accepted,
                b"failed_permanently": run_stats.failed_permanently,
                b"failed_transiently": run_stats.failed_transiently,
            },
        )


@dataclasses.dataclass
class RunStats:
    attempted: int
    templated: int
    accepted: int
    failed_permanently: int
    failed_transiently: int


async def fail_job_and_update_stats(
    itgs: Itgs,
    *,
    email: EmailAttempt,
    action: Literal["template", "send"],
    identifier: EmailFailureInfoErrorIdentifier,
    retryable: bool,
    extra: Optional[str] = None,
    events: Dict[bytes, Optional[bytes]],
    now: float,
) -> None:
    jobs = await itgs.jobs()
    redis = await itgs.redis()

    today = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)
    key = f"stats:email_send:daily:{today}".encode("ascii")

    data_raw = encode_data_for_failure_job(
        email=email,
        info=EmailFailureInfo(
            step=action,
            error_identifier=identifier,
            retryable=retryable,
            extra=extra,
        ),
    )

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_send:daily:earliest", today)
            for event, event_extra in events.items():
                await pipe.hincrby(key, event, 1)  # type: ignore
                if event_extra is not None:
                    await pipe.hincrby(  # type: ignore
                        f"stats:email_send:daily:{today}:extra:{event.decode('utf-8')}".encode(  # type: ignore
                            "utf-8"
                        ),
                        event_extra,  # type: ignore
                        1,
                    )
            await jobs.enqueue_in_pipe(
                pipe,
                email.failure_job.name,
                **email.failure_job.kwargs,
                data_raw=data_raw,
            )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def add_to_pending_and_update_stats(
    itgs: Itgs, *, email: EmailAttempt, message_id: str, now: float
) -> None:
    now = time.time()
    entry = EmailPending(
        aud="pending",
        uid=email.uid,
        message_id=message_id,
        email=email.email,
        subject=email.subject,
        template=email.template,
        template_parameters=email.template_parameters,
        send_initially_queued_at=email.initially_queued_at,
        send_accepted_at=now,
        failure_job=email.failure_job,
        success_job=email.success_job,
    ).as_redis_mapping()

    today = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)
    key = f"stats:email_send:daily:{today}".encode("ascii")
    accepted_extra = f"stats:email_send:daily:{today}:extra:accepted".encode("ascii")

    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:email_send:daily:earliest", today)
            await pipe.hincrby(key, b"attempted", 1)  # type: ignore
            await pipe.hincrby(key, b"templated", 1)  # type: ignore
            await pipe.hincrby(key, b"accepted", 1)  # type: ignore
            await pipe.hincrby(accepted_extra, email.template.encode("utf-8"), 1)  # type: ignore
            await pipe.zadd(
                b"email:receipt_pending",
                mapping={
                    message_id.encode("utf-8"): now,
                },
            )
            await pipe.hset(  # type: ignore
                f"email:receipt_pending:{message_id}".encode("utf-8"), mapping=entry  # type: ignore
            )
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def is_in_suppression_list(itgs: Itgs, email: str) -> bool:
    """Checks if the given email is in our suppression list."""
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        "SELECT 1 FROM suppressed_emails WHERE email_address=? COLLATE NOCASE",
        (email,),
    )

    return not not response.results


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.emails.send")

    asyncio.run(main())
