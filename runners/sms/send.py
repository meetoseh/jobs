"""SMS Send Job"""
import logging
import time
from typing import Optional, Union
import aiohttp
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from lib.sms.sms_info import (
    MessageResource,
    PendingSMS,
    SMSFailureInfo,
    SMSSuccessResult,
    SMSToSend,
    encode_data_for_failure_job,
    encode_data_for_success_job,
)
import os
import base64
from urllib.parse import urlencode
import dataclasses
import lib.sms.send_stats
import email.utils
import asyncio

category = JobCategory.LOW_RESOURCE_COST


MAX_JOB_TIME_SECONDS = 50
TWILIO_RATELIMIT_ERROR_CODES = frozenset(
    ("14107", "30022", "31206", "45010", "51002", "54009", "63017")
)
SUCCEEDED_MESSAGE_STATUSES = frozenset(("sent", "delivered", "read"))
FAILED_MESSAGE_STATUSES = frozenset(("canceled", "undelivered", "failed"))
WEBHOOK_WAIT_TIME_SECONDS = 60 if os.environ["ENVIRONMENT"] == "dev" else 900

STATUS_CALLBACK = (
    None
    if os.environ["ENVIRONMENT"] == "dev"
    else (os.environ["ROOT_BACKEND_URL"] + "/api/1/sms/webhook")
)


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls messages one at a time off the To Send queue, moving them to the To
    Send Purgatory, then to Twilio, then:
    - if immediately successful, queues the success callback
    - if pending, adds to the Receipt Pending Set
    - if failed, queues the failure callback

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(itgs, b"sms:send_job:lock", gd=gd, spin=False):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:sms_send:send_job",
            b"started_at",
            str(started_at).encode("ascii"),
        )

        num_in_purgatory = await redis.llen(b"sms:send_purgatory")
        if num_in_purgatory > 0:
            slack = await itgs.slack()
            await slack.send_web_error_message(
                f"**SMS Send Job** - recovering {num_in_purgatory=} sms from purgatory",
                "SMS Send Job - recovering from purgatory",
            )

            next_to_send_raw = await redis.lindex(b"sms:send_purgatory", 0)
            if next_to_send_raw is None:
                raise Exception(
                    f"SMS Send Job - {num_in_purgatory=} but LINDEX 0 is None"
                )
        else:
            next_to_send_raw = await redis.lmove(
                b"sms:to_send", b"sms:send_purgatory", "LEFT", "RIGHT"
            )
            num_in_purgatory = 1

        if next_to_send_raw is None:
            logging.info("SMS Send Job - no messages to send")
            finished_at = time.time()
            await redis.hset(
                b"stats:sms_send:send_job",
                mapping={
                    b"finished_at": finished_at,
                    b"running_time": finished_at - started_at,
                    b"stop_reason": b"list_exhausted",
                    b"attempted": 0,
                    b"pending": 0,
                    b"succeeded": 0,
                    b"failed_permanently": 0,
                    b"failed_transiently": 0,
                },
            )
            return

        logging.debug("SMS Send Job have work to do, initializing connection")

        twilio_account_sid = os.environ["OSEH_TWILIO_ACCOUNT_SID"]
        twilio_auth_token = os.environ["OSEH_TWILIO_AUTH_TOKEN"]
        twilio_sender = os.environ["OSEH_TWILIO_MESSAGE_SERVICE_SID"]

        create_message_resource_path = (
            f"/2010-04-01/Accounts/{twilio_account_sid}/Messages.json"
        )

        client = aiohttp.ClientSession(
            "https://api.twilio.com",
            headers={
                "user-agent": "oseh via python aiohttp (+https://www.oseh.com)",
                "accept": "application/json",
                "accept-encoding": "identity",
                "authorization": f"Basic {encode_basic_auth(twilio_account_sid, twilio_auth_token)}",
            },
        )

        async def advance_next_to_send_raw():
            nonlocal next_to_send_raw, num_in_purgatory
            assert num_in_purgatory > 0

            async with redis.pipeline() as pipe:
                pipe.multi()
                await pipe.lpop(b"sms:send_purgatory")
                num_in_purgatory -= 1

                if num_in_purgatory == 0:
                    await pipe.lmove(
                        b"sms:to_send", b"sms:send_purgatory", "LEFT", "RIGHT"
                    )
                else:
                    await pipe.lindex(b"sms:send_purgatory", 0)
                result = await pipe.execute()

            next_to_send_raw = result[1]
            if num_in_purgatory == 0 and next_to_send_raw is not None:
                num_in_purgatory = 1

        run_stats = RunStats(
            attempted=0,
            pending=0,
            succeeded=0,
            failed_permanently=0,
            failed_transiently=0,
        )
        stop_reason: Optional[str] = None
        want_failure_sleep: bool = False

        num_failures = 0

        async with client as conn:
            while True:
                if next_to_send_raw is None:
                    logging.debug("SMS Send Job - no more messages to send, stopping")
                    stop_reason = "list_exhausted"
                    break

                if gd.received_term_signal:
                    logging.debug("SMS Send Job - signal received, stopping")
                    stop_reason = "signal"
                    break

                time_running_so_far = time.time() - started_at

                sleep_time = 0
                if want_failure_sleep:
                    num_failures += 1
                    sleep_time = 2 ** min(num_failures, 5)
                    logging.debug(
                        f"SMS Send Job - sleeping {sleep_time}s before next request"
                    )

                if time_running_so_far + sleep_time >= MAX_JOB_TIME_SECONDS:
                    logging.info("SMS Send Job - time limit reached, stopping")
                    stop_reason = "time_exhausted"
                    break

                if sleep_time > 0:
                    want_failure_sleep = False
                    await asyncio.sleep(sleep_time)

                next_to_send = SMSToSend.parse_raw(
                    next_to_send_raw, content_type="application/json"
                )
                logging.info(f"SMS Send Job - sending {next_to_send=}")

                request_data = {
                    "MessagingServiceSid": twilio_sender,
                    "To": next_to_send.phone_number,
                    "Body": next_to_send.body,
                }

                if STATUS_CALLBACK is not None:
                    request_data["StatusCallback"] = STATUS_CALLBACK

                if next_to_send.phone_number == "+15555555555":
                    logging.info(
                        "Test number detected, failing send with i'm a teapot server status code.\n"
                        f"{request_data=}"
                    )
                    await fail_job(
                        itgs,
                        sms=next_to_send,
                        identifier="ClientErrorOther",
                        subidentifier="418",
                        retryable=False,
                        extra=None,
                    )
                    run_stats.attempted += 1
                    run_stats.failed_permanently += 1
                    await lib.sms.send_stats.increment_event(
                        itgs,
                        event="failed_due_to_client_error_other",
                        extra={
                            "http_status_code": "418",
                        },
                        now=next_to_send.initially_queued_at,
                    )
                    await advance_next_to_send_raw()
                    continue

                try:
                    async with basic_redis_lock(itgs, b"twilio:lock", gd=gd, spin=True):
                        response = await conn.post(
                            create_message_resource_path,
                            headers={
                                "content-type": "application/x-www-form-urlencoded",
                            },
                            data=urlencode(request_data),
                        )
                except Exception as e:
                    logging.warning(
                        "SMS Send Job - failed to connect to Twilio", exc_info=True
                    )
                    await fail_job(
                        itgs,
                        sms=next_to_send,
                        identifier="NetworkError",
                        retryable=True,
                        extra=str(e),
                    )
                    run_stats.attempted += 1
                    run_stats.failed_transiently += 1
                    await lib.sms.send_stats.increment_event(
                        itgs,
                        event="failed_due_to_network_error",
                        now=next_to_send.initially_queued_at,
                    )
                    want_failure_sleep = True
                    await advance_next_to_send_raw()
                    continue

                logging.debug(f"Received HTTP status: {response.status}")

                if response.status >= 500:
                    logging.warning(
                        f"SMS Send Job - server error",
                        exc_info=False,
                    )
                    await fail_job(
                        itgs,
                        sms=next_to_send,
                        identifier="ServerError",
                        subidentifier=str(response.status),
                        retryable=True,
                        extra=None,
                    )
                    run_stats.attempted += 1
                    run_stats.failed_transiently += 1
                    await lib.sms.send_stats.increment_event(
                        itgs,
                        event="failed_due_to_server_error",
                        extra={
                            "http_status_code": response.status,
                        },
                        now=next_to_send.initially_queued_at,
                    )
                    want_failure_sleep = True
                    await advance_next_to_send_raw()
                    continue

                # For all non-5xx responses we will attempt to parse the response
                # but we will only consider failure to parse an internal error if
                # the response is 2xx

                try:
                    message_resource_obj = await response.json()
                    if not isinstance(message_resource_obj, dict):
                        raise ValueError(
                            f"response is not a JSON object: {message_resource_obj}"
                        )

                    for req_str_key in ("sid", "status", "date_updated"):
                        if req_str_key not in message_resource_obj:
                            raise ValueError(
                                f"missing '{req_str_key}' in response: {message_resource_obj}"
                            )
                        if not isinstance(message_resource_obj[req_str_key], str):
                            raise ValueError(
                                f"'{req_str_key}' is not a string in response: {message_resource_obj}"
                            )

                    if not isinstance(
                        message_resource_obj.get("error_code", None),
                        (str, int, type(None)),
                    ):
                        raise ValueError(
                            f"'error_code' is not a string or null in response: {message_resource_obj}"
                        )

                    if (
                        response.status >= 300
                        and message_resource_obj.get("error_code") is None
                    ):
                        raise ValueError(
                            f"non-2xx response with no 'error_code': {message_resource_obj}"
                        )

                    date_updated_str: str = message_resource_obj["date_updated"]
                    error_code_raw: Optional[
                        Union[str, int]
                    ] = message_resource_obj.get("error_code")
                    message_resource = MessageResource(
                        sid=message_resource_obj["sid"],
                        status=message_resource_obj["status"],
                        error_code=str(error_code_raw)
                        if error_code_raw is not None
                        else None,
                        date_updated=email.utils.parsedate_to_datetime(
                            date_updated_str
                        ).timestamp(),
                    )
                except Exception as e:
                    logging.debug(
                        f"SMS Send Job - failed to parse response",
                        exc_info=True,
                    )

                    if response.status >= 200 and response.status <= 299:
                        logging.warning(
                            "SMS Send Job - successful status code, failed to parse MessageResource: internal error"
                        )
                        await fail_job(
                            itgs,
                            sms=next_to_send,
                            identifier="InternalError",
                            subidentifier=None,
                            retryable=False,
                            extra=str(e),
                        )
                        run_stats.attempted += 1
                        run_stats.failed_permanently += 1
                        await lib.sms.send_stats.increment_event(
                            itgs,
                            event="failed_due_to_internal_error",
                            now=next_to_send.initially_queued_at,
                        )
                        await advance_next_to_send_raw()
                        continue

                    if response.status == 429:
                        logging.warning(
                            "SMS Send Job - rate limit exceeded via hard 429"
                        )
                        await fail_job(
                            itgs,
                            sms=next_to_send,
                            identifier="ClientError429",
                            subidentifier=None,
                            retryable=True,
                            extra=None,
                        )
                        run_stats.attempted += 1
                        run_stats.failed_transiently += 1
                        await lib.sms.send_stats.increment_event(
                            itgs,
                            event="failed_due_to_client_error_429",
                            now=next_to_send.initially_queued_at,
                        )
                        want_failure_sleep = True
                        await advance_next_to_send_raw()
                        continue

                    logging.warning("SMS Send Job - client error")
                    await fail_job(
                        itgs,
                        sms=next_to_send,
                        identifier="ClientErrorOther",
                        subidentifier=str(response.status),
                        retryable=False,
                        extra=None,
                    )
                    run_stats.attempted += 1
                    run_stats.failed_permanently += 1
                    await lib.sms.send_stats.increment_event(
                        itgs,
                        event="failed_due_to_client_error_other",
                        extra={
                            "http_status_code": response.status,
                        },
                        now=next_to_send.initially_queued_at,
                    )
                    want_failure_sleep = True
                    await advance_next_to_send_raw()
                    continue

                if (
                    message_resource.status in FAILED_MESSAGE_STATUSES
                    and message_resource.error_code is None
                ):
                    logging.warning(
                        f"SMS Send Job - got failure status {message_resource.status} but no error code; "
                        "assigning fake error code 'failure_status_without_error_code'"
                    )
                    message_resource = dataclasses.replace(
                        message_resource, error_code="failure_status_without_error_code"
                    )

                if message_resource.error_code is not None:
                    if (
                        response.status == 429
                        or message_resource.error_code in TWILIO_RATELIMIT_ERROR_CODES
                    ):
                        logging.warning(
                            f"SMS Send Job - application level rate limit exceeded via {message_resource.error_code}"
                        )
                        await fail_job(
                            itgs,
                            sms=next_to_send,
                            identifier="ApplicationErrorRatelimit",
                            subidentifier=message_resource.error_code,
                            retryable=True,
                            extra=None,
                        )
                        run_stats.attempted += 1
                        run_stats.failed_transiently += 1
                        await lib.sms.send_stats.increment_event(
                            itgs,
                            event="failed_due_to_application_error_ratelimit",
                            extra={
                                "error_code": message_resource.error_code,
                            },
                            now=next_to_send.initially_queued_at,
                        )
                        want_failure_sleep = True
                        await advance_next_to_send_raw()
                        continue

                    logging.warning(
                        f"SMS Send Job - application level error: {message_resource.error_code}"
                    )
                    await fail_job(
                        itgs,
                        sms=next_to_send,
                        identifier="ApplicationErrorOther",
                        subidentifier=message_resource.error_code,
                        retryable=False,
                        extra=None,
                    )
                    run_stats.attempted += 1
                    run_stats.failed_permanently += 1
                    await lib.sms.send_stats.increment_event(
                        itgs,
                        event="failed_due_to_application_error_other",
                        extra={
                            "error_code": message_resource.error_code,
                        },
                        now=next_to_send.initially_queued_at,
                    )
                    want_failure_sleep = True
                    await advance_next_to_send_raw()
                    continue

                if response.status < 200 or response.status > 299:
                    logging.warning(
                        "SMS Send Job - unsuccessful status code but successfully parsed MessageResource, "
                        "and it has no error code. Ignoring the http status code, but it is a little odd. "
                        f"{response.status}: {message_resource.json()}"
                    )

                if message_resource.status in SUCCEEDED_MESSAGE_STATUSES:
                    logging.debug(
                        f"SMS Send Job - got immediate success status {message_resource.status}. This "
                        "is a bit surprising since Twilio ought to queue the operation"
                    )
                    await succeed_job(
                        itgs, sms=next_to_send, message_resource=message_resource
                    )
                    run_stats.attempted += 1
                    run_stats.succeeded += 1
                    await lib.sms.send_stats.increment_event(
                        itgs,
                        event="succeeded_immediate",
                        extra={
                            "status": message_resource.status,
                        },
                        now=next_to_send.initially_queued_at,
                    )
                    await advance_next_to_send_raw()
                    continue

                logging.debug(
                    f"SMS Send Job - got pending status {message_resource.status}"
                )
                await add_to_pending(
                    itgs, sms=next_to_send, message_resource=message_resource
                )
                run_stats.attempted += 1
                run_stats.pending += 1
                await lib.sms.send_stats.increment_event(
                    itgs,
                    event="succeeded_pending",
                    extra={
                        "status": message_resource.status,
                    },
                    now=next_to_send.initially_queued_at,
                )
                await advance_next_to_send_raw()
                continue

        finished_at = time.time()
        logging.info(
            f"SMS Send Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Attempted: {run_stats.attempted}\n"
            f"- Pending: {run_stats.pending}\n"
            f"- Succeeded: {run_stats.succeeded}\n"
            f"- Failed Permanently: {run_stats.failed_permanently}\n"
            f"- Failed Transiently: {run_stats.failed_transiently}"
        )
        await redis.hset(
            b"stats:sms_send:send_job",
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"stop_reason": stop_reason.encode("utf-8"),
                b"attempted": run_stats.attempted,
                b"pending": run_stats.pending,
                b"succeeded": run_stats.succeeded,
                b"failed_permanently": run_stats.failed_permanently,
                b"failed_transiently": run_stats.failed_transiently,
            },
        )


@dataclasses.dataclass
class RunStats:
    attempted: int
    pending: int
    succeeded: int
    failed_permanently: int
    failed_transiently: int


def encode_basic_auth(username: str, password: str) -> str:
    return base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")


async def fail_job(
    itgs: Itgs,
    *,
    sms: SMSToSend,
    identifier: str,
    subidentifier: Optional[str] = None,
    retryable: bool,
    extra: Optional[str] = None,
) -> None:
    jobs = await itgs.jobs()
    await jobs.enqueue(
        sms.failure_job.name,
        **sms.failure_job.kwargs,
        data_raw=encode_data_for_failure_job(
            sms=sms,
            failure_info=SMSFailureInfo(
                action="send",
                identifier=identifier,
                subidentifier=subidentifier,
                retryable=retryable,
                extra=extra,
            ),
        ),
    )


async def succeed_job(
    itgs: Itgs,
    *,
    sms: SMSToSend,
    message_resource: MessageResource,
) -> None:
    jobs = await itgs.jobs()
    now = time.time()
    await jobs.enqueue(
        sms.success_job.name,
        **sms.success_job.kwargs,
        data_raw=encode_data_for_success_job(
            sms=sms,
            result=SMSSuccessResult(
                message_resource_created_at=now,
                message_resource_succeeded_at=now,
                message_resource=message_resource,
            ),
        ),
    )


async def add_to_pending(
    itgs: Itgs, *, sms: SMSToSend, message_resource: MessageResource
) -> None:
    now = time.time()
    entry = PendingSMS(
        aud="pending",
        uid=sms.uid,
        send_initially_queued_at=sms.initially_queued_at,
        message_resource_created_at=now,
        message_resource_last_updated_at=now,
        message_resource=message_resource,
        failure_job_last_called_at=None,
        num_failures=0,
        num_changes=0,
        phone_number=sms.phone_number,
        body=sms.body,
        failure_job=sms.failure_job,
        success_job=sms.success_job,
    ).as_redis_mapping()

    redis = await itgs.redis()
    async with redis.pipeline() as pipe:
        pipe.multi()
        await pipe.zadd(
            b"sms:pending",
            mapping={
                message_resource.sid.encode("utf-8"): now + WEBHOOK_WAIT_TIME_SECONDS,
            },
        )
        await pipe.hset(
            f"sms:pending:{message_resource.sid}".encode("utf-8"), mapping=entry
        )
        await pipe.execute()


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sms.send")

    asyncio.run(main())
