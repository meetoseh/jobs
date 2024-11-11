"""Our "Send Job", which converts message attempts into push tickets"""

import gzip
import json
import os
from typing import Dict, List, Optional, Tuple
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import time
from h2client.session import Session
from h2client.simple_connection import ResponseData
from lib.basic_redis_lock import basic_redis_lock
from lib.push.message_attempt_info import (
    MessageAttemptFailureInfo,
    MessageAttemptFailureInfoIdentifier,
    MessageAttemptToSend,
    MessageAttemptToCheck,
    PushTicket,
    encode_data_for_failure_job,
)
import lib.push.ticket_stats
from redis_helpers.lmove_many import lmove_many_safe
from dataclasses import dataclass
import asyncio

category = JobCategory.LOW_RESOURCE_COST


JOB_BATCH_SIZE = 1000
"""The maximum number of tokens we attempt in a single job run"""

REDIS_MOVE_BATCH_SIZE = 100
"""How many items we try to lmove from one redis list to another at a time"""

NETWORK_BATCH_SIZE = 100
"""How many tokens we send to the Expo Push API at a time"""

REDIS_FETCH_BATCH_SIZE = 10
"""How many tokens we request from the redis api at a time via lrange"""

MAX_CONCURRENT_REQUESTS = 7
"""The maximum number of concurrent requests to the Expo Push API at once"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls off batches from the To Send queue, moves them to purgatory, then
    converts them to push tickets via the Expo Push API. Where this is located
    in the push flow is described in moderate detail in the admin dashboard under
    "Notifications"

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(itgs, b"push:send_job:lock", gd=gd, spin=False):
        redis = await itgs.redis()
        await redis.hset(  # type: ignore
            b"stats:push_tickets:send_job",  # type: ignore
            b"last_started_at",  # type: ignore
            str(started_at).encode("ascii"),  # type: ignore
        )

        num_in_purgatory = await redis.llen(b"push:message_attempts:purgatory")  # type: ignore
        if num_in_purgatory > 0:
            num_to_handle = num_in_purgatory
            slack = await itgs.slack()
            await slack.send_web_error_message(
                f"**Push Send Job** - recovering {num_in_purgatory=} message attempts from purgatory",
                "Push Send Job - recovering from purgatory",
            )
        else:
            num_remaining_to_move = JOB_BATCH_SIZE
            num_to_handle = 0
            while num_remaining_to_move > 0:
                max_to_move_this_batch = min(
                    num_remaining_to_move, REDIS_MOVE_BATCH_SIZE
                )
                num_moved_this_batch = await lmove_many_safe(
                    itgs,
                    b"push:message_attempts:to_send",
                    b"push:message_attempts:purgatory",
                    max_to_move_this_batch,
                )
                num_remaining_to_move -= num_moved_this_batch
                num_to_handle += num_moved_this_batch
                if num_moved_this_batch < max_to_move_this_batch:
                    break

        if num_to_handle == 0:
            logging.info("Push Send Job - no message attempts to handle")
            finished_at = time.time()
            await redis.hset(  # type: ignore
                b"stats:push_tickets:send_job",  # type: ignore
                mapping={
                    b"last_finished_at": finished_at,
                    b"last_running_time": finished_at - started_at,
                    b"last_num_messages_attempted": 0,
                    b"last_num_succeeded": 0,
                    b"last_num_failed_permanently": 0,
                    b"last_num_failed_transiently": 0,
                },
            )
            return

        logging.debug(f"Push Send Job - handling {num_to_handle=} message attempts")

        push_api_key = os.environ["OSEH_EXPO_NOTIFICATION_ACCESS_TOKEN"]
        client = Session(
            "exp.host",
            default_headers={
                "user-agent": "oseh via tjstretchalot/h2client (+https://www.oseh.com)",
                "accept": "application/json",
                "accept-encoding": "gzip, deflate",
                "authorization": f"bearer {push_api_key}",
            },
        )

        sending_jobs: List[
            Tuple[List[MessageAttemptToSend], asyncio.Task[ResponseData]]
        ] = []
        num_succeeded = 0
        num_failed_permanently = 0
        num_failed_transiently = 0

        async def sweep_sending_jobs():
            nonlocal sending_jobs
            nonlocal num_succeeded
            nonlocal num_failed_permanently
            nonlocal num_failed_transiently

            finished_jobs = []
            new_sending_jobs = []
            for job in sending_jobs:
                if job[1].done():
                    finished_jobs.append(job)
                else:
                    new_sending_jobs.append(job)
            sending_jobs = new_sending_jobs
            for job in finished_jobs:
                try:
                    response_data = await job[1]
                except Exception as e:
                    logging.error(
                        f"Push Send Job - network error sending push notification: {e}"
                    )
                    await fail_all_attempts(
                        itgs,
                        job[0],
                        MessageAttemptFailureInfo(
                            action="send",
                            ticket=None,
                            receipt=None,
                            identifier="NetworkError",
                            retryable=True,
                            extra=str(e),
                        ),
                    )
                    num_failed_transiently += len(job[0])
                    continue

                result = await handle_send_job_result(itgs, job[0], response_data)
                num_succeeded += result.succeeded
                num_failed_permanently += result.failed_permanently
                num_failed_transiently += result.failed_transiently

        async with client.conn() as conn:
            redis_idx = 0
            remaining = num_to_handle
            while remaining > 0:
                await sweep_sending_jobs()
                while len(sending_jobs) >= MAX_CONCURRENT_REQUESTS:
                    await asyncio.wait(
                        [task for _, task in sending_jobs],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    await sweep_sending_jobs()

                remaining_for_this_network_batch = min(remaining, NETWORK_BATCH_SIZE)
                network_batch: List[MessageAttemptToSend] = []

                while remaining_for_this_network_batch > 0:
                    redis_batch_size = min(
                        remaining_for_this_network_batch, REDIS_FETCH_BATCH_SIZE
                    )
                    redis_batch = await redis.lrange(  # type: ignore
                        b"push:message_attempts:purgatory",  # type: ignore
                        redis_idx,
                        redis_idx + redis_batch_size - 1,
                    )
                    assert (
                        len(redis_batch) == redis_batch_size
                    ), f"expected {redis_batch_size=} but got {len(redis_batch)=}"
                    redis_idx += redis_batch_size
                    network_batch.extend(
                        [
                            MessageAttemptToSend.model_validate_json(attempt)
                            for attempt in redis_batch
                        ]
                    )
                    remaining_for_this_network_batch -= redis_batch_size

                sending_jobs.append(
                    (
                        network_batch,
                        asyncio.create_task(
                            conn.post(
                                "/--/api/v2/push/send",
                                data=gzip.compress(
                                    json.dumps(
                                        [
                                            {
                                                "to": attempt.push_token,
                                                "title": attempt.contents.title,
                                                "body": attempt.contents.body,
                                                "channelId": attempt.contents.channel_id,
                                            }
                                            for attempt in network_batch
                                        ]
                                    ).encode("utf-8"),
                                    compresslevel=9,
                                    mtime=0,
                                ),
                                headers={
                                    "content-type": "application/json; charset=utf-8",
                                    "content-encoding": "gzip",
                                },
                            )
                        ),
                    )
                )
                remaining -= len(network_batch)

            while len(sending_jobs) > 0:
                await asyncio.wait(
                    [task for _, task in sending_jobs],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                await sweep_sending_jobs()

        await redis.delete(b"push:message_attempts:purgatory")
        finished_at = time.time()
        await redis.hset(
            b"stats:push_tickets:send_job",  # type: ignore
            mapping={
                b"last_finished_at": finished_at,
                b"last_running_time": finished_at - started_at,
                b"last_num_messages_attempted": num_to_handle,
                b"last_num_succeeded": num_succeeded,
                b"last_num_failed_permanently": num_failed_permanently,
                b"last_num_failed_transiently": num_failed_transiently,
            },
        )
        logging.info(
            f"Push Send Job - finished sending {num_to_handle} message attempts in {finished_at - started_at} seconds:\n"
            f"  {num_succeeded} succeeded\n"
            f"  {num_failed_permanently} failed permanently\n"
            f"  {num_failed_transiently} failed transiently"
        )


@dataclass
class SendJobProcessResult:
    attempted: int
    succeeded: int
    failed_permanently: int
    failed_transiently: int


async def handle_send_job_result(
    itgs: Itgs, sent: List[MessageAttemptToSend], response: ResponseData
) -> SendJobProcessResult:
    logging.info(
        f"Received response {response.status_code} {response.status_text} to "
        f"{len(sent)} message attempts from Expo Push API..."
    )
    if response.status_code == 429:
        logging.warning(
            f"Expo Push API returned a hard 429 to {len(sent)} message attempts, queueing failure jobs..."
        )

        failure_info = MessageAttemptFailureInfo(
            action="send",
            ticket=None,
            receipt=None,
            identifier="ClientError429",
            retryable=True,
            extra="hard",
        )
        await fail_all_attempts(itgs, sent, failure_info)
        return SendJobProcessResult(
            attempted=len(sent),
            succeeded=0,
            failed_permanently=0,
            failed_transiently=len(sent),
        )

    if response.status_code >= 300 and response.status_code <= 499:
        logging.warning(
            f"Expo Push API returned unexpected client-error status code for {len(sent)} message attempts: {response.status_code} {response.text}"
        )
        failure_info = MessageAttemptFailureInfo(
            action="send",
            ticket=None,
            receipt=None,
            identifier="ClientErrorOther",
            retryable=False,
            extra=str(response.status_code),
        )
        await fail_all_attempts(itgs, sent, failure_info)
        return SendJobProcessResult(
            attempted=len(sent),
            succeeded=0,
            failed_permanently=len(sent),
            failed_transiently=0,
        )

    if response.status_code >= 500:
        logging.warning(
            f"Expo Push API returned unexpected server-error status code for {len(sent)} message attempts: {response.status_code} {response.text}"
        )
        failure_info = MessageAttemptFailureInfo(
            action="send",
            ticket=None,
            receipt=None,
            identifier="ServerError",
            retryable=True,
            extra=str(response.status_code),
        )
        await fail_all_attempts(itgs, sent, failure_info)
        return SendJobProcessResult(
            attempted=len(sent),
            succeeded=0,
            failed_permanently=0,
            failed_transiently=len(sent),
        )

    try:
        response_raw = response.content
        if "gzip" in response.headers.get("content-encoding", ""):
            response_raw = gzip.decompress(response_raw)
        response_json = json.loads(response_raw)
    except Exception as e:
        logging.warning(
            f"Expo Push API returned non-JSON response for {len(sent)} message attempts: {e} (treating as InternalError) ({response.headers=})"
        )
        failure_info = MessageAttemptFailureInfo(
            action="send",
            ticket=None,
            receipt=None,
            identifier="InternalError",
            retryable=False,
            extra=f"Didn't receive valid json: {e}",
        )
        await fail_all_attempts(itgs, sent, failure_info)
        return SendJobProcessResult(
            attempted=len(sent),
            succeeded=0,
            failed_permanently=len(sent),
            failed_transiently=0,
        )

    if (
        not isinstance(response_json, dict)
        or not isinstance(response_json.get("data"), list)
        or len(response_json["data"]) != len(sent)
    ):
        logging.warning(
            f"Expo Push API returned unexpected JSON response for {len(sent)} message attempts: {response_json} (treating as InternalError)"
        )
        failure_info = MessageAttemptFailureInfo(
            action="send",
            ticket=None,
            receipt=None,
            identifier="InternalError",
            retryable=False,
            extra="Valid json but invalid structure received",
        )
        await fail_all_attempts(itgs, sent, failure_info)
        return SendJobProcessResult(
            attempted=len(sent),
            succeeded=0,
            failed_permanently=len(sent),
            failed_transiently=0,
        )

    if response_json.get("errors") is not None and (
        not isinstance(response_json["errors"], list)
        or len(response_json["errors"]) > 0
    ):
        logging.warning(
            f"Expo Push API returned unexpected errors for {len(sent)} message attempts: {response_json['errors']} (treating as InternalError)"
        )
        failure_info = MessageAttemptFailureInfo(
            action="send",
            ticket=None,
            receipt=None,
            identifier="InternalError",
            retryable=False,
            extra=f"Errors received: {json.dumps(response_json['errors'])}",
        )
        await fail_all_attempts(itgs, sent, failure_info)
        return SendJobProcessResult(
            attempted=len(sent),
            succeeded=0,
            failed_permanently=len(sent),
            failed_transiently=0,
        )

    num_succeeded = 0
    num_permanent_failures = 0
    num_error_by_type: Dict[str, int] = {}

    for attempt, result in zip(sent, response_json["data"]):
        if not isinstance(result, dict):
            logging.warning(
                f"Expo Push API gave bad response (not dict) for {attempt.uid=}: {result}"
            )
            num_permanent_failures += 1
            num_error_by_type["InternalError"] = (
                num_error_by_type.get("InternalError", 0) + 1
            )
            await fail_job(
                itgs,
                attempt,
                MessageAttemptFailureInfo(
                    action="send",
                    ticket=None,
                    receipt=None,
                    identifier="InternalError",
                    retryable=False,
                    extra=f"not dict: {result}",
                ),
            )
            continue

        if result.get("status") not in ("ok", "error"):
            logging.warning(
                f"Expo Push API gave bad response (no status) for {attempt.uid=}: {result}"
            )
            num_permanent_failures += 1
            num_error_by_type["InternalError"] = (
                num_error_by_type.get("InternalError", 0) + 1
            )
            await fail_job(
                itgs,
                attempt,
                MessageAttemptFailureInfo(
                    action="send",
                    ticket=None,
                    receipt=None,
                    identifier="InternalError",
                    retryable=False,
                    extra=f"missing status in response: {result}",
                ),
            )
            continue

        if result["status"] == "ok":
            if not isinstance(result.get("id"), str):
                logging.warning(
                    f"Expo Push API gave bad response (no id) for {attempt.uid=}: {result}"
                )
                num_permanent_failures += 1
                num_error_by_type["InternalError"] = (
                    num_error_by_type.get("InternalError", 0) + 1
                )
                await fail_job(
                    itgs,
                    attempt,
                    MessageAttemptFailureInfo(
                        action="send",
                        ticket=None,
                        receipt=None,
                        identifier="InternalError",
                        retryable=False,
                        extra=f"missing id in response: {result}",
                    ),
                )
                continue

            num_succeeded += 1
            ticket_id: str = result["id"]
            await handle_ticket_created_successfully(itgs, attempt, ticket_id)
            continue

        if result.get("message") is not None and not isinstance(result["message"], str):
            logging.warning(
                f"Expo Push API gave bad response (message not str) for {attempt.uid=}: {result}"
            )
            num_permanent_failures += 1
            num_error_by_type["InternalError"] = (
                num_error_by_type.get("InternalError", 0) + 1
            )
            await fail_job(
                itgs,
                attempt,
                MessageAttemptFailureInfo(
                    action="send",
                    ticket=None,
                    receipt=None,
                    identifier="InternalError",
                    retryable=False,
                    extra=f"message not str in response: {result}",
                ),
            )
            continue

        # we're not given many promises about details from a plain reading of the docs,
        # so we will only take it optimistically
        details = result.get("details")
        if details is not None and not isinstance(details, (str, int, float, dict)):
            details = None

        expo_error_id: Optional[str] = None
        if (
            details is not None
            and isinstance(details, dict)
            and isinstance(details.get("error"), str)
        ):
            expo_error_id = details["error"]

        if expo_error_id == "DeviceNotRegistered":
            logging.warning(
                f"Expo Push API gave DeviceNotRegistered for {attempt.uid=}: {result}"
            )
            num_permanent_failures += 1
            num_error_by_type["DeviceNotRegistered"] = (
                num_error_by_type.get("DeviceNotRegistered", 0) + 1
            )
            await fail_job(
                itgs,
                attempt,
                MessageAttemptFailureInfo(
                    action="send",
                    ticket=PushTicket(
                        status="error",
                        id=None,
                        message=result.get("message"),
                        details={
                            "error": expo_error_id,
                        },
                    ),
                    receipt=None,
                    identifier="DeviceNotRegistered",
                    retryable=False,
                    extra=None,
                ),
            )
            continue

        logging.warning(
            f"Ignoring unknown {expo_error_id=}, treating as internal server error"
        )
        num_permanent_failures += 1
        num_error_by_type["InternalError"] = (
            num_error_by_type.get("InternalError", 0) + 1
        )
        await fail_job(
            itgs,
            attempt,
            MessageAttemptFailureInfo(
                action="send",
                ticket=PushTicket(
                    status="error",
                    id=None,
                    message=result.get("message"),
                    details={
                        "error": expo_error_id,
                    },
                ),
                receipt=None,
                identifier="InternalError",
                retryable=False,
                extra=None,
            ),
        )

    logging.info(
        f"Processed {len(sent)} responses from Expo Push API: {num_succeeded=}, {num_permanent_failures=}, {num_error_by_type=}"
    )
    return SendJobProcessResult(
        attempted=len(sent),
        succeeded=num_succeeded,
        failed_permanently=num_permanent_failures,
        failed_transiently=0,
    )


EVENTS_FOR_FAILURE_IDENTIFIER: Dict[
    MessageAttemptFailureInfoIdentifier, lib.push.ticket_stats.PushTicketStatsEvent
] = {
    "DeviceNotRegistered": "failed_due_to_device_not_registered",
    "ClientError429": "failed_due_to_client_error_429",
    "ClientErrorOther": "failed_due_to_client_error_other",
    "ServerError": "failed_due_to_server_error",
    "InternalError": "failed_due_to_internal_error",
    "NetworkError": "failed_due_to_network_error",
}


async def fail_all_attempts(
    itgs: Itgs,
    attempts: List[MessageAttemptToSend],
    failure_info: MessageAttemptFailureInfo,
):
    event = EVENTS_FOR_FAILURE_IDENTIFIER[failure_info.identifier]
    jobs = await itgs.jobs()
    for attempt in attempts:
        await jobs.enqueue(
            attempt.failure_job.name,
            data_raw=encode_data_for_failure_job(attempt, failure_info),
            **attempt.failure_job.kwargs,
        )
        await lib.push.ticket_stats.increment_event(
            itgs, event=event, now=attempt.initially_queued_at
        )


async def fail_job(
    itgs: Itgs, attempt: MessageAttemptToSend, failure_info: MessageAttemptFailureInfo
):
    event = EVENTS_FOR_FAILURE_IDENTIFIER[failure_info.identifier]
    jobs = await itgs.jobs()
    await jobs.enqueue(
        attempt.failure_job.name,
        data_raw=encode_data_for_failure_job(attempt, failure_info),
        **attempt.failure_job.kwargs,
    )
    await lib.push.ticket_stats.increment_event(
        itgs, event=event, now=attempt.initially_queued_at
    )


async def handle_ticket_created_successfully(
    itgs: Itgs, attempt: MessageAttemptToSend, ticket_id: str
):
    now = time.time()
    redis = await itgs.redis()
    await redis.zadd(
        b"push:push_tickets:cold",
        dict(
            [
                (
                    MessageAttemptToCheck(
                        aud="check",
                        uid=attempt.uid,
                        attempt_initially_queued_at=attempt.initially_queued_at,
                        initially_queued_at=now,
                        retry=0,
                        last_queued_at=now,
                        push_ticket=PushTicket(
                            status="ok", id=ticket_id, message=None, details=None
                        ),
                        push_ticket_created_at=now,
                        push_token=attempt.push_token,
                        contents=attempt.contents,
                        failure_job=attempt.failure_job,
                        success_job=attempt.success_job,
                    )
                    .model_dump_json()
                    .encode("utf-8"),
                    now,
                ),
            ]
        ),
    )
    await lib.push.ticket_stats.increment_event(
        itgs, event="succeeded", now=attempt.initially_queued_at
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.push.send")

    asyncio.run(main())
