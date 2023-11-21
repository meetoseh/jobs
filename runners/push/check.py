"""Push Receipt Check Job"""
import gzip
import json
import os
from typing import Any, Dict, List, Literal, Optional, Tuple, cast
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from h2client.session import Session
from h2client.simple_connection import ResponseData
import time
from dataclasses import dataclass
from lib.basic_redis_lock import basic_redis_lock
from lib.push.message_attempt_info import (
    MessageAttemptFailureInfo,
    MessageAttemptFailureInfoIdentifier,
    MessageAttemptSuccess,
    MessageAttemptSuccessResult,
    MessageAttemptToCheck,
    PushReceipt,
    encode_data_for_failure_job,
    encode_data_for_success_job,
)
import lib.push.receipt_stats
import asyncio

from redis_helpers.lmove_many import lmove_many_safe

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
    """Pulls batches from the Push Receipt Hot Set, moves them to Push Receipt Purgatory,
    and queries the Expo push notification service for the receipts corresponding to each
    ticket, then calls the indicated callbacks (which may retry for retryable failures).

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(itgs, b"push:check_job:lock", gd=gd, spin=False):
        redis = await itgs.redis()
        await redis.hset(  # type: ignore
            b"stats:push_receipts:check_job",  # type: ignore
            b"last_started_at",  # type: ignore
            started_at,  # type: ignore
        )

        num_to_check = await prepare_purgatory(itgs)
        if num_to_check == 0:
            logging.info("Push Receipt Check Job found nothing to do, exiting...")
            finished_at = time.time()
            await redis.hset(  # type: ignore
                b"stats:push_receipts:check_job",  # type: ignore
                mapping={
                    b"last_finished_at": finished_at,
                    b"last_running_time": finished_at - started_at,
                    b"last_num_checked": 0,
                    b"last_num_succeeded": 0,
                    b"last_num_failed_permanently": 0,
                    b"last_num_failed_transiently": 0,
                },
            )
            return

        check_job_result = SendJobResult(0, 0, 0, 0)
        receipt_send_jobs: List[
            Tuple[List[MessageAttemptToCheck], asyncio.Task[ResponseData]]
        ] = []

        async def sweep_receipt_send_jobs():
            nonlocal receipt_send_jobs, check_job_result

            pending: List[
                Tuple[List[MessageAttemptToCheck], asyncio.Task[ResponseData]]
            ] = []
            for attempts, task in receipt_send_jobs:
                if not task.done():
                    pending.append((attempts, task))
                    continue
                try:
                    response_data = await task
                except Exception as e:
                    logging.warning(
                        f"Network error while sending {len(attempts)} push tickets to Expo Push API",
                        exc_info=e,
                    )
                    await fail_entire_batch(
                        itgs, attempts, "NetworkError", True, str(e)
                    )
                    continue

                result = await handle_send_job_result(itgs, attempts, response_data)
                check_job_result.add_result(result)

            receipt_send_jobs = pending

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

        remaining_to_check = num_to_check
        redis_idx = 0
        async with client.conn() as conn:
            while remaining_to_check > 0:
                await sweep_receipt_send_jobs()
                while len(receipt_send_jobs) >= MAX_CONCURRENT_REQUESTS:
                    await asyncio.wait(
                        [task for _, task in receipt_send_jobs],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    await sweep_receipt_send_jobs()

                remaining_for_this_network_batch = min(
                    remaining_to_check, NETWORK_BATCH_SIZE
                )
                network_batch: List[MessageAttemptToCheck] = []

                while remaining_for_this_network_batch > 0:
                    redis_batch_size = min(
                        remaining_for_this_network_batch, REDIS_FETCH_BATCH_SIZE
                    )
                    redis_batch = await redis.lrange(  # type: ignore
                        b"push:push_tickets:purgatory",  # type: ignore
                        redis_idx,
                        redis_idx + redis_batch_size - 1,
                    )
                    assert (
                        len(redis_batch) == redis_batch_size
                    ), f"expected {redis_batch_size=} but got {len(redis_batch)=}"
                    redis_idx += redis_batch_size
                    network_batch.extend(
                        [
                            MessageAttemptToCheck.model_validate_json(attempt)
                            for attempt in redis_batch
                        ]
                    )
                    remaining_for_this_network_batch -= redis_batch_size

                receipt_send_jobs.append(
                    (
                        network_batch,
                        asyncio.create_task(
                            conn.post(
                                "/--/api/v2/push/getReceipts",
                                data=gzip.compress(
                                    json.dumps(
                                        {
                                            "ids": [
                                                attempt.push_ticket.id
                                                for attempt in network_batch
                                            ]
                                        }
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

                remaining_to_check -= len(network_batch)

            while len(receipt_send_jobs) > 0:
                await asyncio.wait(
                    [task for _, task in receipt_send_jobs],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                await sweep_receipt_send_jobs()

        await redis.delete(b"push:push_tickets:purgatory")
        finished_at = time.time()
        await redis.hset(  # type: ignore
            b"stats:push_receipts:check_job",  # type: ignore
            mapping={
                b"last_finished_at": finished_at,
                b"last_running_time": finished_at - started_at,
                b"last_num_checked": check_job_result.checked,
                b"last_num_succeeded": check_job_result.succeeded,
                b"last_num_failed_permanently": check_job_result.failed_permanently,
                b"last_num_failed_transiently": check_job_result.failed_transiently,
            },
        )
        logging.info(
            f"Push Receipt Check Job finished checking {check_job_result.checked} push tickets, "
            f"in {finished_at - started_at:.2f} seconds:\n"
            f"  {check_job_result.succeeded} succeeded\n"
            f"  {check_job_result.failed_permanently} failed permanently\n"
            f"  {check_job_result.failed_transiently} failed transiently"
        )


async def prepare_purgatory(itgs: Itgs):
    hot_key = b"push:push_tickets:hot"
    purgatory_key = b"push:push_tickets:purgatory"
    redis = await itgs.redis()

    purgatory_len = await redis.llen(purgatory_key)  # type: ignore
    if purgatory_len > 0:
        msg = (
            f"Push Receipt Check Job found {purgatory_len} items in purgatory, "
            "these must have come from a failed previous run. Recovering by "
            "working on those items this run."
        )
        logging.warning(msg)
        slack = await itgs.slack()
        await slack.send_web_error_message(
            msg, preview="Push Receipt Check Job recovering from purgatory"
        )
        return purgatory_len

    remaining_to_move = JOB_BATCH_SIZE
    while remaining_to_move > 0:
        num_to_move = min(remaining_to_move, REDIS_MOVE_BATCH_SIZE)
        num_moved = await lmove_many_safe(itgs, hot_key, purgatory_key, num_to_move)  # type: ignore

        purgatory_len += num_moved
        remaining_to_move -= num_moved

        if num_moved < num_to_move:
            break

    return purgatory_len


@dataclass
class SendJobResult:
    checked: int
    succeeded: int
    failed_permanently: int
    failed_transiently: int

    def add_result(self, result: "SendJobResult") -> None:
        self.checked += result.checked
        self.succeeded += result.succeeded
        self.failed_permanently += result.failed_permanently
        self.failed_transiently += result.failed_transiently


async def handle_send_job_result(
    itgs: Itgs,
    attempts: List[MessageAttemptToCheck],
    response: ResponseData,
) -> SendJobResult:
    request_finished_at = time.time()
    logging.info(
        f"Received response {response.status_code} {response.status_text} to "
        f"{len(attempts)} push tickets from Expo Push API..."
    )
    if response.status_code == 429:
        logging.warning(
            f"Expo Push API returned a hard 429 to {len(attempts)} message attempts, queueing failure jobs..."
        )
        await fail_entire_batch(itgs, attempts, "ClientError429", True, None)
        return SendJobResult(
            checked=len(attempts),
            succeeded=0,
            failed_permanently=0,
            failed_transiently=len(attempts),
        )

    if response.status_code >= 300 and response.status_code <= 499:
        logging.warning(
            f"Expo Push API returned unexpected client-error status code for {len(attempts)} message attempts: {response.status_code} {response.text}"
        )
        await fail_entire_batch(
            itgs,
            attempts,
            "ClientErrorOther",
            False,
            f"{response.status_code} {response.status_text}",
        )
        return SendJobResult(
            checked=len(attempts),
            succeeded=0,
            failed_permanently=len(attempts),
            failed_transiently=0,
        )

    if response.status_code >= 500:
        logging.warning(
            f"Expo Push API returned unexpected server-error status code for {len(attempts)} message attempts: {response.status_code} {response.text}"
        )
        await fail_entire_batch(
            itgs,
            attempts,
            "ServerError",
            True,
            f"{response.status_code} {response.status_text}",
        )
        return SendJobResult(
            checked=len(attempts),
            succeeded=0,
            failed_permanently=0,
            failed_transiently=len(attempts),
        )

    try:
        response_raw = response.content
        if "gzip" in response.headers.get("content-encoding", ""):
            response_raw = gzip.decompress(response_raw)
        response_json = json.loads(response_raw)
    except Exception as e:
        logging.warning(
            f"Expo Push API returned non-JSON response for {len(attempts)} message attempts: {e} (treating as InternalError) ({response.headers=})"
        )
        await fail_entire_batch(
            itgs,
            attempts,
            "InternalError",
            False,
            f"non-JSON response: {e}",
        )
        return SendJobResult(
            checked=len(attempts),
            succeeded=0,
            failed_permanently=len(attempts),
            failed_transiently=0,
        )

    if not isinstance(response_json, dict):
        logging.warning(
            f"Expo Push API did not return a dict: {response_json=} (treating as InternalError)"
        )
        await fail_entire_batch(
            itgs,
            attempts,
            "InternalError",
            False,
            "non-dict json response",
        )
        return SendJobResult(
            checked=len(attempts),
            succeeded=0,
            failed_permanently=len(attempts),
            failed_transiently=0,
        )

    if response_json.get("errors") is not None and (
        not isinstance(response_json["errors"], list)
        or len(response_json["errors"]) > 0
    ):
        logging.warning(
            f"Expo Push API returned unexpected errors for {len(attempts)} message attempts: {response_json['errors']} (treating as InternalError)"
        )
        await fail_entire_batch(
            itgs,
            attempts,
            "InternalError",
            False,
            f"errors included in response: {json.dumps(response_json['errors'])}",
        )
        return SendJobResult(
            checked=len(attempts),
            succeeded=0,
            failed_permanently=len(attempts),
            failed_transiently=0,
        )

    data = response_json.get("data")
    if not isinstance(data, dict):
        logging.warning(
            f"Expo Push API did not return a dict for key `data` in response: {response_json=}. Treating as InternalError"
        )
        await fail_entire_batch(
            itgs,
            attempts,
            "InternalError",
            False,
            f"data key in response is not dict, got {type(data)}",
        )
        return SendJobResult(
            checked=len(attempts),
            succeeded=0,
            failed_permanently=len(attempts),
            failed_transiently=0,
        )

    assert all(attempt.push_ticket.id is not None for attempt in attempts), attempts
    remaining_attempts_by_ticket_id: Dict[str, MessageAttemptToCheck] = dict(
        (cast(str, attempt.push_ticket.id), attempt) for attempt in attempts
    )

    result = SendJobResult(
        checked=0, succeeded=0, failed_permanently=0, failed_transiently=0
    )
    for ticket_id, ticket_data in data.items():
        attempt = remaining_attempts_by_ticket_id.pop(ticket_id, None)
        if attempt is None:
            logging.warning(
                f"Expo Push API returned a receipt for ticket {ticket_id=}, which we didn't ask for. Ignoring."
            )
            continue

        result.add_result(
            await handle_attempt_result(itgs, attempt, ticket_data, request_finished_at)
        )

    if len(remaining_attempts_by_ticket_id) > 0:
        not_yet_ready = list(remaining_attempts_by_ticket_id.values())
        await fail_entire_batch(itgs, not_yet_ready, "NotReadyYet", True, None)
        result.checked += len(not_yet_ready)
        result.failed_transiently += len(not_yet_ready)

    return result


async def handle_attempt_result(
    itgs: Itgs,
    attempt: MessageAttemptToCheck,
    result: Any,
    request_finished_at: float,
) -> SendJobResult:
    if not isinstance(result, dict):
        logging.warning(
            f"Push receipt check job get a response for push ticket {attempt.push_ticket}, "
            f"but not a dict: {result=}. Treating as InternalError"
        )
        await fail_attempt(
            itgs,
            attempt,
            MessageAttemptFailureInfo(
                action="check",
                ticket=attempt.push_ticket,
                receipt=None,
                identifier="InternalError",
                retryable=False,
                extra=f"not a dict: {result=}",
            ),
        )
        return SendJobResult(
            checked=1, succeeded=0, failed_permanently=1, failed_transiently=0
        )

    if result.get("status") not in ("ok", "error"):
        logging.warning(
            f"Push receipt check job get a response for push ticket {attempt.push_ticket}, "
            f"but the status is neither ok nor error: {result=}. Treating as InternalError"
        )
        await fail_attempt(
            itgs,
            attempt,
            MessageAttemptFailureInfo(
                action="check",
                ticket=attempt.push_ticket,
                receipt=None,
                identifier="InternalError",
                retryable=False,
                extra=f"not ok or error: {result=}",
            ),
        )
        return SendJobResult(
            checked=1, succeeded=0, failed_permanently=1, failed_transiently=0
        )

    if result["status"] == "ok":
        if len(result) != 1:
            logging.warning(
                f"Push receipt check job got a success response for push ticket {attempt.push_ticket}, "
                f"but it contained unexpected additional data: {result=}. Treating as success"
            )
        await handle_attempt_succeeded(
            itgs,
            attempt,
            receipt=PushReceipt(status="ok", message=None, details=None),
            receipt_checked_at=request_finished_at,
        )
        return SendJobResult(
            checked=1, succeeded=1, failed_permanently=0, failed_transiently=0
        )

    message = result.get("message")
    details = result.get("details")
    error_identifier = None

    if isinstance(details, dict) and isinstance(details.get("error"), str):
        error_identifier = details["error"]

    if error_identifier in (
        "DeviceNotRegistered",
        "MessageTooBig",
        "MessageRateExceeded",
        "MismatchSenderId",
        "InvalidCredentials",
    ):
        await fail_attempt(
            itgs,
            attempt,
            MessageAttemptFailureInfo(
                action="check",
                ticket=attempt.push_ticket,
                receipt=PushReceipt(
                    status="error",
                    message=f"{message}",
                    details=details,
                ),
                identifier=cast(
                    Literal[
                        "DeviceNotRegistered",
                        "MessageTooBig",
                        "MessageRateExceeded",
                        "MismatchSenderId",
                        "InvalidCredentials",
                    ],
                    error_identifier,
                ),
                retryable=False,
                extra=None,
            ),
        )
        return SendJobResult(
            checked=1, succeeded=0, failed_permanently=1, failed_transiently=0
        )

    logging.warning(
        f"Push receipt check job got a failure response for push ticket {attempt.push_ticket}, "
        f"but we were not able to identify the error: {result=}. Treating as InternalError"
    )
    await fail_attempt(
        itgs,
        attempt,
        MessageAttemptFailureInfo(
            action="check",
            ticket=attempt.push_ticket,
            receipt=PushReceipt(
                status="error",
                message=f"{message}",
                details=details,
            ),
            identifier="InternalError",
            retryable=False,
            extra="could not identify an error identifier",
        ),
    )
    return SendJobResult(
        checked=1, succeeded=0, failed_permanently=1, failed_transiently=0
    )


async def handle_missing_send_job_result(
    itgs: Itgs,
    attempt: MessageAttemptToCheck,
) -> None:
    await fail_attempt(
        itgs,
        attempt,
        MessageAttemptFailureInfo(
            action="check",
            ticket=attempt.push_ticket,
            receipt=None,
            identifier="NotReadyYet",
            retryable=True,
            extra=None,
        ),
    )


FAILURE_IDENTIFIER_TO_EVENT: Dict[
    MessageAttemptFailureInfoIdentifier, lib.push.receipt_stats.PushReceiptStatsEvent
] = {
    "DeviceNotRegistered": "failed_due_to_device_not_registered",
    "MessageTooBig": "failed_due_to_message_too_big",
    "MessageRateExceeded": "failed_due_to_message_rate_exceeded",
    "MismatchSenderId": "failed_due_to_mismatched_sender_id",
    "InvalidCredentials": "failed_due_to_invalid_credentials",
    "NotReadyYet": "failed_due_to_not_ready_yet",
    "ClientError429": "failed_due_to_client_error_429",
    "ClientErrorOther": "failed_due_to_client_error_other",
    "ServerError": "failed_due_to_server_error",
    "InternalError": "failed_due_to_internal_error",
    "NetworkError": "failed_due_to_network_error",
}


async def fail_entire_batch(
    itgs: Itgs,
    attempts: List[MessageAttemptToCheck],
    identifier: MessageAttemptFailureInfoIdentifier,
    retryable: bool,
    extra: Optional[str],
) -> None:
    for attempt in attempts:
        await fail_attempt(
            itgs,
            attempt,
            MessageAttemptFailureInfo(
                action="check",
                ticket=attempt.push_ticket,
                receipt=None,
                identifier=identifier,
                retryable=retryable,
                extra=extra,
            ),
        )


async def fail_attempt(
    itgs: Itgs, attempt: MessageAttemptToCheck, failure_info: MessageAttemptFailureInfo
) -> None:
    event_name = FAILURE_IDENTIFIER_TO_EVENT[failure_info.identifier]

    jobs = await itgs.jobs()
    await jobs.enqueue(
        attempt.failure_job.name,
        data_raw=encode_data_for_failure_job(
            attempt,
            failure_info,
        ),
        **attempt.failure_job.kwargs,
    )
    await lib.push.receipt_stats.increment_event(
        itgs, event=event_name, now=attempt.attempt_initially_queued_at
    )


async def handle_attempt_succeeded(
    itgs: Itgs,
    attempt: MessageAttemptToCheck,
    receipt: PushReceipt,
    receipt_checked_at: float,
) -> None:
    data_raw = encode_data_for_success_job(
        MessageAttemptSuccess(
            aud="success",
            uid=attempt.uid,
            initially_queued_at=attempt.attempt_initially_queued_at,
            push_token=attempt.push_token,
            contents=attempt.contents,
        ),
        MessageAttemptSuccessResult(
            ticket_created_at=attempt.push_ticket_created_at,
            ticket=attempt.push_ticket,
            receipt_checked_at=receipt_checked_at,
            receipt=receipt,
        ),
    )

    jobs = await itgs.jobs()
    await jobs.enqueue(
        attempt.success_job.name, data_raw=data_raw, **attempt.success_job.kwargs
    )
    await lib.push.receipt_stats.increment_event(
        itgs, event="succeeded", now=attempt.attempt_initially_queued_at
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.push.check")

    asyncio.run(main())
