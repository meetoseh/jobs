"""SMS Receipt Recovery Job"""
import base64
import os
import time
from typing import Literal, Optional, Union
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from lib.sms.events import MessageResourceEvent, push_message_resource_event
from redis_helpers.lmove_using_purgatory import lmove_using_purgatory_safe
import lib.sms.poll_stats as poll_stats
import asyncio
import aiohttp
import email.utils

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""After this time the job stops to avoid blocking other jobs"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls items from the Recovery Queue one at a time and fetches the current
    status of the message from Twilio. Then sends the updated information to the
    right side of the Event Queue, including `lost`, which means Twilio doesn't have
    that message resource anymore

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"sms:receipt_recovery_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(  # type: ignore
            b"stats:sms:receipt_recovery_job",  # type: ignore
            b"started_at",  # type: ignore
            str(started_at).encode("ascii"),  # type: ignore
        )

        # We fast-path the case where there is no work to do to avoid
        # initializing the HTTP client
        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.llen(b"sms:recovery")  # type: ignore
            await pipe.llen(b"sms:recovery_purgatory")  # type: ignore
            result = await pipe.execute()

        if result[0] == 0 and result[1] == 0:
            finished_at = time.time()
            logging.info(f"SMS Receipt Recovery Job: No work to do")
            await redis.hset(
                b"stats:sms:receipt_recovery_job",  # type: ignore
                mapping={
                    b"finished_at": finished_at,
                    b"running_time": finished_at - started_at,
                    b"stop_reason": b"list_exhausted",
                    b"attempted": 0,
                    b"pending": 0,
                    b"succeeded": 0,
                    b"failed": 0,
                    b"lost": 0,
                    b"permanent_error": 0,
                    b"transient_error": 0,
                },
            )
            return

        twilio_account_sid = os.environ["OSEH_TWILIO_ACCOUNT_SID"]
        twilio_auth_token = os.environ["OSEH_TWILIO_AUTH_TOKEN"]

        client = aiohttp.ClientSession(
            "https://api.twilio.com",
            headers={
                "user-agent": "oseh via python aiohttp (+https://www.oseh.com)",
                "accept": "application/json",
                "accept-encoding": "identity",
                "authorization": f"Basic {encode_basic_auth(twilio_account_sid, twilio_auth_token)}",
            },
        )

        attempted: int = 0
        pending: int = 0
        succeeded: int = 0
        failed: int = 0
        lost: int = 0
        permanent_error: int = 0
        transient_error: int = 0
        stop_reason: Optional[
            Literal["list_exhausted", "time_exhausted", "signal"]
        ] = None

        num_failures = 0
        last_requires_sleep = False

        async with client as conn:
            while True:
                if gd.received_term_signal:
                    logging.info(f"SMS Receipt Recovery Job: Received Term Signal")
                    stop_reason = "signal"
                    break

                sleep_time = 0
                if last_requires_sleep:
                    num_failures += 1
                    sleep_time = 2 ** min(num_failures, 5)
                    last_requires_sleep = False

                if time.time() - started_at + sleep_time > MAX_JOB_TIME_SECONDS:
                    logging.info(f"SMS Receipt Recovery Job: Time Exhausted")
                    stop_reason = "time_exhausted"
                    break

                if sleep_time > 0:
                    logging.debug(
                        f"SMS Receipt Recovery Job: Sleeping {sleep_time}s due to failure"
                    )
                    await asyncio.sleep(sleep_time)

                sid_raw = await lmove_using_purgatory_safe(
                    itgs, b"sms:recovery", b"sms:recovery_purgatory"
                )
                if sid_raw is None:
                    stop_reason = "list_exhausted"
                    break

                attempted += 1

                try:
                    sid = (
                        str(sid_raw, "utf-8")
                        if isinstance(sid_raw, (bytes, bytearray, memoryview))
                        else sid_raw
                    )
                except:
                    logging.exception(f"Failed to decode {sid_raw=}")
                    await poll_stats.increment_event(
                        itgs, event="error_internal", now=time.time()
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    permanent_error += 1
                    continue

                try:
                    async with basic_redis_lock(itgs, b"twilio:lock", gd=gd, spin=True):
                        response = await conn.get(
                            f"/2010-04-01/Accounts/{twilio_account_sid}/Messages/{sid}.json"
                        )
                except:
                    logging.warning(
                        "SMS Receipt Recovery Job - failed to connect to Twilio",
                        exc_info=True,
                    )
                    await poll_stats.increment_event(
                        itgs, event="error_network", now=time.time()
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    transient_error += 1
                    last_requires_sleep = True
                    continue

                response_received_at = time.time()

                if response.status == 404:
                    logging.info(f"SMS Receipt Recovery Job: Message {sid} is lost")
                    await push_message_resource_event(
                        itgs,
                        MessageResourceEvent(
                            sid=sid,
                            status="lost",
                            date_updated=None,
                            information_received_at=response_received_at,
                            received_via="poll",
                            error_code=None,
                            error_message=None,
                        ),
                    )
                    await poll_stats.increment_event(
                        itgs, event="error_client_404", now=response_received_at
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    lost += 1
                    continue

                if response.status == 429:
                    logging.info(f"SMS Receipt Recovery Job: Rate Limited")
                    await poll_stats.increment_event(
                        itgs, event="error_client_429", now=response_received_at
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    transient_error += 1
                    last_requires_sleep = True
                    continue

                if (
                    response.status <= 199
                    or response.status >= 300
                    and response.status <= 499
                ):
                    logging.info(
                        f"SMS Receipt Recovery Job: Client Error ({response.status})"
                    )
                    await poll_stats.increment_event(
                        itgs,
                        event="error_client_other",
                        now=response_received_at,
                        extra={
                            "http_status_code": response.status,
                        },
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    permanent_error += 1
                    continue

                if response.status >= 500:
                    logging.info(
                        f"SMS Receipt Recovery Job: Server Error ({response.status})"
                    )
                    await poll_stats.increment_event(
                        itgs,
                        event="error_server",
                        now=response_received_at,
                        extra={
                            "http_status_code": response.status,
                        },
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    transient_error += 1
                    last_requires_sleep = True
                    continue

                try:
                    message_resource_obj = await response.json()
                except:
                    logging.exception(
                        f"Failed to decode response as json for {response.status=}"
                    )
                    await poll_stats.increment_event(
                        itgs, event="error_internal", now=response_received_at
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    permanent_error += 1
                    continue

                try:
                    assert isinstance(message_resource_obj, dict)
                    date_updated_str: str = message_resource_obj["date_updated"]
                    assert isinstance(date_updated_str, str)
                    response_parsed = MessageResourceEvent(
                        sid=message_resource_obj["sid"],
                        status=message_resource_obj["status"],
                        error_code=str(message_resource_obj["error_code"])
                        if message_resource_obj.get("error_code") is not None
                        else None,
                        error_message=str(message_resource_obj["error_message"])
                        if message_resource_obj.get("error_message") is not None
                        else None,
                        date_updated=email.utils.parsedate_to_datetime(
                            date_updated_str
                        ).timestamp(),
                        information_received_at=response_received_at,
                        received_via="poll",
                    )
                except:
                    logging.exception(
                        f"Failed to parse response as MessageResourceEvent for {response.status=}"
                    )
                    await poll_stats.increment_event(
                        itgs, event="error_internal", now=response_received_at
                    )
                    await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                    permanent_error += 1
                    continue

                old_status: Optional[Union[str, bytes]] = await redis.hget(  # type: ignore
                    f"sms:pending:{sid}".encode("utf-8"), b"message_resource_status"  # type: ignore
                )
                if isinstance(old_status, bytes):
                    old_status = old_status.decode("utf-8")
                if old_status is None:
                    old_status = "(unknown)"

                logging.debug(
                    f"SMS Receipt Recovery Job: Message {sid} is in status {repr(response_parsed.status)}; was in status {repr(old_status)}"
                )

                await push_message_resource_event(itgs, response_parsed)
                await poll_stats.increment_event(
                    itgs,
                    event="received",
                    now=response_received_at,
                    extra={
                        "old_status": old_status,
                        "new_status": response_parsed.status,
                    },
                )
                await redis.lpop(b"sms:recovery_purgatory")  # type: ignore
                if response_parsed.status in ("delivered", "sent", "read"):
                    succeeded += 1
                elif response_parsed.status in ("canceled", "undelivered", "failed"):
                    failed += 1
                else:
                    pending += 1

        finished_at = time.time()
        logging.info(
            f"Receipt Recovery Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Attempted: {attempted}\n"
            f"- Pending: {pending}\n"
            f"- Succeeded: {succeeded}\n"
            f"- Failed: {failed}\n"
            f"- Lost: {lost}\n"
            f"- Permanent Error: {permanent_error}\n"
            f"- Transient Error: {transient_error}"
        )
        await redis.hset(
            b"stats:sms:receipt_recovery_job",  # type: ignore
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"stop_reason": stop_reason.encode("utf-8"),
                b"attempted": attempted,
                b"pending": pending,
                b"succeeded": succeeded,
                b"failed": failed,
                b"lost": lost,
                b"permanent_error": permanent_error,
                b"transient_error": transient_error,
            },
        )


def encode_basic_auth(username: str, password: str) -> str:
    return base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sms.receipt_recovery")

    asyncio.run(main())
