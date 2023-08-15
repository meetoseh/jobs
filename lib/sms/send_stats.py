"""Assists with updating statistics related to managing expo push tickets
"""
from typing import Dict, Literal, Optional, Type, Union, overload
from pydantic import BaseModel, Field
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
import redis.asyncio
import unix_dates
import pytz
from itgs import Itgs

timezone = pytz.timezone("America/Los_Angeles")

# TODO: Once on python 3.11, use enum.StrEnum
# Documentation for these events is under `docs/db/stats/sms_send_stats.md`
SMSSendStatsEvent = Literal[
    "queued",
    "retried",
    "succeeded_pending",  # extra: "status"
    "succeeded_immediate",  # extra: "status"
    "abandoned",
    "failed_due_to_application_error_ratelimit",  # extra: "error_code"
    "failed_due_to_application_error_other",  # extra: "error_code"
    "failed_due_to_client_error_429",
    "failed_due_to_client_error_other",  # extra: "http_status_code"
    "failed_due_to_server_error",  # extra: "http_status_code"
    "failed_due_to_internal_error",
    "failed_due_to_network_error",
]


class SmsSendStatsStatusExtra(BaseModel):
    status: str = Field(description="The MessageStatus returned")

    @property
    def redis_key(self) -> bytes:
        return self.status.encode("utf-8")


class SmsSendStatsErrorCodeExtra(BaseModel):
    error_code: str = Field(description="The ErrorCode returned")

    @property
    def redis_key(self) -> bytes:
        return self.error_code.encode("utf-8")


class SmsSendStatsHttpStatusCodeExtra(BaseModel):
    http_status_code: str = Field(description="The HTTP status code returned")

    @property
    def redis_key(self) -> bytes:
        return self.http_status_code.encode("utf-8")


SMS_SEND_STATS_EVENTS: Dict[
    SMSSendStatsEvent,
    Optional[
        Union[
            Type[SmsSendStatsStatusExtra],
            Type[SmsSendStatsErrorCodeExtra],
            Type[SmsSendStatsHttpStatusCodeExtra],
        ]
    ],
] = {
    "queued": None,
    "retried": None,
    "succeeded_pending": SmsSendStatsStatusExtra,
    "succeeded_immediate": SmsSendStatsStatusExtra,
    "abandoned": None,
    "failed_due_to_application_error_ratelimit": SmsSendStatsErrorCodeExtra,
    "failed_due_to_application_error_other": SmsSendStatsErrorCodeExtra,
    "failed_due_to_client_error_429": None,
    "failed_due_to_client_error_other": SmsSendStatsHttpStatusCodeExtra,
    "failed_due_to_server_error": SmsSendStatsHttpStatusCodeExtra,
    "failed_due_to_internal_error": None,
    "failed_due_to_network_error": None,
}


async def increment_event(
    itgs: Itgs,
    *,
    event: SMSSendStatsEvent,
    extra: Optional[dict] = None,
    now: float,
    amount: int = 1,
) -> None:
    """Increments the count for the given event at the given time by one. This
    handles preparing the event, transaction handling, and retries.

    Args:
        itgs (Itgs): the integrations to (re)use
        event (SMSSendStatsEvent): the event to increment
        extra (dict, optional): the extra data to store with the event. The
            content of this depends on the event. Defaults to None.
        now (float): the time to increment the event at
        amount (int, optional): the amount to increment by. Defaults to 1.
    """
    redis = await itgs.redis()

    async def prep(force: bool):
        await prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await attempt_increment_event(
                redis, event=event, extra=extra, now=now, amount=amount
            )
            await pipe.execute()

    await run_with_prep(prep, func)


async def prepare_increment_event(client: redis.asyncio.Redis, *, force: bool = False):
    """Performs necessary work on the given client to prepare it to
    increment a push ticket stats event. This has to be done outside of
    a pipeline, and generally only needs to be called directly if you
    want to call attempt_increment_event alongside other commands within
    the same pipeline. Otherwise, use `increment_event` instead.

    This does not guarrantee that the attempt will succeed, but it does
    make it possible. This loads the scripts which will be required, which
    could then be removed again before the attempt actually starts (such
    as due to certain types of redis failovers, an explicit script flush, etc).
    Should generally retry failures at least once to handle with script flush,
    but note that it's generally complicated to deal with a redis failover
    since redis uses a persistent connection (so just waiting won't help, and
    you can't exactly queue a job since they are queued in redis)

    Args:
        client (redis.asyncio.Redis): The client to prepare, must not
            be actively pipelining
        force (bool, optional): If True, will force the script to be loaded
            even if we have loaded it recently. Defaults to False.
    """
    await ensure_set_if_lower_script_exists(client, force=force)


async def attempt_increment_event(
    client: redis.asyncio.Redis,
    *,
    event: SMSSendStatsEvent,
    extra: Optional[dict] = None,
    now: float,
    amount: int = 1,
) -> None:
    """Increments the given event within the given redis client. This does
    not require anything about the pipelining state of the client, however,
    it does assume certain scripts are loaded (as if via `prepare_increment_event`),
    and if they fail the commands will fail. In a pipelining context, this will
    mean the function call succeeds but the execute() call will fail, and changes
    at the time of increment and later (but not previous commands) will not be
    applied.

    Args:
        client (redis.asyncio.Redis): The client to increment on
        event (SMSSendStatsEvent): The event to increment
        extra (dict, optional): The extra data to store with the event. The
            content of this depends on the event. Defaults to None.
        now (float): The current time, in seconds since the epoch
        amount (int, optional): The amount to increment by. Defaults to 1.
    """
    expected_extra_type = SMS_SEND_STATS_EVENTS.get(event)
    if expected_extra_type is None and event not in SMS_SEND_STATS_EVENTS:
        raise ValueError(f"Invalid event: {event}")

    if expected_extra_type is None and extra is not None:
        raise ValueError(f"Event {event} does not support extra data")

    if expected_extra_type is not None and extra is None:
        raise ValueError(f"Event {event} requires extra data ({expected_extra_type})")

    typed_extra = None if extra is None else expected_extra_type.parse_obj(extra)

    unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=timezone)
    await set_if_lower(client, b"stats:sms_send:daily:earliest", unix_date)
    await client.hincrby(
        f"stats:sms_send:daily:{unix_date}".encode("ascii"),
        event.encode("utf-8"),
        amount,
    )

    if typed_extra is not None:
        await client.hincrby(
            f"stats:sms_send:daily:{unix_date}:extra:{event}".encode("utf-8"),
            typed_extra.redis_key,
            amount,
        )
