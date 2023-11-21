"""Assists with updating statistics related to polling for Twilio message resources
"""
from typing import Dict, Literal, Optional, Type, Union
from pydantic import BaseModel, Field
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
import redis.asyncio
import unix_dates
import pytz
from itgs import Itgs

timezone = pytz.timezone("America/Los_Angeles")

# TODO: Once on python 3.11, use enum.StrEnum
# Documentation for these events is under `docs/db/stats/sms_polling_stats.md`
SMSPollStatsEvent = Literal[
    "detected_stale",  # extra: "status"
    "queued_for_recovery",  # extra: number of previous failures
    "abandoned",  # extra: number of previous failures
    "attempted",
    "received",  # extra: "old_status:new_status"
    "error_client_404",
    "error_client_429",
    "error_client_other",  # extra: HTTP status code
    "error_server",  # extra: HTTP status code
    "error_network",
    "error_internal",
]


class SmsPollStatsStatusExtra(BaseModel):
    status: str = Field(description="The MessageStatus returned")

    @property
    def redis_key(self) -> bytes:
        return self.status.encode("utf-8")


class SmsPollStatsNumPreviousFailuresExtra(BaseModel):
    num_previous_failures: int = Field(description="The number of previous failures")

    @property
    def redis_key(self) -> bytes:
        return str(self.num_previous_failures).encode("utf-8")


class SmsPollStatsStatusChangeExtra(BaseModel):
    old_status: str = Field(description="The old MessageStatus")
    new_status: str = Field(description="The new MessageStatus")

    @property
    def redis_key(self) -> bytes:
        return f"{self.old_status}:{self.new_status}".encode("utf-8")


class SmsPollStatsHttpStatusCodeExtra(BaseModel):
    http_status_code: str = Field(description="The HTTP status code returned")

    @property
    def redis_key(self) -> bytes:
        return self.http_status_code.encode("utf-8")


SMS_POLL_STATS_EVENTS: Dict[
    SMSPollStatsEvent,
    Optional[
        Union[
            Type[SmsPollStatsStatusExtra],
            Type[SmsPollStatsNumPreviousFailuresExtra],
            Type[SmsPollStatsStatusChangeExtra],
            Type[SmsPollStatsHttpStatusCodeExtra],
        ]
    ],
] = {
    "detected_stale": SmsPollStatsStatusExtra,
    "queued_for_recovery": SmsPollStatsNumPreviousFailuresExtra,
    "abandoned": SmsPollStatsNumPreviousFailuresExtra,
    "attempted": None,
    "received": SmsPollStatsStatusChangeExtra,
    "error_client_404": None,
    "error_client_429": None,
    "error_client_other": SmsPollStatsHttpStatusCodeExtra,
    "error_server": SmsPollStatsHttpStatusCodeExtra,
    "error_network": None,
    "error_internal": None,
}


async def increment_event(
    itgs: Itgs,
    *,
    event: SMSPollStatsEvent,
    extra: Optional[dict] = None,
    now: float,
    amount: int = 1,
) -> None:
    """Increments the count for the given event at the given time by one. This
    handles preparing the event, transaction handling, and retries.

    Args:
        itgs (Itgs): the integrations to (re)use
        event (SMSPollStatsEvent): the event to increment
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
    increment a sms poll stats event. This has to be done outside of
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
    event: SMSPollStatsEvent,
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
        event (SMSPollStatsEvent): The event to increment
        extra (dict, optional): The extra data to store with the event. The
            content of this depends on the event. Defaults to None.
        now (float): The current time, in seconds since the epoch
        amount (int, optional): The amount to increment by. Defaults to 1.
    """
    expected_extra_type = SMS_POLL_STATS_EVENTS.get(event)
    if expected_extra_type is None and event not in SMS_POLL_STATS_EVENTS:
        raise ValueError(f"Invalid event: {event}")

    if expected_extra_type is None and extra is not None:
        raise ValueError(f"Event {event} does not support extra data")

    if expected_extra_type is not None and extra is None:
        raise ValueError(f"Event {event} requires extra data ({expected_extra_type})")

    typed_extra = (
        None
        if expected_extra_type is None
        else expected_extra_type.model_validate(extra)
    )

    unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=timezone)
    await set_if_lower(client, b"stats:sms_polling:daily:earliest", unix_date)
    await client.hincrby(  # type: ignore
        f"stats:sms_polling:daily:{unix_date}".encode("ascii"),  # type: ignore
        event.encode("utf-8"),  # type: ignore
        amount,
    )

    if typed_extra is not None:
        await client.hincrby(  # type: ignore
            f"stats:sms_polling:daily:{unix_date}:extra:{event}".encode("utf-8"),  # type: ignore
            typed_extra.redis_key,  # type: ignore
            amount,
        )
