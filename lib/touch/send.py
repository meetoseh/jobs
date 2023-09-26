import secrets
import time
from itgs import Itgs
from typing import Any, Dict, Literal, Optional
from lib.shared.job_callback import JobCallback
from lib.touch.touch_info import TouchToSend
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.touch_send import (
    touch_send as redis_script_send_touch,
    ensure_touch_send_script_exists,
)
from redis.asyncio.client import Redis as AsyncioRedisClient
import unix_dates
import pytz


class SendTouchBackpressureError(Exception):
    """Raised when there is too much backpressure to send a touch."""

    def __init__(self):
        super().__init__("Too much backpressure to send touch")


BACKPRESSURE_LENGTH = 10_000

tz = pytz.timezone("America/Los_Angeles")


def create_touch_uid() -> str:
    """Generates a new random touch uid"""
    return f"oseh_tch_{secrets.token_urlsafe(16)}"


async def send_touch(
    itgs: Itgs,
    *,
    user_sub: str,
    touch_point_event_slug: str,
    channel: Literal["push", "sms", "email"],
    event_parameters: Dict[str, Any],
    uid: Optional[str] = None,
    success_callback: Optional[JobCallback] = None,
    failure_callback: Optional[JobCallback] = None,
) -> str:
    """A method to contact the given user using the touch point
    with the given event slug on the given channel, using the given event
    parameters (which differ based on the touch point used). Typically the
    success/failure callbacks are used for persisting/deleting user touch
    links within the event parameters.

    This only handles pushing the request to the `touch:to_send` queue. It
    checks for backpressure, failing the request with `SendTouchBackpressureError`
    if it's too full.

    Visibility into this system is extensively available; look for entries in
    `user_touch_debug_log`, `user_touches`, and `user_touch_point_states`.
    Furthermore, see the various explanations, values, and charts within the
    admin visibility section (touch, sms, email, push). Database updates will
    be delayed as we target less than 1/10 of a write transaction per touch
    (via batching). Most values in the visibility section will update immediately as
    they are buffered in redis for ~48 hours before being written to the database.

    This function will always send the touch in its own redis transaction,
    so for batching you should use `send_touch_in_pipe` instead. This functions
    implementation can be used as a guide for how to use `send_touch_in_pipe`.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str): the user to contact
        touch_point_event_slug (str): the event slug of the touch point to use
        channel ("push", "sms", "email"): the channel to use to contact the user
        event_parameters (Dict[str, Any]): the parameters to use for the event
            (see the touch point definition for details)
        uid (str, optional): if specified, used for the touch uid. Otherwise,
            a random one is generated.
        success_callback (JobCallback, optional): called as soon as any of the
            destinations selected succeeds.  Not passed any bonus parameters.
        failure_callback (JobCallback, optional): called after the delivery is
            abandoned or fails permanently for all destinations.

    Raises:
        SendTouchBackpressureError: if there is too much backpressure to send

    Returns:
        str: the uid of the touch that was sent
    """
    redis = await itgs.redis()
    touch = initialize_touch(
        user_sub=user_sub,
        touch_point_event_slug=touch_point_event_slug,
        channel=channel,
        event_parameters=event_parameters,
        uid=uid,
        success_callback=success_callback,
        failure_callback=failure_callback,
    )
    enc_touch = encode_touch(touch)

    async def prep(force: bool):
        await prepare_send_touch(redis, force=force)

    async def func():
        return await send_touch_in_pipe(redis, enc_touch)

    result = await run_with_prep(prep, func)
    assert isinstance(result, bool)
    if not result:
        raise SendTouchBackpressureError()
    return touch.uid


def initialize_touch(
    *,
    user_sub: str,
    touch_point_event_slug: str,
    channel: Literal["push", "sms", "email"],
    event_parameters: Dict[str, Any],
    uid: Optional[str] = None,
    success_callback: Optional[JobCallback] = None,
    failure_callback: Optional[JobCallback] = None,
) -> TouchToSend:
    """Initializes a touch to be sent; acts as an alternative constructor for
    TouchToSend that behaves exactly like send_touch

    See Also: `send_touch`

    Args:
        user_sub (str): the user to contact
        touch_point_event_slug (str): the event slug of the touch point to use
        channel ("push", "sms", "email"): the channel to use to contact the user
        event_parameters (Dict[str, Any]): the parameters to use for the event
            (see the touch point definition for details)
        uid (str, optional): if specified, used for the touch uid. Otherwise,
            a random one is generated.
        success_callback (JobCallback, optional): called as soon as any of the
            destinations selected succeeds.  Not passed any bonus parameters.
        failure_callback (JobCallback, optional): called after the delivery is
            abandoned or fails permanently for all destinations.
    """
    return TouchToSend(
        aud="send",
        uid=uid if uid is not None else create_touch_uid(),
        user_sub=user_sub,
        touch_point_event_slug=touch_point_event_slug,
        channel=channel,
        event_parameters=event_parameters,
        success_callback=success_callback,
        failure_callback=failure_callback,
        queued_at=time.time(),
    )


def encode_touch(touch: TouchToSend) -> bytes:
    """Encodes the given touch for sending via `send_touch_in_pipe`"""
    return touch.json().encode("utf-8")


async def prepare_send_touch(redis: AsyncioRedisClient, *, force: bool) -> None:
    """Prepares to send touches within a pipeline via `send_touch_in_pipe`. Generally
    only needs to be used if batching; use `send_touch` otherwise.

    Args:
        redis (redis.asyncio.client.Redis): the redis client to use
        force (bool): if True, forces the script to be checked even if it has
            been checked or created recently. False if it should only be checked
            if it hasn't been checked or created recently.
    """
    await ensure_touch_send_script_exists(redis, force=force)


async def send_touch_in_pipe(
    pipe: AsyncioRedisClient, touch: TouchToSend, enc_touch: bytes
) -> Optional[bool]:
    """Sends the given touch within a pipe, which allows for batching. The result
    must be checked when the pipeline is executed in order to know if backpressure was hit.

    Args:
        pipe (AsyncioRedisClient): the pipe to send the touch in
        touch (TouchToSend): the touch to send
        enc_touch (bytes): the touch encoded via `encode_touch`. we pass this
            in instead of creating it as it's almost always preferable to create
            it outside of the pipeline (to reduce the odds of an error inside the
            pipeline)

    Returns:
        (bool, None): If the pipeline is actually a pipeline, this returns None
            since the result isn't known yet. Otherwise, it returns a bool
            which is true if the touch was sent and false if backpressure was hit.
            The result can be parsed with just bool(result) in a pipeline.
    """
    unix_date = unix_dates.unix_timestamp_to_unix_date(touch.queued_at, tz=tz)
    stats_key = f"stats:touch_send:daily:{unix_date}".encode("ascii")

    return await redis_script_send_touch(
        pipe,
        b"touch:to_send",
        stats_key,
        b"stats:touch_send:daily:earliest",
        enc_touch,
        BACKPRESSURE_LENGTH,
        unix_date,
    )
