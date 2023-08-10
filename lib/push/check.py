"""Utilities for failure jobs for push notifications handling failures at
the check step
"""


import time
from typing import Optional
from itgs import Itgs
from lib.push.message_attempt_info import MessageAttemptToCheck
import lib.push.receipt_stats
import redis_helpers.run_with_prep


async def retry_check_push(
    itgs: Itgs, *, attempt: MessageAttemptToCheck, now: Optional[float] = None
) -> None:
    """Queues the given attempt back to the cold set to be checked in about 15 minutes.
    This MUST NOT be done if the attempt is not retryable. If the attempt is retryable,
    either this or `abandon_check_push` MUST be called.

    Args:
        itgs (Itgs): the integrations to (re)use
        attempt (MessageAttemptToCheck): the attempt to retry
        now (float, None): if specified, takes the place of `time.time()`;
            generally used to get more consistent timestamps if this is
            happening at "the same time" as something else.
    """
    if now is None:
        now = time.time()

    entry = (
        MessageAttemptToCheck(
            aud="check",
            uid=attempt.uid,
            attempt_initially_queued_at=attempt.attempt_initially_queued_at,
            initially_queued_at=attempt.initially_queued_at,
            retry=attempt.retry + 1,
            last_queued_at=now,
            push_ticket=attempt.push_ticket,
            push_ticket_created_at=attempt.push_ticket_created_at,
            push_token=attempt.push_token,
            contents=attempt.contents,
            failure_job=attempt.failure_job,
            success_job=attempt.success_job,
        )
        .json()
        .encode("utf-8")
    )

    redis = await itgs.redis()

    async def prep(force: bool):
        await lib.push.receipt_stats.prepare_increment_event(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await lib.push.receipt_stats.attempt_increment_event(
                pipe, event="retried", now=attempt.attempt_initially_queued_at
            )
            await pipe.zadd(b"push:push_tickets:cold", mapping=dict([(entry, now)]))
            await pipe.execute()

    await redis_helpers.run_with_prep.run_with_prep(prep, func)


async def abandon_check_push(itgs: Itgs, *, attempt: MessageAttemptToCheck):
    """Abandons the given retryable message attempt. Failure jobs which receive
    a MessageAttemptToCheck on a retryable error should call this to abandon
    the message. This MUST NOT be called for a non-retryable error.

    Args:
        itgs (Itgs): The integrations to (re)use
        attempt (MessageAttemptToCheck): The message attempt to abandon
    """
    await lib.push.receipt_stats.increment_event(
        itgs, event="abandoned", now=attempt.attempt_initially_queued_at
    )
