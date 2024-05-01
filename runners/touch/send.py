"""Touch Send Job"""

import io
import json
import random
import secrets
import sys
import time
import asyncio
from typing import (
    Awaitable,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from pydantic import TypeAdapter
from error_middleware import handle_error, handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import dataclasses
from lib.basic_redis_lock import basic_redis_lock
from lib.emails.send import EmailMessageContents, create_email_uid
from lib.emails.email_info import EmailAttempt
from lib.push.message_attempt_info import MessageAttemptToSend, MessageContents
from lib.push.send import create_message_attempt_uid
from lib.redis_stats_preparer import RedisStatsPreparer
from lib.shared.job_callback import JobCallback
from lib.sms.send import SmsMessageContents, create_sms_uid
from lib.sms.sms_info import SMSToSend
from lib.touch.touch_info import (
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchDebugLogInsertFields,
    TouchLogUserTouchEmailMessage,
    TouchLogUserTouchPointStateInsert,
    TouchLogUserTouchPointStateInsertFields,
    TouchLogUserTouchPointStateUpdate,
    TouchLogUserTouchPointStateUpdateFields,
    TouchLogUserTouchPushInsertMessage,
    TouchLogUserTouchSMSMessage,
    TouchPending,
    TouchToSend,
    UserTouchDebugLogEventSendAttempt,
    UserTouchDebugLogEventSendReachable,
    UserTouchDebugLogEventSendStale,
    UserTouchDebugLogEventSendUnreachable,
    UserTouchPointStateState,
    UserTouchPointStateStateOrderedResettable,
)
from lib.touch.touch_points import (
    TouchPointEmailMessage,
    TouchPointMessages,
    TouchPointPushMessage,
    TouchPointSmsMessage,
)
from redis_helpers.lmove_many import ensure_lmove_many_script_exists, lmove_many
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.set_if_lower import ensure_set_if_lower_script_exists, set_if_lower
import pytz
import unix_dates
from redis.asyncio import Redis as AsyncioRedisClient
import base64
import gzip
import bisect

category = JobCategory.LOW_RESOURCE_COST
tz = pytz.timezone("America/Los_Angeles")

MAX_JOB_TIME_SECONDS = 50
"""The amount of time after which we stop due to time_exhausted to allow other
jobs to run
"""

SUBQUEUE_BACKPRESSURE_THRESHOLD = 1_000
"""The length of a subqueue that triggers backpressure"""

STALE_THRESHOLD_SECONDS = 60 * 60 * 8
"""The number of seconds a touch can be in the send queue before we drop it due to
staleness
"""

JOB_BATCH_SIZE = 400
"""The number of touches we handle at a time. 

These are moved within a redis script in a single transaction. Without
serialization being required, this is very fast, and unlikely to be
the primarily limiter for this size.

The entire batch is then loaded into memory (in batches) and converted into the
redis commands that will need to be executed, before any are executed. If
anything goes wrong constructing the redis commands, nothing is executed and the
job fails. If, however, something goes wrong executing the commands, all the
commands will need to be rerun, meaning some are repeated.

Hence there are two primary considerations for restricting this value: memory
usage and error mitigation. For memory usage, multiply by 10 for a rough
estimate of memory usage in kilobytes (e.g., 400 -> 4MB). For error handling, a
larger batch means more re-attempts on errors, meaning more duplicate messages
if redis fails over mid-job.

The main reason to increase this value is it will result in better database
query batches and better redis transactions, which will result in faster
performance. In particular, querying the database within a transaction is
roughly 300x faster than querying with individual transactions, and redis
is about 100x faster with pipelines. However, for both there is an upper
limit on how big our batches can be without causing other callers to hang
excessively long, and hence there is an upper limit on practical performance
(i.e., when we start consistently reaching the database batch size limit
and redis batch size limit for most batches)
"""

DATABASE_BATCH_SIZE = 100
"""How many items we request within a single query from the database at a time.
Because of how rqlite is structured, all these items will need to be in memory
at least 2x (once for us and once for the database instances), and more
practically 4x (as serialized/deserialized representations are briefly
held simultaneously unless extremely careful)
"""

MAX_CONCURRENT_FORMING_BATCHES = 3
"""The maximum number of concurrently forming batches. In other words, as we
read from the purgatory queue we can form batches by channel. As we receive
batches we concurrently start augmenting them with the additional information
(contact addresses). This limits the number of batches we are augmenting at a time.

A rule of thumb is this should be about the same size as the rqlite cluster,
since we can read from any of the nodes without contacting others, the peak
parallelization is proportional to the number of nodes (sqlite performs best
when single-threaded)
"""

REDIS_FETCH_BATCH_SIZE = 12
"""How many items we request from redis at a time; should be small enough to avoid
blocking redis for too long. Counter-intuitively, extremely small values (like
1) will increase average block time for other requests since they are so much
more likely to encounter contention as this job will take so long, so there is a
happy medium where this job finishes quickly but other requests can still weave
in while this is running.
"""

REDIS_SEND_BATCH_SIZE = 12
"""How many items we send to redis at a time; should be small enough to avoid
blocking redis for too long. Counter-intuitively, extremely small values (like
1) will increase average block time for other requests since they are so much
more likely to encounter contention as this job will take so long, so there is a
happy medium where this job finishes quickly but other requests can still weave
in while this is running.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls messages in batches off the left of the To Send queue, moving them
    to purgatory for processing. Splits the messages by channel, then collects
    the contact addresses in batches by channel while simultaneously determining
    the touch points for the messages. Then realizes the message for each touch
    point, where email is "realized" into a template slug and template parameters,
    keeping it small.

    Finally, the touches in the batch are sent to the appropriate subqueues and
    removed from purgatory.

    NOTE:
        This is implemented with several helper classes and functions due to the
        complexity of interweaving so many operations.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(itgs, b"touch:send_job:lock", gd=gd, spin=False):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_send:send_job", b"started_at", str(started_at).encode("ascii")  # type: ignore
        )

        job_batch_handler = JobBatchHandler()
        touch_points_set = TouchPointsSet()
        run_stats = RunStats()

        if await any_subqueue_backpressure(itgs):
            logging.warning("Touch Send Job - backpressure detected")
            await report_run_stats(
                itgs, touch_points_set, run_stats, started_at, "backpressure"
            )
            return

        await job_batch_handler.get_first_batch(itgs)
        stop_reason: Optional[str] = None

        while True:
            if gd.received_term_signal:
                logging.warning("Touch Send Job - received signal, stopping")
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.warning("Touch Send Job - time exhausted, stopping")
                stop_reason = "time_exhausted"
                break

            if await any_subqueue_backpressure(itgs):
                logging.warning("Touch Send Job - backpressure detected")
                stop_reason = "backpressure"
                break

            if job_batch_handler.purgatory_size is None:
                await job_batch_handler.get_next_batch(itgs)
                assert job_batch_handler.purgatory_size is not None

            if job_batch_handler.purgatory_size == 0:
                logging.info("Touch Send Job - list exhausted, stopping")
                stop_reason = "list_exhausted"
                break

            redis_transactions: List[RedisTransaction] = []
            # All the redis transactions we need to perform to handle the batch.
            # In theory some of these could be combined, but doing so while handling
            # scripts and partially-run transactions would be complicated. These callbacks
            # should already have everything eagerly loaded, as once we start executing
            # there will be no way to roll back, so if an error occurs we will repeat
            # processing when the send job is run again (due to the purgatory queue)
            # In other words, these are run at least once.

            batch_at: float = time.time()

            async def on_stale(touch: TouchToSend):
                unix_date = get_touch_unix_date(touch)
                stats_key = touch_send_stats_key(unix_date)
                attempted_extra_key = touch_send_stats_extra_key(unix_date, "attempted")
                attempt_breakdown = (
                    f"{touch.touch_point_event_slug}:{touch.channel}".encode("utf-8")
                )
                attempt_uid = create_user_touch_debug_log_uid()
                logs: List[bytes] = [
                    TouchLogUserTouchDebugLogInsert(
                        table="user_touch_debug_log",
                        action="insert",
                        fields=TouchLogUserTouchDebugLogInsertFields(
                            uid=attempt_uid,
                            user_sub=touch.user_sub,
                            event=UserTouchDebugLogEventSendAttempt(
                                type="send_attempt",
                                queued_at=touch.queued_at,
                                channel=touch.channel,
                                event=touch.touch_point_event_slug,
                                event_parameters=touch.event_parameters,
                            ),
                            created_at=batch_at,
                        ),
                        queued_at=batch_at,
                    )
                    .model_dump_json()
                    .encode("utf-8"),
                    TouchLogUserTouchDebugLogInsert(
                        table="user_touch_debug_log",
                        action="insert",
                        fields=TouchLogUserTouchDebugLogInsertFields(
                            uid=create_user_touch_debug_log_uid(),
                            user_sub=touch.user_sub,
                            event=UserTouchDebugLogEventSendStale(
                                type="send_stale",
                                parent=attempt_uid,
                            ),
                            created_at=batch_at,
                        ),
                        queued_at=batch_at,
                    )
                    .model_dump_json()
                    .encode("utf-8"),
                ]
                job: Optional[bytes] = None

                if touch.failure_callback is not None:
                    job = json.dumps(
                        {
                            "name": touch.failure_callback.name,
                            "kwargs": touch.failure_callback.kwargs,
                            "queued_at": batch_at,
                        }
                    ).encode("utf-8")

                async def _prep(force: bool):
                    await ensure_set_if_lower_script_exists(redis, force=force)

                async def _func():
                    async with redis.pipeline() as pipe:
                        pipe.multi()
                        await set_if_lower(
                            pipe, touch_send_stats_earliest_key, unix_date
                        )
                        await pipe.hincrby(stats_key, b"attempted", 1)  # type: ignore
                        await pipe.hincrby(attempted_extra_key, attempt_breakdown, 1)  # type: ignore
                        await pipe.hincrby(stats_key, b"stale", 1)  # type: ignore
                        await pipe.rpush(b"touch:to_log", *logs)  # type: ignore
                        if job is not None:
                            await pipe.rpush(b"jobs:hot", job)  # type: ignore
                        await pipe.execute()

                redis_transactions.append(
                    RedisTransaction(RunStats(attempted=1, stale=1), _prep, _func)
                )

            async def on_push_batch(batch: AugmentedPushBatch):
                for start_idx in range(0, len(batch.touches), REDIS_SEND_BATCH_SIZE):
                    end_idx = min(start_idx + REDIS_SEND_BATCH_SIZE, len(batch.touches))
                    transaction = await PushBatchSegmentPreparer.prepare_batch_segment(
                        itgs, touch_points_set, batch, start_idx, end_idx, batch_at
                    )
                    redis_transactions.append(transaction)

            async def on_sms_batch(batch: AugmentedSmsBatch):
                for start_idx in range(0, len(batch.touches), REDIS_SEND_BATCH_SIZE):
                    end_idx = min(start_idx + REDIS_SEND_BATCH_SIZE, len(batch.touches))
                    transaction = await SmsBatchSegmentPreparer.prepare_batch_segment(
                        itgs, touch_points_set, batch, start_idx, end_idx, batch_at
                    )
                    redis_transactions.append(transaction)

            async def on_email_batch(batch: AugmentedEmailBatch):
                for start_idx in range(0, len(batch.touches), REDIS_SEND_BATCH_SIZE):
                    end_idx = min(start_idx + REDIS_SEND_BATCH_SIZE, len(batch.touches))
                    transaction = await EmailBatchSegmentPreparer.prepare_batch_segment(
                        itgs, touch_points_set, batch, start_idx, end_idx, batch_at
                    )
                    redis_transactions.append(transaction)

            batch_former = AugmentedBatchFormer(
                on_push_batch, on_sms_batch, on_email_batch, on_stale
            )
            batch_at = time.time()
            await batch_former.form_augmented_batches(
                itgs, touch_points_set, job_batch_handler.purgatory_size, now=batch_at
            )
            batches_formed_at = time.time()
            logging.debug(
                f"Touch Send Job formed augmented batches in {batches_formed_at-batch_at:.3f}s:"
            )

            # It's still safe to stop if we got a signal, and we don't want to push our luck
            # and get SIGKILL'd while executing
            if gd.received_term_signal:
                logging.warning(
                    "Touch Send Job - received signal between preparing and executing, "
                    f"stopping safely with {job_batch_handler.purgatory_size} items in purgatory"
                )
                stop_reason = "signal"
                break

            for transaction in redis_transactions:
                await run_with_prep(transaction.prep, transaction.func)

            transactions_finished_at = time.time()
            logging.debug(
                f"Touch Send Job executed {len(redis_transactions)} redis transactions in {transactions_finished_at-batches_formed_at:.3f}s"
            )
            await job_batch_handler.clear_purgatory(itgs)

            for transaction in redis_transactions:
                run_stats.add(transaction.run_stats)

        assert stop_reason is not None
        await report_run_stats(
            itgs, touch_points_set, run_stats, started_at, stop_reason
        )


class JobBatchHandler:
    """Handles the purgatory queue, i.e., resuming a previously failed run
    and moving data from the larger to_send queue into the purgatory queue
    """

    def __init__(self):
        self.purgatory_size: Optional[int] = None
        """The number of items we believe are in purgatory. Due to the job
        lock we assume this value doesn't change unless we alter it. If we
        were to parallelize this job, this would get complicated
        """

        self.seen_last_batch: bool = False
        """True if we've seen the end of the base queue, i.e., we exhausted
        it when trying to move items to purgatory. This is to avoid repeatedly
        doing tiny batches while items are being inserted.
        """

    async def get_first_batch(self, itgs: Itgs) -> bool:
        """Checks the purgatory list. If it's already got items, we are resuming
        a failed run and will use that as our first batch. Otherwise, we attempt
        to move JOB_BATCH_SIZE items from the to_send queue into the purgatory
        queue to be used as the first batch.

        Returns:
            True if we found items for the first batch, False if there is nothing
            to do.
        """
        assert self.purgatory_size is None, "already have a batch"
        redis = await itgs.redis()
        initial_size = await redis.llen(b"touch:send_purgatory")  # type: ignore
        if initial_size > 0:
            logging.warning(
                f"Touch Send Job - Resuming failed previous run, have {initial_size} items in purgatory"
            )
            slack = await itgs.slack()
            await slack.send_web_error_message(
                f"*Touch Send Job* - recovering {initial_size=} touches from purgatory",
                "Touch Send Job - recovering from purgatory",
            )
            self.purgatory_size = initial_size
            return True

        return await self.get_next_batch(itgs)

    async def get_next_batch(self, itgs: Itgs):
        """Moves a batch from the to_send queue into the purgatory queue, returning
        true if any items were moved and false otherwise.

        This assumes that the purgatory queue is empty.
        """
        assert self.purgatory_size is None, "already have a batch"
        if self.seen_last_batch:
            self.purgatory_size = 0
            return False

        redis = await itgs.redis()

        async def prep(force: bool):
            await ensure_lmove_many_script_exists(redis, force=force)

        async def func():
            return await lmove_many(
                redis, b"touch:to_send", b"touch:send_purgatory", JOB_BATCH_SIZE
            )

        num_moved = await run_with_prep(prep, func)
        assert isinstance(num_moved, int)
        self.purgatory_size = num_moved

        if num_moved == 0:
            return False

        self.seen_last_batch = num_moved < JOB_BATCH_SIZE
        return True

    async def clear_purgatory(self, itgs: Itgs):
        """Removes the items from purgatory, assuming they have been processed."""
        redis = await itgs.redis()
        await redis.delete(b"touch:send_purgatory")
        self.purgatory_size = None


class PurgatoryReader:
    """Handles reading from the purgatory queue in batches. Not re-entrant,
    i.e., only one read_next_batch call should be active at a time.
    """

    def __init__(self, purgatory_size: int) -> None:
        self.purgatory_size = purgatory_size
        self.index = 0

    @property
    def have_more(self) -> bool:
        """Whether there are more items to read"""
        return self.index < self.purgatory_size

    async def read_next_batch(self, itgs: Itgs) -> List[TouchToSend]:
        """Reads the next batch of items from the purgatory queue, or an
        empty list if there are no more items. Reads up to REDIS_FETCH_BATCH_SIZE
        items at a time.
        """
        if self.index >= self.purgatory_size:
            return []

        end_index_inclusive = min(
            self.index + REDIS_FETCH_BATCH_SIZE - 1, self.purgatory_size - 1
        )

        redis = await itgs.redis()
        result = await redis.lrange(
            b"touch:send_purgatory",  # type: ignore
            self.index,
            end_index_inclusive,
        )

        self.index = end_index_inclusive + 1
        return [TouchToSend.model_validate_json(item) for item in result]

    async def reset(self, purgatory_size: int) -> None:
        """Resets the reader to the beginning of the purgatory queue"""
        self.purgatory_size = purgatory_size
        self.index = 0


@dataclasses.dataclass
class TouchPoint:
    uid: str
    """The unique id of the touch point"""

    event_slug: str
    """The event slug of the touch point"""

    selection_strategy: Literal[
        "random_with_replacement", "fixed", "ordered_resettable"
    ]
    """The selection strategy of the touch point"""

    messages: TouchPointMessages
    """The messages that can be sent from this touch point"""


class TouchPointsSet:
    """When we encounter touch points within the touches we want to read them from
    the database so that we can realize the corresponding messages. However, since the
    messages can be somewhat large we do not want to fetch multiple touch points within
    a single query. Hence, this facilitates eagerly fetch them one at a time as we encounter them
    """

    def __init__(self):
        self.fetched: Dict[str, TouchPoint] = dict()
        """The touch points we have already fetched by event slug"""

        self.fetching: Dict[str, asyncio.Task[TouchPoint]] = dict()
        """The touch points we are actively fetching by event slug"""

        self._lock = asyncio.Lock()
        """a lock for modifying fetched/fetching"""

    async def add(self, itgs: Itgs, event_slug: str) -> None:
        """If we are not fetching and have not fetched the event slug with the
        given string, queues a background task to fetch it and returns, otherwise
        does nothing.
        """
        async with self._lock:
            if event_slug in self.fetched or event_slug in self.fetching:
                return
            self.fetching[event_slug] = asyncio.create_task(
                self._fetch(itgs, event_slug)
            )

    async def has_ready(self, event_slug: str) -> bool:
        """Returns if the given event slug is ready to be returned immediately"""
        async with self._lock:
            return event_slug in self.fetched

    async def get_immediately(self, event_slug: str) -> TouchPoint:
        """Returns the touch point with the given event slug, raising a
        KeyError if it's not immediately available
        """
        async with self._lock:
            return self.fetched[event_slug]

    async def get(self, itgs: Itgs, event_slug: str) -> TouchPoint:
        """Fetches the touch point with the given event slug, blocking until
        it is available.
        """
        async with self._lock:
            if event_slug in self.fetched:
                return self.fetched[event_slug]
            task = self.fetching.get(event_slug)
        if task is None:
            await self.add(itgs, event_slug)
            return await self.get(itgs, event_slug)
        return await task

    async def get_first_available(self, event_slugs: Iterable[str]) -> TouchPoint:
        """Gets the first touch point that becomes available from the given
        event slugs, raising an error if any of them are not either fetched
        or being fetched.
        """
        tasks: List[asyncio.Task[TouchPoint]] = []
        async with self._lock:
            for slug in event_slugs:
                if slug in self.fetched:
                    return self.fetched[slug]
                tasks.append(self.fetching[slug])

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for itm in done:
            return itm.result()
        assert False, "all tasks are pending despite asyncio.wait"

    async def _fetch(self, itgs: Itgs, event_slug: str) -> TouchPoint:
        """Fetches the touch point with the given event slug. Once fetched, its
        removed from self.fetching, added to self.fetched, and returned
        """
        conn = await itgs.conn()
        cursor = conn.cursor("none")

        response = await cursor.execute(
            "SELECT uid, selection_strategy, messages FROM touch_points WHERE event_slug = ?",
            (event_slug,),
        )

        assert response.results, f"no touch point with event slug {event_slug=}"
        row = response.results[0]
        encoded_compressed_messages = row[2]
        compressed_messages = base64.b85decode(encoded_compressed_messages)
        raw_messages = gzip.decompress(compressed_messages)
        messages = TouchPointMessages.model_validate_json(raw_messages)
        result = TouchPoint(
            uid=row[0],
            event_slug=event_slug,
            selection_strategy=row[1],
            messages=messages,
        )
        async with self._lock:
            del self.fetching[event_slug]
            self.fetched[event_slug] = result
        return result

    async def size(self) -> int:
        """Returns the number of items we have fetched or are fetching"""
        async with self._lock:
            return len(self.fetched) + len(self.fetching)


class BatchFormer:
    """Fetches touches to send from the purgatory queue, grouping them into similar
    items (those with the same channel) so that caller can transform them as soon
    as they become available
    """

    def __init__(
        self,
        on_batch: Callable[[str, List[TouchToSend]], Awaitable[None]],
        on_stale: Callable[[TouchToSend], Awaitable[None]],
    ) -> None:
        self.on_batch = on_batch
        """The handler for when we form a batch containing all non-stale touches of a single channel.
        Has the signature

        ```py
        async def on_batch(channel: str, touches: List[TouchToSend]) -> None:
            ...
        ```

        This does not proceed until the handler returns, which usually means that the
        handler is just responsible for starting another task with the batch and returning,
        potentially blocking to reduce concurrency if it has many tasks already running.
        """

        self.on_stale = on_stale
        """This will filter out stale items and send them to a separate handler with the
        signature

        ```py
        async def on_stale(touch: TouchToSend) -> None:
            ...
        ```
        """

    async def form_batches(
        self,
        itgs: Itgs,
        touch_point_set: TouchPointsSet,
        purgatory_size: int,
        *,
        now: float,
    ) -> None:
        """Forms batches from the purgatory set which has the given size until
        the entire purgatory has been read and all items have been assigned a
        batch, calling the on_batch handler for each batch, and the last
        handler has completed.

        As touches are encountered their touch points are immediately added
        to the touch point set, so that we can begin fetching touch points
        without waiting for batches to be formed.

        Args:
            itgs (Itgs): the integrations to (re)use
            touch_point_set (TouchPointsSet): the object to handle fetching touch
                points; we will attempt to fetch touch points as soon as they are
                detected, but the callback may be called before all the touch points
                in the batch are available
            purgatory_size (int): the length of the purgatory queue, to facilitate
                iteration
            now (float): the current time in seconds since the epoch, used to determine
                staleness
        """
        reader = PurgatoryReader(purgatory_size)

        by_channel: Dict[str, List[TouchToSend]] = dict()
        min_age = now - STALE_THRESHOLD_SECONDS

        while reader.have_more:
            unsorted = await reader.read_next_batch(itgs)
            for touch in unsorted:
                if touch.queued_at < min_age:
                    await self.on_stale(touch)
                    continue
                await touch_point_set.add(itgs, touch.touch_point_event_slug)
                channel_batch = by_channel.get(touch.channel)
                if channel_batch is None:
                    channel_batch = list()
                    by_channel[touch.channel] = channel_batch

                channel_batch.append(touch)
                if len(channel_batch) >= JOB_BATCH_SIZE:
                    await self.on_batch(touch.channel, channel_batch)
                    del by_channel[touch.channel]

        for channel, channel_batch in by_channel.items():
            await self.on_batch(channel, channel_batch)


async def augment_push_batch_with_tokens(
    itgs: Itgs, batch: List[TouchToSend]
) -> Dict[str, List[str]]:
    """For each reachable user_sub within the given batch of push touches,
    the returned dictionary contains the list of push tokens to use to contact
    that user sub.
    """
    distinct_user_subs = frozenset(touch.user_sub for touch in batch)
    if not distinct_user_subs:
        return dict()

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    query_sql = io.StringIO()
    query_params = []

    query_sql.write("WITH user_subs(sub) AS (VALUES ")
    for idx, user_sub in enumerate(distinct_user_subs):
        if idx > 0:
            query_sql.write(", ")
        query_sql.write("(?)")
        query_params.append(user_sub)
    query_sql.write(
        ") "
        "SELECT"
        " user_subs.sub,"
        " user_push_tokens.token "
        "FROM user_subs "
        "JOIN users ON users.sub = user_subs.sub "
        "JOIN user_push_tokens ON user_push_tokens.user_id = users.id "
        "WHERE user_push_tokens.receives_notifications"
    )

    response = await cursor.execute(query_sql.getvalue(), query_params)

    result = dict()
    for user_sub, token in response.results or []:
        user_tokens = result.get(user_sub)
        if user_tokens is None:
            user_tokens = []
            result[user_sub] = user_tokens
        user_tokens.append(token)
    return result


async def augment_sms_batch_with_phone_numbers(
    itgs: Itgs, batch: List[TouchToSend]
) -> Dict[str, List[str]]:
    """For each reachable user_sub within the given batch of sms touches,
    the returned dictionary contains the list of phone numbers to use to contact
    that user sub.
    """
    distinct_user_subs = frozenset(touch.user_sub for touch in batch)

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    query_sql = io.StringIO()
    query_params = []

    query_sql.write("WITH user_subs(sub) AS (VALUES ")
    for idx, user_sub in enumerate(distinct_user_subs):
        if idx > 0:
            query_sql.write(", ")
        query_sql.write("(?)")
        query_params.append(user_sub)
    query_sql.write(
        ") "
        "SELECT"
        " user_subs.sub,"
        " user_phone_numbers.phone_number "
        "FROM user_subs, users, user_phone_numbers "
        "WHERE"
        " user_subs.sub = users.sub"
        " AND users.id = user_phone_numbers.user_id"
        " AND user_phone_numbers.verified"
        " AND user_phone_numbers.receives_notifications"
        " AND NOT EXISTS (SELECT 1 FROM suppressed_phone_numbers WHERE suppressed_phone_numbers.phone_number = user_phone_numbers.phone_number)"
    )

    response = await cursor.execute(query_sql.getvalue(), query_params)

    result = dict()
    for user_sub, phone_number in response.results or []:
        user_phone_numbers = result.get(user_sub)
        if user_phone_numbers is None:
            user_phone_numbers = []
            result[user_sub] = user_phone_numbers
        user_phone_numbers.append(phone_number)
    return result


async def augment_email_batch_with_email_addresses(
    itgs: Itgs, batch: List[TouchToSend]
) -> Dict[str, List[str]]:
    """For each reachable user_sub within the given batch of email touches,
    the returned dictionary contains the list of email addresses to use to contact
    that user sub.
    """
    distinct_user_subs = frozenset(touch.user_sub for touch in batch)

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    query_sql = io.StringIO()
    query_params = []

    query_sql.write("WITH user_subs(sub) AS (VALUES ")
    for idx, user_sub in enumerate(distinct_user_subs):
        if idx > 0:
            query_sql.write(", ")
        query_sql.write("(?)")
        query_params.append(user_sub)
    query_sql.write(
        ") "
        "SELECT"
        " user_subs.sub,"
        " user_email_addresses.email "
        "FROM user_subs, users, user_email_addresses "
        "WHERE"
        " user_subs.sub = users.sub"
        " AND users.id = user_email_addresses.user_id"
        " AND user_email_addresses.verified"
        " AND user_email_addresses.receives_notifications"
        " AND NOT EXISTS (SELECT 1 FROM suppressed_emails WHERE suppressed_emails.email_address = user_email_addresses.email COLLATE NOCASE)"
    )

    response = await cursor.execute(query_sql.getvalue(), query_params)

    result = dict()
    for user_sub, email in response.results or []:
        user_emails = result.get(user_sub)
        if user_emails is None:
            user_emails = []
            result[user_sub] = user_emails
        user_emails.append(email)
    return result


@dataclasses.dataclass
class UserTouchPointState:
    version: int
    state: UserTouchPointStateState

user_touch_point_state_adapter = cast(TypeAdapter[UserTouchPointStateState], TypeAdapter(UserTouchPointStateState))


async def augment_batch_with_user_touch_point_states(
    itgs: Itgs, touch_points: TouchPointsSet, batch: List[TouchToSend]
) -> Dict[Tuple[str, str], UserTouchPointState]:
    """For each (user, touch point) within the batch, the returned dictionary
    contains the user touch point state for that user and touch point, which is
    required to determine what message to send, or none if there is none for
    that combination. This will block on the touch point being available for
    each of the touch points within the batch as if the selection strategy does
    not require states, this will return an empty dictionary

    NOTE:
        This requires all the items within the batch are for the same channel
    """
    if not batch:
        return dict()

    unique_touch_point_event_slugs = list(
        frozenset(touch.touch_point_event_slug for touch in batch)
    )

    relevant_touch_points: Dict[str, TouchPoint] = dict(
        zip(
            unique_touch_point_event_slugs,
            await asyncio.gather(
                *[
                    touch_points.get(itgs, event_slug)
                    for event_slug in unique_touch_point_event_slugs
                ]
            ),
        )
    )

    unique_identifiers = frozenset(
        (touch.user_sub, touch.touch_point_event_slug)
        for touch in batch
        if relevant_touch_points[touch.touch_point_event_slug].selection_strategy
        != "random_with_replacement"
    )
    if not unique_identifiers:
        return dict()

    unique_channels = frozenset(touch.channel for touch in batch)
    assert len(unique_channels) == 1

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    query_sql = io.StringIO()
    query_params = []

    query_sql.write("WITH state_identifiers(sub, slug) AS (VALUES ")
    for idx, (user_sub, event_slug) in enumerate(unique_identifiers):
        if idx > 0:
            query_sql.write(", ")
        query_sql.write("(?, ?)")
        query_params.append(user_sub)
        query_params.append(event_slug)
    query_sql.write(
        ") "
        "SELECT"
        " state_identifiers.sub,"
        " state_identifiers.slug,"
        " user_touch_point_states.state,"
        " user_touch_point_states.version "
        "FROM state_identifiers "
        "JOIN users ON users.sub = state_identifiers.sub "
        "JOIN touch_points ON touch_points.event_slug = state_identifiers.slug "
        "JOIN user_touch_point_states ON ("
        " user_touch_point_states.user_id = users.id"
        " AND user_touch_point_states.touch_point_id = touch_points.id"
        " AND user_touch_point_states.channel = ?"
        ")"
    )
    query_params.append(batch[0].channel)

    response = await cursor.execute(query_sql.getvalue(), query_params)

    result = dict()
    for user_sub, event_slug, state, version in response.results or []:
        result[(user_sub, event_slug)] = UserTouchPointState(
            version=version,
            state=user_touch_point_state_adapter.validate_json(state),
        )
    return result


@dataclasses.dataclass
class AugmentedPushBatch:
    touches: List[TouchToSend]
    """The touches within the batch"""

    tokens: Dict[str, List[str]]
    """The push tokens for each reachable user sub within the batch"""

    user_touch_point_states: Dict[Tuple[str, str], UserTouchPointState]
    """The user touch point states for each (user, touch point) within the batch"""


@dataclasses.dataclass
class AugmentedSmsBatch:
    touches: List[TouchToSend]
    """The touches within the batch"""

    phone_numbers: Dict[str, List[str]]
    """The phone numbers for each reachable user sub within the batch"""

    user_touch_point_states: Dict[Tuple[str, str], UserTouchPointState]
    """The user touch point states for each (user, touch point) within the batch"""


@dataclasses.dataclass
class AugmentedEmailBatch:
    touches: List[TouchToSend]
    """The touches within the batch"""

    email_addresses: Dict[str, List[str]]
    """The email addresses for each reachable user sub within the batch"""

    user_touch_point_states: Dict[Tuple[str, str], UserTouchPointState]
    """The user touch point states for each (user, touch point) within the batch"""


class AugmentedBatchFormer:
    def __init__(
        self,
        on_push_batch: Callable[[AugmentedPushBatch], Awaitable[None]],
        on_sms_batch: Callable[[AugmentedSmsBatch], Awaitable[None]],
        on_email_batch: Callable[[AugmentedEmailBatch], Awaitable[None]],
        on_stale: Callable[[TouchToSend], Awaitable[None]],
    ) -> None:
        self.on_push_batch = on_push_batch
        """The callback for when a push batch is formed."""

        self.on_sms_batch = on_sms_batch
        """The callback for when an sms batch is formed."""

        self.on_email_batch = on_email_batch
        """The callback for when an email batch is formed."""

        self.on_stale = on_stale
        """The callback when a stale touch is encountered and excluded from batches"""

    async def form_augmented_batches(
        self,
        itgs: Itgs,
        touch_points_set: TouchPointsSet,
        purgatory_size: int,
        *,
        now: float,
    ) -> None:
        """Forms augmented batches from the purgatory set which has the given size until
        the entire purgatory has been read and all items have been assigned a
        batch, every batch has been augmented, each augmented batch has been
        sent to the corresponding callback, and the last callback has completed.

        Args:
            itgs (Itgs): the integrations to (re)use
            touch_points_set (TouchPointsSet): the object to handle fetching touch
                points; we will attempt to fetch touch points as soon as they are
                detected, and we will ensure all the touch points within a batch
                are immediately available before calling the callback
            purgatory_size (int): the number of items in purgatory
            now (float): the current time in seconds since the epoch, used to determine
                staleness
        """
        augmentation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FORMING_BATCHES)
        augmenting: Set[asyncio.Task[None]] = set()

        async def handle_raw_batch(channel: str, raw_batch: List[TouchToSend]):
            assert channel in ("push", "sms", "email"), channel
            async with augmentation_semaphore:
                if channel == "push":
                    tokens, user_touch_point_states = await asyncio.gather(
                        augment_push_batch_with_tokens(itgs, raw_batch),
                        augment_batch_with_user_touch_point_states(
                            itgs, touch_points_set, raw_batch
                        ),
                    )
                    batch = AugmentedPushBatch(
                        touches=raw_batch,
                        tokens=tokens,
                        user_touch_point_states=user_touch_point_states,
                    )
                    await self.on_push_batch(batch)
                elif channel == "sms":
                    logging.debug("  getting phone numbers and touch point states..")
                    phone_numbers, user_touch_point_states = await asyncio.gather(
                        augment_sms_batch_with_phone_numbers(itgs, raw_batch),
                        augment_batch_with_user_touch_point_states(
                            itgs, touch_points_set, raw_batch
                        ),
                    )
                    logging.debug("  creating batch..")
                    batch = AugmentedSmsBatch(
                        touches=raw_batch,
                        phone_numbers=phone_numbers,
                        user_touch_point_states=user_touch_point_states,
                    )
                    logging.debug("  dispatching batch..")
                    await self.on_sms_batch(batch)
                    logging.debug("  dispatched batch..")
                elif channel == "email":
                    email_addresses, user_touch_point_states = await asyncio.gather(
                        augment_email_batch_with_email_addresses(itgs, raw_batch),
                        augment_batch_with_user_touch_point_states(
                            itgs, touch_points_set, raw_batch
                        ),
                    )
                    batch = AugmentedEmailBatch(
                        touches=raw_batch,
                        email_addresses=email_addresses,
                        user_touch_point_states=user_touch_point_states,
                    )
                    await self.on_email_batch(batch)
                else:
                    raise AssertionError(f"unknown channel {channel=}")

        async def handle_raw_batch_wrapper(channel: str, raw_batch: List[TouchToSend]):
            try:
                await handle_raw_batch(channel, raw_batch)
            except Exception as exc:
                await handle_error(
                    exc, extra_info=f"error handling batch {channel=} {len(raw_batch)=}"
                )

        async def on_raw_batch(channel: str, raw_batch: List[TouchToSend]):
            logging.debug(f"  augmenting {channel=} batch ({len(raw_batch)=})")

            task = asyncio.create_task(handle_raw_batch_wrapper(channel, raw_batch))
            augmenting.add(task)
            task.add_done_callback(lambda _: augmenting.remove(task))

        batch_former = BatchFormer(on_raw_batch, self.on_stale)
        await batch_former.form_batches(itgs, touch_points_set, purgatory_size, now=now)
        remaining = list(augmenting)
        logging.debug(f"batches formed, waiting for {len(remaining)=} to augment")
        if remaining:
            await asyncio.wait(remaining, return_when=asyncio.ALL_COMPLETED)
        logging.debug(f"all augmented")


async def any_subqueue_backpressure(itgs: Itgs) -> bool:
    """Returns True if any of the subqueues are excessively large"""
    redis = await itgs.redis()

    async with redis.pipeline(transaction=False) as pipe:
        await pipe.llen(b"sms:to_send")  # type: ignore
        await pipe.llen(b"email:to_send")  # type: ignore
        await pipe.llen(b"push:message_attempts:to_send")  # type: ignore
        sms_size, email_size, push_size = await pipe.execute()

    return any(
        s > SUBQUEUE_BACKPRESSURE_THRESHOLD for s in (sms_size, email_size, push_size)
    )


class MyRedisStatUpdatePreparer(RedisStatsPreparer):
    """Helper object for constructing what changes to write to daily stats keys
    in redis, e.g., stats:touch_send_daily:{unix_date}
    """

    def __init__(self) -> None:
        super().__init__()

    def incr_touch_send_stats(
        self, touch: TouchToSend, event: str, *, extra: Optional[str] = None
    ) -> None:
        """Increments the touch send stats event by one, optionally incrementing
        a breakdown stat by the same amount. Generally prefer one of the typed variants,
        e.g., incr_touch_send_attempted
        """
        assert isinstance(event, str)
        assert isinstance(extra, (str, type(None)))
        unix_date = get_touch_unix_date(touch)
        stats = self.get_for_key(touch_send_stats_key(unix_date))
        stats[event.encode("ascii")] = stats.get(event.encode("ascii"), 0) + 1
        self.set_earliest(touch_send_stats_earliest_key, unix_date)

        if extra is not None:
            extra_stats = self.get_for_key(touch_send_stats_extra_key(unix_date, event))
            extra_stats[extra.encode("ascii")] = (
                extra_stats.get(extra.encode("ascii"), 0) + 1
            )

    def incr_touch_send_attempted(self, touch: TouchToSend) -> None:
        """Increments the number of touches attempted to include the given touch"""
        self.incr_touch_send_stats(
            touch, "attempted", extra=f"{touch.touch_point_event_slug}:{touch.channel}"
        )

    def incr_touch_send_reachable(
        self, touch: TouchToSend, num_destinations: int
    ) -> None:
        """Increments the number of reachable touches to include the given touch"""
        self.incr_touch_send_stats(
            touch,
            "reachable",
            extra=f"{touch.touch_point_event_slug}:{touch.channel}:{num_destinations}",
        )

    def incr_touch_send_unreachable(self, touch: TouchToSend) -> None:
        """Increments the number of unreachable touches to include the given touch"""
        self.incr_touch_send_stats(
            touch,
            "unreachable",
            extra=f"{touch.touch_point_event_slug}:{touch.channel}",
        )

    def incr_pushes_queued(self, batch_at: float) -> None:
        """Increments how many push ticket sends have been queued"""
        assert isinstance(batch_at, float)
        unix_date = unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz)
        stats = self.get_for_key(
            f"stats:push_tickets:daily:{unix_date}".encode("ascii")
        )
        stats[b"queued"] = stats.get(b"queued", 0) + 1
        self.set_earliest(b"stats:push_tickets:daily:earliest", unix_date)

    def incr_sms_queued(self, batch_at: float) -> None:
        """Increments how many sms sends have been queued"""
        assert isinstance(batch_at, float)
        unix_date = unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz)
        stats = self.get_for_key(f"stats:sms_send:daily:{unix_date}".encode("ascii"))
        stats[b"queued"] = stats.get(b"queued", 0) + 1
        self.set_earliest(b"stats:sms_send:daily:earliest", unix_date)

    def incr_emails_queued(self, batch_at: float) -> None:
        """increments how many emails have been queued"""
        assert isinstance(batch_at, float)
        unix_date = unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz)
        stats = self.get_for_key(f"stats:email_send:daily:{unix_date}".encode("ascii"))
        stats[b"queued"] = stats.get(b"queued", 0) + 1
        self.set_earliest(b"stats:email_send:daily:earliest", unix_date)


BatchT = TypeVar("BatchT", AugmentedPushBatch, AugmentedSmsBatch, AugmentedEmailBatch)
MessagesT = TypeVar(
    "MessagesT", TouchPointPushMessage, TouchPointSmsMessage, TouchPointEmailMessage
)
MessageContentsT = TypeVar(
    "MessageContentsT", MessageContents, SmsMessageContents, EmailMessageContents
)
MessageContentsLogT = TypeVar(
    "MessageContentsLogT",
    TouchLogUserTouchPushInsertMessage,
    TouchLogUserTouchSMSMessage,
    TouchLogUserTouchEmailMessage,
)


@dataclasses.dataclass
class MessageBatchSegmentStats:
    attempted: int = 0
    reachable: int = 0
    unreachable: int = 0


class MessageBatchSegmentPreparer(
    Generic[BatchT, MessagesT, MessageContentsT, MessageContentsLogT]
):
    @classmethod
    async def prepare_batch_segment(
        cls,
        itgs: Itgs,
        touch_points_set: TouchPointsSet,
        batch: BatchT,
        start_idx: int,
        end_idx: int,
        batch_at: float,
    ) -> "RedisTransaction":
        """Prepares the given batch segment for sending by constructing the redis
        transaction that, when run, increments all the appropriate stats and writes
        to the appropriate subqueue.

        Implicitly, this also has all the other class methods on the
        MessageBatchSegmentPreparer as arguments. Since there are a signficant
        number of them, for developer convenience these arguments are specified
        by subclassing this class and overriding them, rather than providing
        them as function arguments.

        Args:
            itgs (Itgs): the integrations to (re)use
            touch_points_set (TouchPointsSet): the touch points, which should have
                all the touch points used in this batch segment immediately available
            batch (BatchT): the batch to prepare
            start_idx (int): the start index within batch.touches to consider, inclusive
            end_idx (int): the end index within batch.touches to consider, exclusive
            batch_at (float): the canonical time for the batch, used as "now" where
                appropriate (e.g., queueing to the subqueue)

        Returns:
            RedisTransaction: the change in run stats and the function which writes to
                redis within a single transaction (in the "run_with_prep" style, i.e.,
                with scripts)
        """
        stats = MyRedisStatUpdatePreparer()
        messages: List[bytes] = []
        logs: List[bytes] = []
        jobs_to_queue: List[bytes] = []
        pending_zset_adds: Dict[bytes, float] = dict()
        # values to insert into touch:pending
        pending_callbacks: Dict[bytes, Dict[bytes, bytes]] = dict()
        # keys of the form touch:pending:{uid} to the hash values to set them to
        pending_remaining: Dict[bytes, List[bytes]] = dict()
        # keys of the form touch:pending:{uid}:remaining to the set values to set them to
        run_stats = MessageBatchSegmentStats()

        for idx in range(start_idx, end_idx):
            touch = batch.touches[idx]
            destinations = cls.get_destinations(batch, touch)
            state = batch.user_touch_point_states.get(
                (touch.user_sub, touch.touch_point_event_slug)
            )
            touch_point = await touch_points_set.get_immediately(
                touch.touch_point_event_slug
            )

            attempt_uid = create_user_touch_debug_log_uid()
            stats.incr_touch_send_attempted(touch)
            run_stats.attempted += 1
            logs.append(
                TouchLogUserTouchDebugLogInsert(
                    table="user_touch_debug_log",
                    action="insert",
                    fields=TouchLogUserTouchDebugLogInsertFields(
                        uid=attempt_uid,
                        user_sub=touch.user_sub,
                        event=UserTouchDebugLogEventSendAttempt(
                            type="send_attempt",
                            queued_at=touch.queued_at,
                            channel=touch.channel,
                            event=touch.touch_point_event_slug,
                            event_parameters=touch.event_parameters,
                        ),
                        created_at=batch_at,
                    ),
                    queued_at=batch_at,
                )
                .model_dump_json()
                .encode("utf-8")
            )

            if not destinations:
                stats.incr_touch_send_unreachable(touch)
                run_stats.unreachable += 1
                logs.append(
                    TouchLogUserTouchDebugLogInsert(
                        table="user_touch_debug_log",
                        action="insert",
                        fields=TouchLogUserTouchDebugLogInsertFields(
                            uid=create_user_touch_debug_log_uid(),
                            user_sub=touch.user_sub,
                            event=UserTouchDebugLogEventSendUnreachable(
                                type="send_unreachable",
                                parent=attempt_uid,
                            ),
                            created_at=batch_at,
                        ),
                        queued_at=batch_at,
                    )
                    .model_dump_json()
                    .encode("utf-8")
                )
                if touch.failure_callback is not None:
                    jobs_to_queue.append(
                        json.dumps(
                            {
                                "name": touch.failure_callback.name,
                                "kwargs": touch.failure_callback.kwargs,
                                "queued_at": batch_at,
                            }
                        ).encode("utf-8")
                    )
                continue

            message, new_state = cls.select_message(touch, touch_point, state)
            stats.incr_touch_send_reachable(touch, len(destinations))

            try:
                message_contents = cls.format_message_contents(touch, message)
            except KeyError:
                raise Exception(
                    f"failed to form message contents for touch {touch.model_dump_json()}"
                )

            run_stats.reachable += 1
            logs.append(
                TouchLogUserTouchDebugLogInsert(
                    table="user_touch_debug_log",
                    action="insert",
                    fields=TouchLogUserTouchDebugLogInsertFields(
                        uid=create_user_touch_debug_log_uid(),
                        user_sub=touch.user_sub,
                        event=UserTouchDebugLogEventSendReachable(
                            type="send_reachable",
                            parent=attempt_uid,
                            message=cls.message_contents_for_log(message_contents),
                            destinations=destinations,
                        ),
                        created_at=batch_at,
                    ),
                    queued_at=batch_at,
                )
                .model_dump_json()
                .encode("utf-8")
            )

            if new_state is not None:
                if state is None:
                    logs.append(
                        TouchLogUserTouchPointStateInsert(
                            table="user_touch_point_states",
                            action="insert",
                            fields=TouchLogUserTouchPointStateInsertFields(
                                touch_point_uid=touch_point.uid,
                                user_sub=touch.user_sub,
                                channel=touch.channel,
                                state=new_state.state,
                            ),
                            queued_at=batch_at,
                        )
                        .model_dump_json()
                        .encode("utf-8")
                    )
                else:
                    logs.append(
                        TouchLogUserTouchPointStateUpdate(
                            table="user_touch_point_states",
                            action="update",
                            expected_version=state.version,
                            fields=TouchLogUserTouchPointStateUpdateFields(
                                touch_point_uid=touch_point.uid,
                                user_sub=touch.user_sub,
                                channel=touch.channel,
                                state=new_state.state,
                            ),
                            queued_at=batch_at,
                        )
                        .model_dump_json()
                        .encode("utf-8")
                    )

            remaining: List[bytes] = []
            for dest in destinations:
                dest_assigned_uid, dest_message = cls.prepare_message(
                    dest,
                    batch_at,
                    message_contents,
                    attempt_uid,
                    touch.user_sub,
                    touch_point.uid,
                    touch.uid,
                )
                remaining.append(dest_assigned_uid)
                messages.append(dest_message)
                cls.incr_subqueue_queued_stats(stats, batch_at)

            if touch.success_callback is not None or touch.failure_callback is not None:
                pending_zset_adds[touch.uid.encode("utf-8")] = batch_at
                pending_callbacks[f"touch:pending:{touch.uid}".encode("utf-8")] = (
                    TouchPending(
                        success_callback=touch.success_callback,
                        failure_callback=touch.failure_callback,
                    ).as_redis_mapping()
                )
                pending_remaining[
                    f"touch:pending:{touch.uid}:remaining".encode("utf-8")
                ] = remaining

        redis = await itgs.redis()

        async def _prep(force: bool):
            await ensure_set_if_lower_script_exists(redis, force=force)

        async def _func():
            async with redis.pipeline() as pipe:
                pipe.multi()
                await stats.write_earliest(pipe)
                await stats.write_increments(pipe)
                await cls.rpush_messages(pipe, messages)
                if pending_zset_adds:
                    await pipe.zadd(b"touch:pending", mapping=pending_zset_adds)
                for cb_key, cbs in pending_callbacks.items():
                    await pipe.hset(cb_key, mapping=cbs)  # type: ignore
                for rem_key, remaining in pending_remaining.items():
                    await pipe.sadd(rem_key, *remaining)  # type: ignore
                if jobs_to_queue:
                    await pipe.rpush(b"jobs:hot", *jobs_to_queue)  # type: ignore
                await pipe.rpush(b"touch:to_log", *logs)  # type: ignore
                await pipe.execute()

        return RedisTransaction(cls.promote_stats(run_stats), _prep, _func)

    @classmethod
    def get_destinations(cls, batch: BatchT, touch: TouchToSend) -> Optional[List[str]]:
        """Determines the destinations that the given touch should be written to.
        This will already be available in the batch but depends on the channel,
        which is known to the subclass. For example, if the batch is an AugmentedSmsBatch,
        then the destinations are the push tokens for the user, e.g., batch.tokens.get(touch.user_sub)
        """
        raise NotImplementedError()

    @classmethod
    def select_message(
        cls,
        touch: TouchToSend,
        touch_point: TouchPoint,
        state: Optional[UserTouchPointState],
    ) -> Tuple[MessagesT, Optional[UserTouchPointState]]:
        """Determines which message should be send for the given touch, which uses
        the given touch point, and the new user touch point state after.
        Generally, delegates to the appropriate `select_channel_message` helper
        """
        raise NotImplementedError()

    @classmethod
    def format_message_contents(
        cls, touch: TouchToSend, message: MessagesT
    ) -> MessageContentsT:
        """Formats the message contents for the given message and touch"""
        raise NotImplementedError()

    @classmethod
    def message_contents_for_log(cls, message: MessageContentsT) -> MessageContentsLogT:
        """Converts the message contents to the variant used for logging, usually
        this is trivially copying over the fields (i.e., this could be implemented
        by MessageContentsLogT.parse_obj(message.dict()) at the cost of some
        performance)
        """
        raise NotImplementedError()

    @classmethod
    def promote_stats(cls, segment_stats: MessageBatchSegmentStats) -> "RunStats":
        """The stats collected during the prepare_batch_segment are not specific
        to the channel. This needs to make it specific by e.g. converting
        "reachable" into "reachable_push" as appropriate for the channel
        """
        raise NotImplementedError()

    @classmethod
    def prepare_message(
        cls,
        dest: str,
        batch_at: float,
        contents: MessageContentsT,
        attempt_uid: str,
        user_sub: str,
        touch_point_uid: str,
        touch_uid: str,
    ) -> Tuple[bytes, bytes]:
        """Forms the message to write to the subqueue for the given destination,
        returning the uid assigned to the attempt and the message to insert into
        the subsystems to send queue.

        Arguments:
            dest (str): the destination (push token, phone number, or email as appropriate
                for the channel) to send to
            batch_at (float): the canonical time for the batch, used as "now" where
                appropriate (e.g., queueing to the subqueue)
            contents (MessageContentsT): the message contents to send to the destination
            attempt_uid (str): the uid of the send_attempt in user_touch_debug_log, usually
                passed to the failure/success job callbacks so that their log events can
                be tied to the corresponding send_attempt
            user_sub (str): the user we were originally trying to reach when this destination
                was selected, for logging and the write to user_touches
            touch_point_uid (str): the uid of the touch point used to create the message contents,
                for the success callback (as it will generally write to user_touches)
            touch_uid (str): the uid of the touch this message is for, for calling the
                appropriate success/failure callback via the lib.touch.pending module

        Returns:
            bytes: the uid assigned to the attempt, to be included in the touch:pending:{uid}:remaining
                list
            bytes: the value to add to the list ultimately sent to rpush_messages
        """
        raise NotImplementedError()

    @classmethod
    async def rpush_messages(
        cls, pipe: AsyncioRedisClient, messages: List[bytes]
    ) -> None:
        """Writes the given messages constructed via prepare_message to the
        appropriate subqueue, generally via `await pipe.rpush(subqueue, *messages)`
        """
        raise NotImplementedError()

    @classmethod
    def incr_subqueue_queued_stats(
        cls, stats: MyRedisStatUpdatePreparer, batch_at: float
    ) -> None:
        """Increments the number of queued items within the subqueue given
        when the message was queued
        """
        raise NotImplementedError()


class PushBatchSegmentPreparer(
    MessageBatchSegmentPreparer[
        AugmentedPushBatch,
        TouchPointPushMessage,
        MessageContents,
        TouchLogUserTouchPushInsertMessage,
    ]
):
    @classmethod
    def get_destinations(
        cls, batch: AugmentedPushBatch, touch: TouchToSend
    ) -> Optional[List[str]]:
        return batch.tokens.get(touch.user_sub)

    @classmethod
    def select_message(
        cls,
        touch: TouchToSend,
        touch_point: TouchPoint,
        state: Optional[UserTouchPointState],
    ) -> Tuple[TouchPointPushMessage, Optional[UserTouchPointState]]:
        return select_push_message(touch, touch_point, state)

    @classmethod
    def format_message_contents(
        cls, touch: TouchToSend, message: TouchPointPushMessage
    ) -> MessageContents:
        return MessageContents(
            title=message.title_format.format_map(
                dict((k, touch.event_parameters[k]) for k in message.title_parameters)
            ),
            body=message.body_format.format_map(
                dict((k, touch.event_parameters[k]) for k in message.body_parameters)
            ),
            channel_id=message.channel_id,
        )

    @classmethod
    def message_contents_for_log(
        cls, message: MessageContents
    ) -> TouchLogUserTouchPushInsertMessage:
        return TouchLogUserTouchPushInsertMessage(
            title=message.title, body=message.body, channel_id=message.channel_id
        )

    @classmethod
    def promote_stats(cls, segment_stats: MessageBatchSegmentStats) -> "RunStats":
        return RunStats(
            attempted=segment_stats.attempted,
            attempted_push=segment_stats.attempted,
            reachable_push=segment_stats.reachable,
            unreachable_push=segment_stats.unreachable,
        )

    @classmethod
    def prepare_message(
        cls,
        dest: str,
        batch_at: float,
        contents: MessageContents,
        attempt_uid: str,
        user_sub: str,
        touch_point_uid: str,
        touch_uid: str,
    ) -> Tuple[bytes, bytes]:
        message_attempt_uid = create_message_attempt_uid()
        return (
            message_attempt_uid.encode("utf-8"),
            MessageAttemptToSend(
                aud="send",
                uid=message_attempt_uid,
                initially_queued_at=batch_at,
                retry=0,
                last_queued_at=batch_at,
                push_token=dest,
                contents=contents,
                failure_job=JobCallback(
                    name="runners.touch.push_failure_handler",
                    kwargs={
                        "attempt_uid": attempt_uid,
                        "touch_uid": touch_uid,
                        "user_sub": user_sub,
                    },
                ),
                success_job=JobCallback(
                    name="runners.touch.push_success_handler",
                    kwargs={
                        "attempt_uid": attempt_uid,
                        "touch_point_uid": touch_point_uid,
                        "touch_uid": touch_uid,
                        "user_sub": user_sub,
                    },
                ),
            )
            .model_dump_json()
            .encode("utf-8"),
        )

    @classmethod
    async def rpush_messages(
        cls, pipe: AsyncioRedisClient, messages: List[bytes]
    ) -> None:
        if messages:
            await pipe.rpush(b"push:message_attempts:to_send", *messages)  # type: ignore

    @classmethod
    def incr_subqueue_queued_stats(
        cls, stats: MyRedisStatUpdatePreparer, batch_at: float
    ) -> None:
        stats.incr_pushes_queued(batch_at)


class SmsBatchSegmentPreparer(
    MessageBatchSegmentPreparer[
        AugmentedSmsBatch,
        TouchPointSmsMessage,
        SmsMessageContents,
        TouchLogUserTouchSMSMessage,
    ]
):
    @classmethod
    def get_destinations(
        cls, batch: AugmentedSmsBatch, touch: TouchToSend
    ) -> Optional[List[str]]:
        return batch.phone_numbers.get(touch.user_sub)

    @classmethod
    def select_message(
        cls,
        touch: TouchToSend,
        touch_point: TouchPoint,
        state: Optional[UserTouchPointState],
    ) -> Tuple[TouchPointSmsMessage, Optional[UserTouchPointState]]:
        return select_sms_message(touch, touch_point, state)

    @classmethod
    def format_message_contents(
        cls, touch: TouchToSend, message: TouchPointSmsMessage
    ) -> SmsMessageContents:
        return SmsMessageContents(
            body=message.body_format.format_map(
                dict((k, touch.event_parameters[k]) for k in message.body_parameters)
            ),
        )

    @classmethod
    def message_contents_for_log(
        cls, message: SmsMessageContents
    ) -> TouchLogUserTouchSMSMessage:
        return TouchLogUserTouchSMSMessage(body=message.body)

    @classmethod
    def promote_stats(cls, segment_stats: MessageBatchSegmentStats) -> "RunStats":
        return RunStats(
            attempted=segment_stats.attempted,
            attempted_sms=segment_stats.attempted,
            reachable_sms=segment_stats.reachable,
            unreachable_sms=segment_stats.unreachable,
        )

    @classmethod
    def prepare_message(
        cls,
        dest: str,
        batch_at: float,
        contents: SmsMessageContents,
        attempt_uid: str,
        user_sub: str,
        touch_point_uid: str,
        touch_uid: str,
    ) -> Tuple[bytes, bytes]:
        sms_uid = create_sms_uid()
        return (
            sms_uid.encode("utf-8"),
            SMSToSend(
                aud="send",
                uid=sms_uid,
                initially_queued_at=batch_at,
                retry=0,
                last_queued_at=batch_at,
                phone_number=dest,
                body=contents.body,
                failure_job=JobCallback(
                    name="runners.touch.sms_failure_handler",
                    kwargs={
                        "attempt_uid": attempt_uid,
                        "touch_uid": touch_uid,
                        "user_sub": user_sub,
                    },
                ),
                success_job=JobCallback(
                    name="runners.touch.sms_success_handler",
                    kwargs={
                        "attempt_uid": attempt_uid,
                        "touch_point_uid": touch_point_uid,
                        "touch_uid": touch_uid,
                        "user_sub": user_sub,
                    },
                ),
            )
            .model_dump_json()
            .encode("utf-8"),
        )

    @classmethod
    async def rpush_messages(
        cls, pipe: AsyncioRedisClient, messages: List[bytes]
    ) -> None:
        if messages:
            await pipe.rpush(b"sms:to_send", *messages)  # type: ignore

    @classmethod
    def incr_subqueue_queued_stats(
        cls, stats: MyRedisStatUpdatePreparer, batch_at: float
    ) -> None:
        stats.incr_sms_queued(batch_at)


class EmailBatchSegmentPreparer(
    MessageBatchSegmentPreparer[
        AugmentedEmailBatch,
        TouchPointEmailMessage,
        EmailMessageContents,
        TouchLogUserTouchEmailMessage,
    ]
):
    @classmethod
    def get_destinations(
        cls, batch: AugmentedEmailBatch, touch: TouchToSend
    ) -> Optional[List[str]]:
        return batch.email_addresses.get(touch.user_sub)

    @classmethod
    def select_message(
        cls,
        touch: TouchToSend,
        touch_point: TouchPoint,
        state: Optional[UserTouchPointState],
    ) -> Tuple[TouchPointEmailMessage, Optional[UserTouchPointState]]:
        return select_email_message(touch, touch_point, state)

    @classmethod
    def format_message_contents(
        cls, touch: TouchToSend, message: TouchPointEmailMessage
    ) -> EmailMessageContents:
        template_parameters = dict()
        for fixed_key, fixed_item in message.template_parameters_fixed.items():
            template_parameters[fixed_key] = fixed_item
        for substitution in message.template_parameters_substituted:
            key_value = substitution.format.format_map(
                dict((k, touch.event_parameters[k]) for k in substitution.parameters)
            )

            cur = template_parameters
            for idx in range(len(substitution.key) - 1):
                if substitution.key[idx] not in cur:
                    cur[substitution.key[idx]] = dict()
                cur = cur[substitution.key[idx]]
            cur[substitution.key[-1]] = key_value

        return EmailMessageContents(
            subject=message.subject_format.format_map(
                dict((k, touch.event_parameters[k]) for k in message.subject_parameters)
            ),
            template=message.template,
            template_parameters=template_parameters,
        )

    @classmethod
    def message_contents_for_log(
        cls, message: EmailMessageContents
    ) -> TouchLogUserTouchEmailMessage:
        return TouchLogUserTouchEmailMessage(
            subject=message.subject,
            template=message.template,
            template_parameters=message.template_parameters,
        )

    @classmethod
    def promote_stats(cls, segment_stats: MessageBatchSegmentStats) -> "RunStats":
        return RunStats(
            attempted=segment_stats.attempted,
            attempted_email=segment_stats.attempted,
            reachable_email=segment_stats.reachable,
            unreachable_email=segment_stats.unreachable,
        )

    @classmethod
    def prepare_message(
        cls,
        dest: str,
        batch_at: float,
        contents: EmailMessageContents,
        attempt_uid: str,
        user_sub: str,
        touch_point_uid: str,
        touch_uid: str,
    ) -> Tuple[bytes, bytes]:
        email_uid = create_email_uid()
        return (
            email_uid.encode("utf-8"),
            EmailAttempt(
                aud="send",
                uid=email_uid,
                email=dest,
                subject=contents.subject,
                template=contents.template,
                template_parameters=contents.template_parameters,
                initially_queued_at=batch_at,
                retry=0,
                last_queued_at=batch_at,
                failure_job=JobCallback(
                    name="runners.touch.email_failure_handler",
                    kwargs={
                        "attempt_uid": attempt_uid,
                        "touch_uid": touch_uid,
                        "user_sub": user_sub,
                    },
                ),
                success_job=JobCallback(
                    name="runners.touch.email_success_handler",
                    kwargs={
                        "attempt_uid": attempt_uid,
                        "touch_point_uid": touch_point_uid,
                        "touch_uid": touch_uid,
                        "user_sub": user_sub,
                    },
                ),
            )
            .model_dump_json()
            .encode("utf-8"),
        )

    @classmethod
    async def rpush_messages(
        cls, pipe: AsyncioRedisClient, messages: List[bytes]
    ) -> None:
        if messages:
            await pipe.rpush(b"email:to_send", *messages)  # type: ignore

    @classmethod
    def incr_subqueue_queued_stats(
        cls, stats: MyRedisStatUpdatePreparer, batch_at: float
    ) -> None:
        stats.incr_emails_queued(batch_at)


T = TypeVar("T", TouchPointPushMessage, TouchPointSmsMessage, TouchPointEmailMessage)


def select_message(
    touch: TouchToSend,
    touch_point: TouchPoint,
    state: Optional[UserTouchPointState],
    messages: List[T],
) -> Tuple[T, Optional[UserTouchPointState]]:
    """Selects the appropriate message to send for the given touch, using the touch
    points selection strategy, plus the new state for the user touch point after
    this message.
    """
    assert messages, messages
    if touch_point.selection_strategy == "random_with_replacement":
        return random.choice(messages), None
    elif touch_point.selection_strategy == "fixed":
        return _select_message_fixed(touch, touch_point, state, messages)
    else:
        assert (
            touch_point.selection_strategy == "ordered_resettable"
        ), touch_point.selection_strategy
        return _select_message_ordered_resettable(touch, touch_point, state, messages)


def _select_message_fixed(
    touch: TouchToSend,
    touch_point: TouchPoint,
    state: Optional[UserTouchPointState],
    messages: List[T],
) -> Tuple[T, Optional[UserTouchPointState]]:
    assert touch_point.selection_strategy == "fixed", touch_point.selection_strategy

    if state is None:
        seen_set = set()
    else:
        assert isinstance(state.state, list), state.state
        seen_set = set(state.state)

    priority = None
    choices: List[T] = []
    for msg in messages:
        if msg.uid in seen_set:
            continue
        if priority is not None and priority < msg.priority:
            break
        if priority is None:
            priority = msg.priority
        choices.append(msg)

    if not choices:
        return select_message(touch, touch_point, None, messages)

    selected = random.choice(choices)
    seen_set.add(selected.uid)
    return selected, UserTouchPointState(
        state.version + 1 if state is not None else 1, list(seen_set)
    )


def _select_message_ordered_resettable(
    touch: TouchToSend,
    touch_point: TouchPoint,
    state: Optional[UserTouchPointState],
    messages: List[T],
) -> Tuple[T, Optional[UserTouchPointState]]:
    assert (
        touch_point.selection_strategy == "ordered_resettable"
    ), touch_point.selection_strategy
    assert messages, "cannot have empty messages list"

    if state is None:
        last_priority = float("-inf")
        counter = 0
        seen = dict()
    else:
        assert isinstance(
            state.state, UserTouchPointStateStateOrderedResettable
        ), state.state
        last_priority = state.state.last_priority
        counter = state.state.counter
        seen = state.state.seen

    is_requested_reset = not not touch.event_parameters.get("ss_reset", False)
    if not is_requested_reset:
        message = _select_message_ordered_resettable_helper(
            messages, seen, last_priority
        )
        if message is None:
            message = _select_message_ordered_resettable_helper(
                messages, seen, float("-inf")
            )
    else:
        message = _select_message_ordered_resettable_helper(
            messages, seen, float("-inf")
        )

    assert message is not None
    seen = dict(seen)
    seen[message.uid] = counter
    return message, UserTouchPointState(
        version=state.version + 1 if state is not None else 1,
        state=UserTouchPointStateStateOrderedResettable(
            last_priority=message.priority,
            counter=counter + 1,
            seen=seen,
        ),
    )


def _select_message_ordered_resettable_helper(
    messages: List[T], seen: Dict[str, int], priority: Union[int, float]
) -> Optional[T]:
    """Selects the first message in the list according to the following sort:

    - Require that the messages priority is strictly larger than the given priority
    - Prefer lower priority
    - Prefer not in the seen map
    - Prefer lower value in the seen map
    - Prefer lower indices

    This uses a binary search to find the start index to search, and stops as soon
    as a higher priority is found, thus relying on the messages being sorted by
    ascending priority.

    The priority should either be an int, or the special value float('-inf').
    """
    best_choice: Optional[T] = None
    start_index = (
        0
        if priority == float("-inf")
        else adapt_bisect_left(messages, priority, key=lambda x: x.priority)
    )

    for idx in range(start_index, len(messages)):
        message = messages[idx]
        if message.priority <= priority:
            continue
        last_seen = seen.get(message.uid)
        if last_seen is None and message.priority == priority + 1:
            return message
        if best_choice is None:
            best_choice = message
            continue
        if message.priority < best_choice.priority:
            best_choice = message
            continue
        if message.priority > best_choice.priority:
            return best_choice

        best_last_seen = seen.get(best_choice.uid)
        if best_last_seen is None:
            continue
        if last_seen is None:
            best_choice = message
            continue

        if last_seen < best_last_seen:
            best_choice = message
            continue

    return best_choice


def select_push_message(
    touch: TouchToSend,
    touch_point: TouchPoint,
    state: Optional[UserTouchPointState],
) -> Tuple[TouchPointPushMessage, Optional[UserTouchPointState]]:
    """Determines which push message to send for the given touch, using the touch
    point for the options and the state for the selection strategy.
    """
    return select_message(touch, touch_point, state, touch_point.messages.push)


def select_sms_message(
    touch: TouchToSend,
    touch_point: TouchPoint,
    state: Optional[UserTouchPointState],
) -> Tuple[TouchPointSmsMessage, Optional[UserTouchPointState]]:
    """Determines which sms message to send for the given touch, using the touch
    point for the options and the state for the selection strategy.
    """
    return select_message(touch, touch_point, state, touch_point.messages.sms)


def select_email_message(
    touch: TouchToSend,
    touch_point: TouchPoint,
    state: Optional[UserTouchPointState],
) -> Tuple[TouchPointEmailMessage, Optional[UserTouchPointState]]:
    """Determines which email message to send for the given touch, using the touch
    point for the options and the state for the selection strategy.
    """
    return select_message(touch, touch_point, state, touch_point.messages.email)


@dataclasses.dataclass
class RunStats:
    attempted: int = 0
    attempted_sms: int = 0
    reachable_sms: int = 0
    unreachable_sms: int = 0
    attempted_push: int = 0
    reachable_push: int = 0
    unreachable_push: int = 0
    attempted_email: int = 0
    reachable_email: int = 0
    unreachable_email: int = 0
    stale: int = 0

    def add(self, other: "RunStats") -> None:
        self.attempted += other.attempted
        self.attempted_sms += other.attempted_sms
        self.reachable_sms += other.reachable_sms
        self.unreachable_sms += other.unreachable_sms
        self.attempted_push += other.attempted_push
        self.reachable_push += other.reachable_push
        self.unreachable_push += other.unreachable_push
        self.attempted_email += other.attempted_email
        self.reachable_email += other.reachable_email
        self.unreachable_email += other.unreachable_email
        self.stale += other.stale


@dataclasses.dataclass
class RedisTransaction:
    run_stats: RunStats
    prep: Callable[[bool], Awaitable[None]]
    func: Callable[[], Awaitable[None]]


async def report_run_stats(
    itgs: Itgs,
    touch_points_set: TouchPointsSet,
    run_stats: RunStats,
    started_at: float,
    stop_reason: Literal["list_exhausted", "time_exhausted", "backpressure", "signal"],
):
    finished_at = time.time()
    num_touch_points = await touch_points_set.size()
    logging.info(
        f"Touch Send Job Finished:\n"
        f"- Started At: {started_at:.3f}\n"
        f"- Finished At: {finished_at:.3f}\n"
        f"- Running Time: {finished_at - started_at:.3f}\n"
        f"- Stop Reason: {stop_reason}\n"
        f"- Attempted: {run_stats.attempted}\n"
        f"- Touch Points: {num_touch_points}\n"
        f"- Attempted SMS: {run_stats.attempted_sms}\n"
        f"- Reachable SMS: {run_stats.reachable_sms}\n"
        f"- Unreachable SMS: {run_stats.unreachable_sms}\n"
        f"- Attempted Push: {run_stats.attempted_push}\n"
        f"- Reachable Push: {run_stats.reachable_push}\n"
        f"- Unreachable Push: {run_stats.unreachable_push}\n"
        f"- Attempted Email: {run_stats.attempted_email}\n"
        f"- Reachable Email: {run_stats.reachable_email}\n"
        f"- Unreachable Email: {run_stats.unreachable_email}\n"
        f"- Stale: {run_stats.stale}\n"
    )

    if stop_reason == "backpressure":
        try:
            await handle_warning(
                f"{__name__}:backpressure", "touch send job backpressure"
            )
        except:
            logging.warning("failed to send backpressure warning", exc_info=True)

    redis = await itgs.redis()
    await redis.hset(
        b"stats:touch_send:send_job",  # type: ignore
        mapping={
            b"finished_at": finished_at,
            b"running_time": finished_at - started_at,
            b"attempted": run_stats.attempted,
            b"touch_points": num_touch_points,
            b"attempted_sms": run_stats.attempted_sms,
            b"reachable_sms": run_stats.reachable_sms,
            b"unreachable_sms": run_stats.unreachable_sms,
            b"attempted_push": run_stats.attempted_push,
            b"reachable_push": run_stats.reachable_push,
            b"unreachable_push": run_stats.unreachable_push,
            b"attempted_email": run_stats.attempted_email,
            b"reachable_email": run_stats.reachable_email,
            b"unreachable_email": run_stats.unreachable_email,
            b"stale": run_stats.stale,
            b"stop_reason": stop_reason.encode("ascii"),
        },
    )


def get_touch_unix_date(touch: TouchToSend) -> int:
    return unix_dates.unix_timestamp_to_unix_date(touch.queued_at, tz=tz)


def touch_send_stats_key(unix_date: int) -> bytes:
    return f"stats:touch_send:daily:{unix_date}".encode("ascii")


def touch_send_stats_extra_key(unix_date: int, event: str) -> bytes:
    assert isinstance(event, str)
    return f"stats:touch_send:daily:{unix_date}:extra:{event}".encode("ascii")


def create_user_touch_debug_log_uid() -> str:
    return f"oseh_utbl_{secrets.token_urlsafe(16)}"


if sys.version_info >= (3, 10):
    adapt_bisect_left = bisect.bisect_left
else:
    def adapt_bisect_left(arr: List[T], val: T, *, key: Callable[[T], Any]) -> int:
        """bisect.bisect_left 3.10+ has a key argument; if we're not on that, this
        will ponyfill
        """
        return bisect.bisect_left([key(x) for x in arr], key(val))

touch_send_stats_earliest_key = b"stats:touch_send:daily:earliest"

if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.send")

    asyncio.run(main())
