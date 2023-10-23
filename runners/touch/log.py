"""Touch system log job"""
import io
import json
import secrets
from typing import List, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import time
from lib.basic_redis_lock import basic_redis_lock
import dataclasses
from lib.touch.touch_info import (
    TouchLog,
    TouchLogUserPushTokenUpdate,
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserTouchInsert,
    TouchLogUserTouchPointStateInsert,
    TouchLogUserTouchPointStateUpdate,
    touch_log_parse_raw,
)

from redis_helpers.lmove_many import ensure_lmove_many_script_exists, lmove_many
from redis_helpers.run_with_prep import run_with_prep

category = JobCategory.LOW_RESOURCE_COST

JOB_BATCH_SIZE = 1000
"""How many items we process at once. These items are then grouped by
operation and table and subbatched
"""

REDIS_FETCH_BATCH_SIZE = 12
"""How many items we request from redis at a time; should be small enough to avoid
blocking redis for too long.
"""

MAX_JOB_TIME_SECONDS = 50
"""If the job takes longer than this amount of time to finish without exhausting
the to_log queue, it will stop to allow other jobs to run.
"""

DATABASE_WRITE_BATCH_SIZE = 100
"""The maximum number of items to write to the database within a single transaction
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls messages in large batches off the to log queue and efficiently
    writes them to the database. Not all of these messages are logs necessarily,
    but they are all things that need to be written within the success path of
    a touch, and hence which benefit from batching with even a modest number of
    users.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(itgs, b"touch:log_job:lock", gd=gd, spin=False):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_log:log_job", b"started_at", str(started_at).encode("ascii")
        )

        stats = RunStats()
        job_batch_handler = JobBatchHandler()

        await job_batch_handler.get_first_batch(itgs)
        stop_reason: Optional[str] = None

        while True:
            if gd.received_term_signal:
                logging.warning("Touch Log Job - received signal, stopping")
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.warning("Touch Log Job - time exhausted, stopping")
                stop_reason = "time_exhausted"
                break

            if job_batch_handler.purgatory_size is None:
                await job_batch_handler.get_next_batch(itgs)

            if job_batch_handler.purgatory_size == 0:
                logging.info("Touch Log Job - list exhausted, stopping")
                stop_reason = "list_exhausted"
                break

            user_touch_point_state_updates: List[TouchLogUserTouchPointStateUpdate] = []
            user_touch_point_state_inserts: List[TouchLogUserTouchPointStateInsert] = []
            user_touch_inserts: List[TouchLogUserTouchInsert] = []
            user_touch_debug_log_inserts: List[TouchLogUserTouchDebugLogInsert] = []
            user_push_token_updates: List[TouchLogUserPushTokenUpdate] = []

            reader = PurgatoryReader(job_batch_handler.purgatory_size)
            while reader.have_more and not gd.received_term_signal:
                batch = await reader.read_next_batch(itgs)
                for itm in batch:
                    if isinstance(itm, TouchLogUserTouchPointStateUpdate):
                        user_touch_point_state_updates.append(itm)
                    elif isinstance(itm, TouchLogUserTouchPointStateInsert):
                        user_touch_point_state_inserts.append(itm)
                    elif isinstance(itm, TouchLogUserTouchInsert):
                        user_touch_inserts.append(itm)
                    elif isinstance(itm, TouchLogUserTouchDebugLogInsert):
                        user_touch_debug_log_inserts.append(itm)
                    elif isinstance(itm, TouchLogUserPushTokenUpdate):
                        user_push_token_updates.append(itm)
                    else:
                        raise AssertionError(
                            f"unknown touch log type {type(itm)=} {itm=}"
                        )

            if gd.received_term_signal:
                continue

            now = time.time()
            # writes are ultimately sequential anyway, so there is no point
            # in parallelizing them
            await write_user_touch_point_state_updates(
                itgs, stats, user_touch_point_state_updates, now
            )
            await write_user_touch_point_state_inserts(
                itgs, stats, user_touch_point_state_inserts, now
            )
            await write_user_touch_inserts(itgs, stats, user_touch_inserts, now)
            await write_user_touch_debug_log_inserts(
                itgs, stats, user_touch_debug_log_inserts, now
            )
            await write_user_push_token_updates(
                itgs, stats, user_push_token_updates, now
            )

            await job_batch_handler.clear_purgatory(itgs)

        assert stop_reason is not None
        finished_at = time.time()
        logging.info(
            f"Touch Log Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Inserts: {stats.inserts}\n"
            f"- Updates: {stats.updates}\n"
            f"- Full Batch Inserts: {stats.full_batch_inserts}\n"
            f"- Full Batch Updates: {stats.full_batch_updates}\n"
            f"- Partial Batch Inserts: {stats.partial_batch_inserts}\n"
            f"- Partial Batch Updates: {stats.partial_batch_updates}\n"
            f"- Accepted Inserts: {stats.accepted_inserts}\n"
            f"- Accepted Updates: {stats.accepted_updates}\n"
            f"- Failed Inserts: {stats.failed_inserts}\n"
            f"- Failed Updates: {stats.failed_updates}\n"
        )

        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_log:log_job",
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"inserts": stats.inserts,
                b"updates": stats.updates,
                b"full_batch_inserts": stats.full_batch_inserts,
                b"full_batch_updates": stats.full_batch_updates,
                b"partial_batch_inserts": stats.partial_batch_inserts,
                b"partial_batch_updates": stats.partial_batch_updates,
                b"accepted_inserts": stats.accepted_inserts,
                b"accepted_updates": stats.accepted_updates,
                b"failed_inserts": stats.failed_inserts,
                b"failed_updates": stats.failed_updates,
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


async def write_user_touch_point_state_updates(
    itgs: Itgs,
    stats: "RunStats",
    rows: List[TouchLogUserTouchPointStateUpdate],
    now: float,
):
    conn = await itgs.conn()
    cursor = conn.cursor()
    full_batch_query: Optional[str] = None

    for start_idx in range(0, len(rows), DATABASE_WRITE_BATCH_SIZE):
        end_idx = min(start_idx + DATABASE_WRITE_BATCH_SIZE, len(rows))
        is_full_batch = (end_idx - start_idx) == DATABASE_WRITE_BATCH_SIZE
        assert end_idx - start_idx > 0

        if not is_full_batch or full_batch_query is None:
            query_sql = io.StringIO()
            query_sql.write(
                "WITH batch(expected_version, user_sub, touch_point_uid, channel, state) AS (VALUES "
            )
            for idx in range(start_idx, end_idx):
                if idx > start_idx:
                    query_sql.write(", ")
                query_sql.write("(?, ?, ?, ?, ?)")
            query_sql.write(
                ") UPDATE user_touch_point_states "
                "SET"
                " state=batch.state,"
                " version=batch.expected_version + 1,"
                " updated_at=? "
                "FROM batch "
                "JOIN users ON users.sub = batch.user_sub "
                "JOIN touch_points ON touch_points.uid = batch.touch_point_uid "
                "WHERE"
                " user_touch_point_states.user_id = users.id"
                " AND user_touch_point_states.touch_point_id = touch_points.id"
                " AND user_touch_point_states.channel = batch.channel"
                " AND user_touch_point_states.version = batch.expected_version"
            )
            query = query_sql.getvalue()
            if is_full_batch:
                full_batch_query = query
        else:
            query = full_batch_query

        params = []
        for idx in range(start_idx, end_idx):
            itm = rows[idx]
            params.append(itm.expected_version)
            params.append(itm.fields.user_sub)
            params.append(itm.fields.touch_point_uid)
            params.append(itm.fields.channel)
            params.append(json.dumps(itm.fields.state))
        params.append(now)

        response = await cursor.execute(query, params)
        rows_affected = 0 if response.rows_affected is None else response.rows_affected
        stats.updates += end_idx - start_idx
        if is_full_batch:
            stats.full_batch_updates += 1
        else:
            stats.partial_batch_updates += 1
        stats.accepted_updates += rows_affected
        stats.failed_updates += (end_idx - start_idx) - rows_affected


async def write_user_touch_point_state_inserts(
    itgs: Itgs,
    stats: "RunStats",
    rows: List[TouchLogUserTouchPointStateInsert],
    now: float,
):
    conn = await itgs.conn()
    cursor = conn.cursor()
    full_batch_query: Optional[str] = None

    for start_idx in range(0, len(rows), DATABASE_WRITE_BATCH_SIZE):
        end_idx = min(start_idx + DATABASE_WRITE_BATCH_SIZE, len(rows))
        is_full_batch = (end_idx - start_idx) == DATABASE_WRITE_BATCH_SIZE
        assert end_idx - start_idx > 0

        if not is_full_batch or full_batch_query is None:
            query_sql = io.StringIO()
            query_sql.write(
                "WITH batch(uid, touch_point_uid, user_sub, channel, state) AS (VALUES "
            )
            for idx in range(start_idx, end_idx):
                if idx > start_idx:
                    query_sql.write(", ")
                query_sql.write("(?, ?, ?, ?, ?)")
            query_sql.write(
                ") INSERT INTO user_touch_point_states ("
                " uid,"
                " user_id,"
                " touch_point_id,"
                " channel,"
                " state,"
                " version,"
                " created_at,"
                " updated_at"
                ") SELECT"
                " batch.uid,"
                " users.id,"
                " touch_points.id,"
                " batch.channel,"
                " batch.state,"
                " 1,"
                " ?,"
                " ? "
                "FROM batch "
                "JOIN users ON users.sub = batch.user_sub "
                "JOIN touch_points ON touch_points.uid = batch.touch_point_uid "
                "WHERE NOT EXISTS ("
                " SELECT 1 FROM user_touch_point_states utps"
                " WHERE utps.user_id = users.id"
                " AND utps.touch_point_id = touch_points.id"
                " AND utps.channel = batch.channel"
                ")"
            )
            query = query_sql.getvalue()
            if is_full_batch:
                full_batch_query = query
        else:
            query = full_batch_query

        params = []
        for idx in range(start_idx, end_idx):
            itm = rows[idx]
            params.append(f"oseh_utps_{secrets.token_urlsafe(16)}")
            params.append(itm.fields.touch_point_uid)
            params.append(itm.fields.user_sub)
            params.append(itm.fields.channel)
            params.append(json.dumps(itm.fields.state))
        params.append(now)
        params.append(now)

        response = await cursor.execute(query, params)

        rows_affected = 0 if response.rows_affected is None else response.rows_affected
        stats.inserts += end_idx - start_idx
        if is_full_batch:
            stats.full_batch_inserts += 1
        else:
            stats.partial_batch_inserts += 1
        stats.accepted_inserts += rows_affected
        stats.failed_inserts += (end_idx - start_idx) - rows_affected


async def write_user_touch_inserts(
    itgs: Itgs,
    stats: "RunStats",
    rows: List[TouchLogUserTouchInsert],
    now: float,
):
    conn = await itgs.conn()
    cursor = conn.cursor()
    full_batch_query: Optional[str] = None

    for start_idx in range(0, len(rows), DATABASE_WRITE_BATCH_SIZE):
        end_idx = min(start_idx + DATABASE_WRITE_BATCH_SIZE, len(rows))
        is_full_batch = (end_idx - start_idx) == DATABASE_WRITE_BATCH_SIZE
        assert end_idx - start_idx > 0

        if not is_full_batch or full_batch_query is None:
            query_sql = io.StringIO()
            query_sql.write(
                "WITH batch(uid, send_uid, user_sub, channel, touch_point_uid, destination, message, created_at) AS (VALUES "
            )
            for idx in range(start_idx, end_idx):
                if idx > start_idx:
                    query_sql.write(", ")
                query_sql.write("(?, ?, ?, ?, ?, ?, ?, ?)")
            query_sql.write(
                ") INSERT INTO user_touches ("
                " uid,"
                " send_uid,"
                " user_id,"
                " channel,"
                " touch_point_id,"
                " destination,"
                " message,"
                " created_at"
                ") SELECT"
                " batch.uid,"
                " batch.send_uid,"
                " users.id,"
                " batch.channel,"
                " touch_points.id,"
                " batch.destination,"
                " batch.message,"
                " batch.created_at "
                "FROM batch "
                "JOIN users ON users.sub = batch.user_sub "
                "JOIN touch_points ON touch_points.uid = batch.touch_point_uid"
            )
            query = query_sql.getvalue()
            if is_full_batch:
                full_batch_query = query
        else:
            query = full_batch_query

        params = []
        for idx in range(start_idx, end_idx):
            itm = rows[idx]
            params.append(f"oseh_tch_r_{secrets.token_urlsafe(16)}")
            params.append(itm.fields.uid)
            params.append(itm.fields.user_sub)
            params.append(itm.fields.channel)
            params.append(itm.fields.touch_point_uid)
            params.append(itm.fields.destination)
            params.append(itm.fields.message.json())
            params.append(itm.fields.created_at)

        response = await cursor.execute(query, params)

        rows_affected = 0 if response.rows_affected is None else response.rows_affected
        stats.inserts += end_idx - start_idx
        if is_full_batch:
            stats.full_batch_inserts += 1
        else:
            stats.partial_batch_inserts += 1
        stats.accepted_inserts += rows_affected
        stats.failed_inserts += (end_idx - start_idx) - rows_affected


async def write_user_touch_debug_log_inserts(
    itgs: Itgs,
    stats: "RunStats",
    rows: List[TouchLogUserTouchDebugLogInsert],
    now: float,
):
    conn = await itgs.conn()
    cursor = conn.cursor()
    full_batch_query: Optional[str] = None

    for start_idx in range(0, len(rows), DATABASE_WRITE_BATCH_SIZE):
        end_idx = min(start_idx + DATABASE_WRITE_BATCH_SIZE, len(rows))
        is_full_batch = (end_idx - start_idx) == DATABASE_WRITE_BATCH_SIZE
        assert end_idx - start_idx > 0

        if not is_full_batch or full_batch_query is None:
            query_sql = io.StringIO()
            query_sql.write("WITH batch(uid, user_sub, event, created_at) AS (VALUES ")
            for idx in range(start_idx, end_idx):
                if idx > start_idx:
                    query_sql.write(", ")
                query_sql.write("(?, ?, ?, ?)")
            query_sql.write(
                ") INSERT INTO user_touch_debug_log ("
                " uid, user_id, event, created_at"
                ") SELECT"
                " batch.uid, users.id, batch.event, batch.created_at "
                "FROM batch "
                "JOIN users ON users.sub = batch.user_sub"
            )
            query = query_sql.getvalue()
            if is_full_batch:
                full_batch_query = query
        else:
            query = full_batch_query

        params = []
        for idx in range(start_idx, end_idx):
            itm = rows[idx]
            params.append(f"oseh_utbl_{secrets.token_urlsafe(16)}")
            params.append(itm.fields.user_sub)
            params.append(itm.fields.event.json())
            params.append(itm.fields.created_at)

        response = await cursor.execute(query, params)

        rows_affected = 0 if response.rows_affected is None else response.rows_affected
        stats.inserts += end_idx - start_idx
        if is_full_batch:
            stats.full_batch_inserts += 1
        else:
            stats.partial_batch_inserts += 1
        stats.accepted_inserts += rows_affected
        stats.failed_inserts += (end_idx - start_idx) - rows_affected


async def write_user_push_token_updates(
    itgs: Itgs,
    stats: "RunStats",
    rows: List[TouchLogUserPushTokenUpdate],
    now: float,
):
    conn = await itgs.conn()
    cursor = conn.cursor()
    full_batch_query: Optional[str] = None

    for start_idx in range(0, len(rows), DATABASE_WRITE_BATCH_SIZE):
        end_idx = min(start_idx + DATABASE_WRITE_BATCH_SIZE, len(rows))
        is_full_batch = (end_idx - start_idx) == DATABASE_WRITE_BATCH_SIZE
        assert end_idx - start_idx > 0

        if not is_full_batch or full_batch_query is None:
            query_sql = io.StringIO()
            query_sql.write("WITH batch(token, last_confirmed_at) AS (VALUES ")
            for idx in range(start_idx, end_idx):
                if idx > start_idx:
                    query_sql.write(", ")
                query_sql.write("(?, ?)")
            query_sql.write(
                ") UPDATE user_push_tokens "
                "SET"
                "  last_confirmed_at=batch.last_confirmed_at "
                "FROM batch "
                "WHERE"
                "  user_push_tokens.token = batch.token"
            )
            query = query_sql.getvalue()
            if is_full_batch:
                full_batch_query = query
        else:
            query = full_batch_query

        params = []
        for idx in range(start_idx, end_idx):
            itm = rows[idx]
            params.append(itm.fields.token)
            params.append(itm.fields.last_confirmed_at)

        response = await cursor.execute(query, params)

        rows_affected = 0 if response.rows_affected is None else response.rows_affected
        stats.updates += end_idx - start_idx
        if is_full_batch:
            stats.full_batch_updates += 1
        else:
            stats.partial_batch_updates += 1
        stats.accepted_updates += rows_affected
        stats.failed_updates += (end_idx - start_idx) - rows_affected


@dataclasses.dataclass
class RunStats:
    inserts: int = 0
    updates: int = 0
    full_batch_inserts: int = 0
    full_batch_updates: int = 0
    partial_batch_inserts: int = 0
    partial_batch_updates: int = 0
    accepted_inserts: int = 0
    accepted_updates: int = 0
    failed_inserts: int = 0
    failed_updates: int = 0


class JobBatchHandler:
    """Handles the purgatory queue, i.e., resuming a previously failed run
    and moving data from the larger to_log queue into the purgatory queue
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
        to move JOB_BATCH_SIZE items from the to_log queue into the purgatory
        queue to be used as the first batch.

        Returns:
            True if we found items for the first batch, False if there is nothing
            to do.
        """
        assert self.purgatory_size is None, "already have a batch"
        redis = await itgs.redis()
        initial_size = await redis.llen(b"touch:log_purgatory")
        if initial_size > 0:
            logging.warning(
                f"Touch Log Job - Resuming failed previous run, have {initial_size} items in purgatory"
            )
            slack = await itgs.slack()
            await slack.send_web_error_message(
                f"*Touch Log Job* - recovering {initial_size=} touches from purgatory",
                "Touch Log Job - recovering from purgatory",
            )
            self.purgatory_size = initial_size
            return True

        return await self.get_next_batch(itgs)

    async def get_next_batch(self, itgs: Itgs):
        """Moves a batch from the to_log queue into the purgatory queue, returning
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
                redis, b"touch:to_log", b"touch:log_purgatory", JOB_BATCH_SIZE
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
        await redis.delete(b"touch:log_purgatory")
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

    async def read_next_batch(self, itgs: Itgs) -> List[TouchLog]:
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
            b"touch:log_purgatory",
            self.index,
            end_index_inclusive,
        )

        self.index = end_index_inclusive + 1
        return [touch_log_parse_raw(item) for item in result]

    async def reset(self, purgatory_size: int) -> None:
        """Resets the reader to the beginning of the purgatory queue"""
        self.purgatory_size = purgatory_size
        self.index = 0


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.log")

    asyncio.run(main())
