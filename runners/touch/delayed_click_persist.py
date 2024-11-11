"""Touch Delayed Click Persist Job"""

from typing import Awaitable, Callable, Dict, List, Optional, cast
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import time
from dataclasses import dataclass
from lib.basic_redis_lock import basic_redis_lock
from lib.touch.link_info import TouchLinkDelayedClick
from lib.touch.link_stats import LinkStatsPreparer
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.set_if_lower import ensure_set_if_lower_script_exists
from redis_helpers.zshift import ensure_zshift_script_exists, zshift
from redis.asyncio.client import Redis as AsyncioRedisClient
import unix_dates
import pytz
import io

category = JobCategory.LOW_RESOURCE_COST
tz = pytz.timezone("America/Los_Angeles")

JOB_BATCH_SIZE = 1000
"""How many items we process at once. These items are then subbatched before
being written to the database.
"""

REDIS_FETCH_BATCH_SIZE = 12
"""How many items we request from redis at a time; should be small enough to avoid
blocking redis for too long.
"""

REDIS_WRITE_BATCH_SIZE = 12
"""How many items we write to redis at a time; should be small enough to avoid
blocking redis for too long
"""

MAX_JOB_TIME_SECONDS = 50
"""If the job takes longer than this amount of time to finish without exhausting
the overdue delayed clicks, it will stop to allow other jobs to run.
"""

REDELAY_TIME_SECONDS = 600
"""If we have to delay a click again, how long we delay it for."""

DATABASE_WRITE_BATCH_SIZE = 100
"""The maximum number of items to write to the database within a single transaction
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls large batches off the Delayed Link Clicks sorted set, moving them to
    purgatory for processing. The clicks that are still in the persist purgatory
    are returned to the Delayed Link Clicks sorted set, while the remaining are
    divided into subbatches and persisted to the database before being removed
    from purgatory.

    PERF:
        This does a basic sequential step-by-step process, but weaving is possible
        (i.e., once we have 100 items we can begin a subbatch without waiting for
        the remaining 900 items) and may improve performance.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"touch_links:delayed_click_persist_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_links:delayed_clicks_persist_job",  # type: ignore
            b"started_at",  # type: ignore
            str(started_at).encode("ascii"),  # type: ignore
        )

        stats = RunStats()
        stop_reason: Optional[str] = None
        expect_empty_purgatory: bool = True

        while True:
            if gd.received_term_signal:
                logging.warning(
                    "Touch Delayed Click Persist Job received term signal during loop"
                )
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.warning("Touch Delayed Click Persist Job exceeded max time")
                stop_reason = "time_exhausted"
                break

            purgatory_size = cast(
                int, await redis.zcard(b"touch_links:delayed_clicks_purgatory")
            )
            if purgatory_size > 0:
                if not expect_empty_purgatory:
                    raise Exception(
                        "Touch Delayed Click Persist Job failed to empty purgatory during last iteration"
                    )

                await handle_warning(
                    f"{__name__}:recovering",
                    f"Touch Delayed Click Persist Job recovering {purgatory_size=}",
                )

            expect_empty_purgatory = True
            if purgatory_size == 0:
                purgatory_size = await run_with_prep(
                    lambda force: ensure_zshift_script_exists(redis, force=force),
                    lambda: zshift(
                        redis,
                        b"touch_links:delayed_clicks",
                        b"touch_links:delayed_clicks_purgatory",
                        JOB_BATCH_SIZE,
                        started_at,
                    ),
                )
                assert purgatory_size is not None

            if purgatory_size == 0:
                logging.info(
                    "Touch Delayed Click Persist Job reached end of overdue delayed clicks"
                )
                stop_reason = "list_exhausted"
                break

            batch_at = time.time()
            batch_unix_date = unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz)

            logging.debug(
                f"Touch Delayed Click Persist Job processing batch of {purgatory_size} items"
            )

            click_uids_to_persist: List[str] = []
            for start_index in range(0, purgatory_size, REDIS_FETCH_BATCH_SIZE):
                end_index_excl = min(
                    start_index + REDIS_FETCH_BATCH_SIZE, purgatory_size
                )
                result = await redis.zrange(
                    b"touch_links:delayed_clicks_purgatory",
                    start_index,
                    end_index_excl - 1,
                )
                assert len(result) == end_index_excl - start_index
                click_uids_to_persist.extend([c.decode("utf-8") for c in result])

            batch_clicks: List[TouchLinkDelayedClick] = []
            for start_index in range(0, purgatory_size, REDIS_FETCH_BATCH_SIZE):
                end_index_excl = min(
                    start_index + REDIS_FETCH_BATCH_SIZE, purgatory_size
                )
                async with redis.pipeline(transaction=False) as pipe:
                    for click_uid in click_uids_to_persist[start_index:end_index_excl]:
                        await pipe.hgetall(
                            f"touch_links:delayed_clicks:{click_uid}".encode("utf-8")  # type: ignore
                        )
                    result = await pipe.execute()
                assert len(result) == end_index_excl - start_index
                batch_clicks.extend(
                    [TouchLinkDelayedClick.from_redis_mapping(r) for r in result]
                )

            clicks_to_delay: List[TouchLinkDelayedClick] = []
            clicks_to_persist: List[TouchLinkDelayedClick] = []

            for start_index in range(0, purgatory_size, REDIS_FETCH_BATCH_SIZE):
                end_index_excl = min(
                    start_index + REDIS_FETCH_BATCH_SIZE, purgatory_size
                )
                result = await redis.zmscore(
                    b"touch_links:persist_purgatory",
                    [  # type: ignore
                        c.uid.encode("utf-8")
                        for c in batch_clicks[start_index:end_index_excl]
                    ],
                )
                assert len(result) == end_index_excl - start_index

                for inner_idx, score in enumerate(result):
                    if score is not None:
                        clicks_to_delay.append(batch_clicks[start_index + inner_idx])
                    else:
                        clicks_to_persist.append(batch_clicks[start_index + inner_idx])

            if clicks_to_delay:
                await handle_warning(
                    f"{__name__}:delay_bouncing",
                    "Delayed Click Persist Job forced to delay clicks further; persist job is likely failing",
                )

            for start_index in range(0, len(clicks_to_delay), REDIS_WRITE_BATCH_SIZE):
                end_index_excl = min(
                    start_index + REDIS_WRITE_BATCH_SIZE, len(clicks_to_delay)
                )
                transaction = create_delay_clicks_transaction(
                    redis,
                    clicks_to_delay[start_index:end_index_excl],
                    batch_at,
                    batch_unix_date,
                )
                await run_with_prep(transaction.prep, transaction.func)
                stats.add(transaction.run_stats)

            for start_index in range(
                0, len(clicks_to_persist), DATABASE_WRITE_BATCH_SIZE
            ):
                end_index_excl = min(
                    start_index + DATABASE_WRITE_BATCH_SIZE, len(clicks_to_persist)
                )
                subbatch_stats = await persist_clicks(
                    itgs,
                    clicks_to_persist[start_index:end_index_excl],
                    batch_at,
                    batch_unix_date,
                    query_cacheable=(end_index_excl - start_index)
                    == DATABASE_WRITE_BATCH_SIZE,
                )
                stats.add(subbatch_stats)

        if stats.lost > 0:
            await handle_warning(
                f"{__name__}:lost",
                f"Touch Delayed Click Persist Job lost {stats.lost} clicks",
            )

        if stats.duplicate > 0:
            await handle_warning(
                f"{__name__}:duplicate",
                f"Touch Delayed Click Persist Job found {stats.duplicate} duplicate clicks",
            )

        assert stop_reason is not None
        finished_at = time.time()
        logging.info(
            f"Touch Delayed Click Persist Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Attempted: {stats.attempted}\n"
            f"- Persisted: {stats.persisted}\n"
            f"- Delayed: {stats.delayed}\n"
            f"- Lost: {stats.lost}\n"
            f"- Duplicate: {stats.duplicate}\n"
        )

        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_links:delayed_clicks_persist_job",  # type: ignore
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"attempted": stats.attempted,
                b"persisted": stats.persisted,
                b"delayed": stats.delayed,
                b"lost": stats.lost,
                b"duplicate": stats.duplicate,
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


@dataclass
class RunStats:
    attempted: int = 0
    persisted: int = 0
    delayed: int = 0
    lost: int = 0
    duplicate: int = 0

    def add(self, other: "RunStats"):
        self.attempted += other.attempted
        self.persisted += other.persisted
        self.delayed += other.delayed
        self.lost += other.lost
        self.duplicate += other.duplicate


@dataclass
class RedisTransaction:
    run_stats: RunStats
    prep: Callable[[bool], Awaitable[None]]
    func: Callable[[], Awaitable[None]]


def create_delay_clicks_transaction(
    redis: AsyncioRedisClient,
    clicks: List[TouchLinkDelayedClick],
    now: float,
    unix_date: int,
) -> RedisTransaction:
    stats = LinkStatsPreparer()
    stats.incr_touch_links(
        unix_date=unix_date, event="delayed_clicks_attempted", amt=len(clicks)
    )
    stats.incr_touch_links(
        unix_date=unix_date, event="delayed_clicks_delayed", amt=len(clicks)
    )

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await stats.write_earliest(pipe)
            await pipe.zadd(
                b"touch_links:delayed_clicks",
                mapping={
                    c.uid.encode("utf-8"): now + REDELAY_TIME_SECONDS for c in clicks
                },
            )
            await pipe.zrem(
                b"touch_links:delayed_clicks_purgatory",
                *[c.uid.encode("utf-8") for c in clicks],
            )
            await pipe.execute()

    return RedisTransaction(
        run_stats=RunStats(attempted=len(clicks), delayed=len(clicks)),
        prep=prep,
        func=func,
    )


_insert_query_cache: Dict[int, str] = dict()
"""Goes from the number of clicks inserted to the sql query that
tries to insert those clicks
"""


def _create_insert_query(num_clicks: int) -> str:
    assert num_clicks >= 1
    result = io.StringIO()
    result.write(
        "WITH batch(uid, link_code, track_type, parent_uid, user_sub, visitor_uid, clicked_at) AS (VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    for _ in range(num_clicks - 1):
        result.write(", (?, ?, ?, ?, ?, ?, ?)")

    result.write(
        ") INSERT INTO user_touch_link_clicks ("
        " uid,"
        " user_touch_link_id,"
        " track_type,"
        " parent_id,"
        " user_id,"
        " visitor_id,"
        " parent_known,"
        " user_known,"
        " visitor_known,"
        " child_known,"
        " clicked_at,"
        " created_at"
        ") SELECT"
        " batch.uid,"
        " user_touch_links.id,"
        " batch.track_type,"
        " parents.id,"
        " users.id,"
        " visitors.id,"
        " batch.parent_uid IS NOT NULL,"
        " batch.user_sub IS NOT NULL,"
        " batch.visitor_uid IS NOT NULL,"
        " 0,"
        " batch.clicked_at,"
        " ? "
        "FROM batch "
        "JOIN user_touch_links ON user_touch_links.code = batch.link_code "
        "LEFT OUTER JOIN user_touch_link_clicks AS parents ON parents.uid = batch.parent_uid "
        "LEFT OUTER JOIN users ON users.sub = batch.user_sub "
        "LEFT OUTER JOIN visitors ON visitors.uid = batch.visitor_uid "
        "WHERE"
        " NOT EXISTS (SELECT 1 FROM user_touch_link_clicks utlc WHERE utlc.uid = batch.uid)"
        " AND (parents.id IS NULL OR NOT EXISTS (SELECT 1 FROM user_touch_link_clicks utlc WHERE utlc.id = parents.id))"
    )
    return result.getvalue()


def _create_update_query(num_clicks: int) -> str:
    """Creates the update query to set child_known to true for the given number
    of parent clicks
    """
    assert num_clicks >= 1
    result = io.StringIO()
    result.write("WITH batch(parent_uid) AS (VALUES (?)")
    for _ in range(num_clicks - 1):
        result.write(", (?)")

    result.write(
        ") UPDATE user_touch_link_clicks "
        "SET child_known = 1 "
        "FROM batch "
        "WHERE user_touch_link_clicks.uid = batch.parent_uid"
    )
    return result.getvalue()


def _create_count_existing_query(num_clicks: int) -> str:
    """Creates the select query to count how many uids correspond to clicks"""
    assert num_clicks >= 1
    result = io.StringIO()
    result.write("WITH batch(uid) AS (VALUES (?)")
    for _ in range(num_clicks - 1):
        result.write(", (?)")

    result.write(
        ") SELECT COUNT(*) "
        "FROM batch "
        "WHERE"
        " EXISTS (SELECT 1 FROM user_touch_link_clicks utlc WHERE utlc.uid = batch.uid)"
    )
    return result.getvalue()


async def persist_clicks(
    itgs: Itgs,
    clicks: List[TouchLinkDelayedClick],
    now: float,
    unix_date: int,
    *,
    query_cacheable: bool,
) -> RunStats:
    """Persists the given clicks to the database; if any fail to insert,
    distinguishes lost and duplicate failures in a follow-up query, for
    a maximum total of 2 db transactions performed by this function.

    This handles incrementing stats in redis.

    Args:
        itgs (Itgs): the integrations to (re)use
        clicks (list[TouchLinkDelayed]): the clicks to persist
        now (float): the current time
        unix_date (int): the current unix date
        query_cacheable (bool): true if we are likely to see the same number of
            clicks in future batches, false otherwise.

    Returns:
        RunStats: the stats for this subbatch
    """
    insert_query = _insert_query_cache.get(len(clicks))
    if insert_query is None:
        insert_query = _create_insert_query(len(clicks))
        if query_cacheable:
            _insert_query_cache[len(clicks)] = insert_query

    num_children = sum(c.parent_uid is not None for c in clicks)
    update_query = _create_update_query(num_children) if num_children > 0 else None

    insert_params = []
    for c in clicks:
        insert_params.extend(
            [
                c.uid,
                c.link_code,
                c.track_type,
                c.parent_uid,
                c.user_sub,
                c.visitor_uid,
                c.clicked_at,
            ]
        )
    insert_params.append(now)

    update_params = [c.parent_uid for c in clicks if c.parent_uid is not None]

    conn = await itgs.conn()
    cursor = conn.cursor()
    response = await cursor.executemany3(
        (
            (
                insert_query,
                insert_params,
            ),
            *([] if update_query is None else [(update_query, update_params)]),
        )
    )
    num_persisted = response[0].rows_affected
    if num_persisted is None:
        num_persisted = 0

    if num_persisted == len(clicks):
        stats = LinkStatsPreparer()
        stats.incr_touch_links(
            unix_date=unix_date, event="delayed_clicks_attempted", amt=len(clicks)
        )
        for click in clicks:
            stats.incr_touch_links(
                unix_date=unix_date,
                event="delayed_clicks_persisted",
                event_extra=f"{click.track_type}:vis={click.visitor_uid is not None}:user={click.user_sub is not None}".encode(
                    "utf-8"
                ),
            )
        await stats.store(itgs)
        return RunStats(attempted=len(clicks), persisted=len(clicks))

    existing_query = _create_count_existing_query(len(clicks))
    existing_params = [c.uid for c in clicks]

    response = await cursor.execute(existing_query, existing_params)
    assert response.results, response
    num_existing = response.results[0][0]
    num_duplicates = max(num_existing - num_persisted, 0)
    num_lost = len(clicks) - num_persisted - num_duplicates

    await (
        LinkStatsPreparer()
        .incr_touch_links(
            unix_date=unix_date, event="delayed_clicks_attempted", amt=len(clicks)
        )
        .incr_touch_links(
            unix_date=unix_date,
            event="delayed_clicks_persisted",
            event_extra=b"in_failed_batch",
            amt=num_persisted,
        )
        .incr_touch_links(
            unix_date=unix_date,
            event="delayed_clicks_failed",
            event_extra=b"lost",
            amt=num_lost,
        )
        .incr_touch_links(
            unix_date=unix_date,
            event="delayed_clicks_failed",
            event_extra=b"duplicate",
            amt=num_duplicates,
        )
        .store(itgs)
    )
    return RunStats(
        attempted=len(clicks),
        persisted=num_persisted,
        lost=num_lost,
        duplicate=num_duplicates,
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.delayed_click_persist")

    asyncio.run(main())
