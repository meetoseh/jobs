"""Touch Persist Link Job"""
import json
from typing import Awaitable, Callable, List, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import dataclasses
import time
from lib.basic_redis_lock import basic_redis_lock
from lib.touch.link_stats import LinkStatsPreparer
from redis_helpers.touch_persist_cleanup import (
    ensure_touch_persist_cleanup_script_exists,
    touch_persist_cleanup,
)
from redis_helpers.touch_read_to_persist import (
    TouchReadToPersistResult,
    ensure_touch_read_to_persist_script_exists,
    touch_read_to_persist,
)
from redis_helpers.zshift import ensure_zshift_script_exists, zshift
from redis_helpers.run_with_prep import run_with_prep
import socket
import pytz
import unix_dates
import io

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""The maximum time the job runs in seconds before stopping itself to allow other
jobs to run
"""

JOB_BATCH_SIZE = 400
"""How many items we process at once
"""

REDIS_FETCH_BATCH_SIZE = 12
"""How many items we request from redis at a time; should be small enough to avoid
blocking redis for too long.
"""

REDIS_DELETE_BATCH_SIZE = 20
"""How many items we delete from redis at a time; should be small enough to avoid
blocking redis for too long.
"""

DATABASE_WRITE_BATCH_SIZE = 100
"""The maximum number of items to write to the database within a single transaction.
We may add slightly more items to avoid splitting a touch and its clicks across batches
"""


tz = pytz.timezone("America/Los_Angeles")


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls batches of messages off the persistable buffered link sorted set, moves
    them to purgatory for processing, then persists them to the database. Finally,
    deletes them from purgatory.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"touch_links:persist_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_links:persist_link_job",
            b"started_at",
            str(started_at).encode("ascii"),
        )

        stats = RunStats()
        is_first_loop = True
        is_last_loop = False
        stop_reason: Optional[str] = None

        while True:
            if gd.received_term_signal:
                logging.warning("Touch Persist Link job received signal; exiting early")
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.warning(
                    "Touch Persist Link job reached max run time; exiting early"
                )
                stop_reason = "time_exhausted"
                break

            # Because we use a zrem strategy for purgatory we check it every loop
            # rather than assuming it's empty, as it is actually possible to detect
            # non-trivial issues this way.
            purgatory_size = await redis.zcard(b"touch_links:persist_purgatory")
            if is_first_loop:
                is_first_loop = False
                if purgatory_size > 0:
                    slack = await itgs.slack()
                    await slack.send_web_error_message(
                        f"{socket.gethostname()} Touch Persist Link job recovering {purgatory_size} items from purgatory",
                        preview="Touch Persist Link job recovering",
                    )
            else:
                if purgatory_size > 0:
                    raise Exception(
                        f"Touch Persist Link Job did not exhaust purgatory when executing subbatches"
                    )

            if purgatory_size == 0:
                purgatory_size = await run_with_prep(
                    lambda force: ensure_zshift_script_exists(redis, force=force),
                    lambda: zshift(
                        redis,
                        b"touch_links:to_persist",
                        b"touch_links:persist_purgatory",
                        JOB_BATCH_SIZE,
                    ),
                )
                is_last_loop = purgatory_size < JOB_BATCH_SIZE

            if purgatory_size == 0:
                logging.info("Touch Persist Link found no more items to persist")
                stop_reason = "list_exhausted"
                break

            batch: List[TouchReadToPersistResult] = []

            for start_idx in range(0, purgatory_size, REDIS_FETCH_BATCH_SIZE):
                end_idx_excl = min(start_idx + REDIS_FETCH_BATCH_SIZE, purgatory_size)
                batch.extend(
                    await run_with_prep(
                        lambda force: ensure_touch_read_to_persist_script_exists(
                            redis, force=force
                        ),
                        lambda: touch_read_to_persist(
                            redis,
                            b"touch_links:persist_purgatory",
                            b"touch_links:buffer",
                            start_idx,
                            end_idx_excl - 1,
                        ),
                    )
                )

            assert len(batch) == purgatory_size

            subbatches: List[Batch] = []
            subbatch_sizes: List[int] = []

            current_subbatch: List[TouchReadToPersistResult] = []
            current_subbatch_num_rows: int = 0

            batch_at = time.time()

            for touch in batch:
                current_subbatch.append(touch)
                current_subbatch_num_rows += count_rows_for_touch(touch)

                if current_subbatch_num_rows >= DATABASE_WRITE_BATCH_SIZE:
                    subbatches.append(
                        await form_batch(itgs, current_subbatch, batch_at=batch_at)
                    )
                    subbatch_sizes.append(current_subbatch_num_rows)
                    current_subbatch = []
                    current_subbatch_num_rows = 0

            if current_subbatch:
                subbatches.append(
                    await form_batch(itgs, current_subbatch, batch_at=batch_at)
                )
                subbatch_sizes.append(current_subbatch_num_rows)

            logging.debug(
                f"Touch Persist Link Job formed batch of {purgatory_size} items into "
                f"{len(subbatches)} subbatches with sizes {subbatch_sizes}"
            )

            for subbatch in subbatches:
                subbatch_stats = await execute_batch(itgs, subbatch, batch_at=batch_at)
                stats.add(subbatch_stats)

            if is_last_loop:
                logging.info("Touch Persist Link job reached end of persistable items")
                stop_reason = "list_exhausted"
                break

        assert stop_reason is not None
        finished_at = time.time()
        logging.info(
            f"Touch Persist Link Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Attempted: {stats.attempted}\n"
            f"- Lost: {stats.lost}\n"
            f"- Integrity Error: {stats.integrity_error}\n"
            f"- Persisted: {stats.persisted}\n"
            f"- Persisted Without Clicks: {stats.persisted_without_clicks}\n"
            f"- Persisted With One Click: {stats.persisted_with_one_click}\n"
            f"- Persisted With Multiple Clicks: {stats.persisted_with_multiple_clicks}\n"
        )

        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_links:persist_link_job",
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"attempted": stats.attempted,
                b"lost": stats.lost,
                b"integrity_error": stats.integrity_error,
                b"persisted": stats.persisted,
                b"persisted_without_clicks": stats.persisted_without_clicks,
                b"persisted_with_one_click": stats.persisted_with_one_click,
                b"persisted_with_multiple_clicks": stats.persisted_with_multiple_clicks,
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


@dataclasses.dataclass
class RunStats:
    attempted: int = 0
    lost: int = 0
    integrity_error: int = 0
    persisted: int = 0
    persisted_without_clicks: int = 0
    persisted_with_one_click: int = 0
    persisted_with_multiple_clicks: int = 0

    def add(self, other: "RunStats"):
        self.attempted += other.attempted
        self.lost += other.lost
        self.integrity_error += other.integrity_error
        self.persisted += other.persisted

        if self.persisted_without_clicks == -1 or other.persisted_without_clicks == -1:
            self.persisted_without_clicks = -1
        else:
            self.persisted_without_clicks += other.persisted_without_clicks

        if self.persisted_with_one_click == -1 or other.persisted_with_one_click == -1:
            self.persisted_with_one_click = -1
        else:
            self.persisted_with_one_click += other.persisted_with_one_click

        if (
            self.persisted_with_multiple_clicks == -1
            or other.persisted_with_multiple_clicks == -1
        ):
            self.persisted_with_multiple_clicks = -1
        else:
            self.persisted_with_multiple_clicks += other.persisted_with_multiple_clicks


@dataclasses.dataclass
class SqlQuery:
    query: str
    params: list


@dataclasses.dataclass
class RedisTransaction:
    prep: Callable[[bool], Awaitable[None]]
    func: Callable[[], Awaitable[None]]


@dataclasses.dataclass
class Batch:
    """Describes a batch"""

    lost: int
    """how many links were skipped because the data was None"""

    persisting: List[TouchReadToPersistResult]
    """The touches we are actually trying to persist"""

    links: Optional[SqlQuery]
    """The sql query to persist the links we do want to persist. If persisting
    is empty, this is None
    """

    clicks: Optional[SqlQuery]
    """the sql query to persist clicks related to the links; None if there are no
    clicks to persist
    """

    num_clicks_to_persist: int
    """the total number of clicks we are trying to persist"""

    redis_cleanup: List[RedisTransaction]
    """The redis transactions that can be executed with run_with_prep to remove
    the items in this batch from:
    - touch_links:persist_purgatory
    - touch_links:buffer
    - touch_links:buffer:{code}
    - touch_links:buffer:clicks:{code}
    - touch_links:buffer:on_clicks_by_uid:{uid}
    This is broken into multiple transactions each deleting at most
    REDIS_DELETE_BATCH_SIZE items
    """


async def form_batch(
    itgs: Itgs, batch: List[TouchReadToPersistResult], batch_at: float
) -> Batch:
    """Prepares a batch of links to be persisted. Should be executed
    with execute_batch
    """

    links_sql = io.StringIO()
    links_sql.write(
        "WITH batch("
        " uid,"
        " user_touch_uid,"
        " code,"
        " page_identifier,"
        " page_extra,"
        " preview_identifier,"
        " preview_extra"
        ") AS (VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

    lost = 0
    persisting: List[TouchReadToPersistResult] = []
    link_params = []

    for item in batch:
        if item.data is None:
            lost += 1
            continue

        persisting.append(item)
        link_params.extend(
            [
                item.data.uid,
                item.data.touch_uid,
                item.data.code,
                item.data.page_identifier,
                json.dumps(item.data.page_extra, sort_keys=True),
                item.data.preview_identifier,
                json.dumps(item.data.preview_extra, sort_keys=True),
            ]
        )
        if len(persisting) > 1:
            links_sql.write(", (?, ?, ?, ?, ?, ?, ?)")

    links_sql.write(
        ") INSERT INTO user_touch_links ("
        " uid,"
        " user_touch_id,"
        " code,"
        " page_identifier,"
        " page_extra,"
        " preview_identifier,"
        " preview_extra"
        ") SELECT"
        " batch.uid,"
        " user_touches.id,"
        " batch.code,"
        " batch.page_identifier,"
        " batch.page_extra,"
        " batch.preview_identifier,"
        " batch.preview_extra "
        "FROM batch "
        "JOIN user_touches ON user_touches.uid = batch.user_touch_uid "
        "WHERE NOT EXISTS (SELECT 1 FROM user_touch_links WHERE uid = batch.uid OR code = batch.code)"
    )

    clicks_sql = io.StringIO()
    clicks_sql.write(
        "WITH batch("
        " uid,"
        " user_touch_link_uid,"
        " track_type,"
        " parent_uid,"
        " user_sub,"
        " visitor_uid,"
        " child_known,"
        " clicked_at"
        ") AS (VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    num_clicks_to_persist = 0
    click_params = []

    for item in persisting:
        if not item.clicks:
            continue

        num_clicks_to_persist += len(item.clicks)
        parents = set()
        for click in item.clicks:
            if click.parent_uid is not None:
                parents.add(click.parent_uid)

        for click in item.clicks:
            click_params.extend(
                [
                    click.uid,
                    item.data.uid,
                    click.track_type,
                    click.parent_uid,
                    click.user_sub,
                    click.visitor_uid,
                    int(click.parent_uid in parents),
                    click.clicked_at,
                ]
            )
            if len(click_params) > 1:
                clicks_sql.write(", (?, ?, ?, ?, ?, ?, ?, ?)")

    clicks_sql.write(
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
        " batch.parents_uid IS NOT NULL,"
        " batch.user_sub IS NOT NULL,"
        " batch.visitor_uid IS NOT NULL,"
        " batch.child_known,"
        " batch.clicked_at,"
        " ? "
        "FROM batch "
        "JOIN user_touch_links ON user_touch_links.uid = batch.user_touch_link_uid "
        "LEFT JOIN users ON users.sub = batch.user_sub "
        "LEFT JOIN visitors ON visitors.uid = batch.visitor_uid "
        "LEFT JOIN user_touch_links AS parents ON parents.uid = batch.parent_uid "
        "WHERE"
        " NOT EXISTS (SELECT 1 FROM user_touch_link_clicks WHERE uid = batch.uid)"
    )
    click_params.append(batch_at)

    cleanup_batches: List[RedisTransaction] = []

    for cleanup_start_idx in range(0, len(batch), REDIS_DELETE_BATCH_SIZE):
        cleanup_end_idx_excl = min(
            cleanup_start_idx + REDIS_DELETE_BATCH_SIZE, len(batch)
        )
        cleanup_batches.append(
            await form_cleanup_transaction(
                itgs,
                [item.code for item in batch[cleanup_start_idx:cleanup_end_idx_excl]],
            )
        )

    return Batch(
        lost=lost,
        persisting=persisting,
        links=SqlQuery(links_sql.getvalue(), link_params) if persisting else None,
        clicks=SqlQuery(clicks_sql.getvalue(), click_params)
        if num_clicks_to_persist > 0
        else None,
        num_clicks_to_persist=num_clicks_to_persist,
        redis_cleanup=cleanup_batches,
    )


async def form_cleanup_transaction(itgs: Itgs, codes: List[str]) -> RedisTransaction:
    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_touch_persist_cleanup_script_exists(redis, force=force)

    async def func():
        await touch_persist_cleanup(
            redis, b"touch_links:persist_purgatory", b"touch_links:buffer", codes
        )

    return RedisTransaction(prep, func)


def count_rows_for_touch(item: TouchReadToPersistResult) -> int:
    """Counts how many rows need to be written to persist the given item,
    so that batches can be formed
    """
    if item.data is None:
        return 0

    return 1 + len(item.clicks)


async def execute_batch(itgs: Itgs, batch: Batch, batch_at: float) -> RunStats:
    conn = await itgs.conn()
    cursor = conn.cursor()

    if batch.clicks is not None:
        assert batch.links is not None

    if batch.links is not None:
        response = await cursor.executemany3(
            (
                (batch.links.query, batch.links.params),
                *(
                    []
                    if batch.clicks is None
                    else [(batch.clicks.query, batch.clicks.params)]
                ),
            )
        )

        num_links_persisted = response[0].rows_affected
        if num_links_persisted is None:
            num_links_persisted = 0

        if batch.clicks is not None:
            num_clicks_persisted = response[1].rows_affected

            if num_clicks_persisted is None:
                num_clicks_persisted = 0
        else:
            num_clicks_persisted = 0
    else:
        num_links_persisted = 0
        num_clicks_persisted = 0

    for transaction in batch.redis_cleanup:
        await run_with_prep(transaction.prep, transaction.func)

    links_batch_succeeded = num_links_persisted == len(batch.persisting)
    clicks_batch_succeeded = num_clicks_persisted == batch.num_clicks_to_persist

    stats = LinkStatsPreparer()

    stats.incr_touch_links(
        unix_date=unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz),
        event="persists_failed",
        event_extra=b"lost",
        amt=batch.lost,
    )

    if links_batch_succeeded:
        for touch in batch.persisting:
            unix_date = unix_dates.unix_timestamp_to_unix_date(
                touch.data.created_at, tz=tz
            )
            stats.incr_touch_links(
                unix_date=unix_date,
                event="persisted",
                event_extra=touch.data.page_identifier.encode("utf-8"),
            )
    else:
        stats.incr_touch_links(
            unix_date=unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz),
            event="persisted_in_failed_batch",
            amt=num_links_persisted,
        )
        stats.incr_touch_links(
            unix_date=unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz),
            event="persists_failed",
            event_extra=b"integrity",
            amt=len(batch.persisting) - num_links_persisted,
        )

    if clicks_batch_succeeded:
        for touch in batch.persisting:
            stats.incr_touch_links(
                unix_date=unix_dates.unix_timestamp_to_unix_date(
                    touch.data.created_at, tz=tz
                ),
                event="persisted_clicks",
                event_extra=f"{touch.data.page_identifier}:{len(touch.clicks)}",
                amt=len(touch.clicks),
            )
    else:
        stats.incr_touch_links(
            unix_date=unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz),
            event="persisted_clicks_in_failed_batch",
            amt=num_clicks_persisted,
        )
        stats.incr_touch_links(
            unix_date=unix_dates.unix_timestamp_to_unix_date(batch_at, tz=tz),
            event="persist_click_failed",
            amt=batch.num_clicks_to_persist - num_clicks_persisted,
        )

    await stats.store(itgs)

    if not links_batch_succeeded:
        await handle_warning(
            f"{__name__}:links_batch_failed",
            f"Expected to insert {len(batch.persisting)} links but only inserted {num_links_persisted}",
        )

    if not clicks_batch_succeeded:
        await handle_warning(
            f"{__name__}:clicks_batch_failed",
            f"Expected to insert {batch.num_clicks_to_persist} clicks but only inserted {num_clicks_persisted}",
        )

    return RunStats(
        attempted=len(batch.persisting),
        lost=batch.lost,
        integrity_error=len(batch.persisting) - num_links_persisted,
        persisted=num_links_persisted,
        persisted_without_clicks=sum(
            len(touch.clicks) == 0 for touch in batch.persisting
        )
        if links_batch_succeeded and clicks_batch_succeeded
        else -1,
        persisted_with_one_click=sum(
            len(touch.clicks) == 1 for touch in batch.persisting
        )
        if links_batch_succeeded and clicks_batch_succeeded
        else -1,
        persisted_with_multiple_clicks=sum(
            len(touch.clicks) > 1 for touch in batch.persisting
        )
        if links_batch_succeeded and clicks_batch_succeeded
        else -1,
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.persist_link")

    asyncio.run(main())
