"""Persists entries from the View To Log Queue"""
import io
import logging
import time
from typing import List, Optional
from error_middleware import handle_contextless_error, handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
from lib.basic_redis_lock import basic_redis_lock
from lib.shared.redis_hash import RedisHash
from redis_helpers.move_to_set_purgatory import move_to_set_purgatory_safe
from jobs import JobCategory
from dataclasses import dataclass

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""The maximum amount of time this job will run before yielding to other jobs"""

REDIS_MOVE_BATCH_SIZE = 100
"""How many items to move from views_to_log to the views_log_purgatory at a time
"""

REDIS_READ_COUNT = 10
"""Approximately how many items we read from the purgatory to work on at a time;
this works like the COUNT argument to SCAN
"""

REDIS_DELETE_BATCH_SIZE = 10
"""How many items to delete from purgatory and associated at a time, after persisting
to the database
"""

DATABASE_WRITE_BATCH_SIZE = 100
"""Approximately how many items we write to the database at a time"""


@dataclass
class _RunStats:
    attempted: int = 0
    persisted: int = 0
    partially_persisted: int = 0
    failed: int = 0


@dataclass
class _ViewToPersist:
    view_uid: str
    journey_share_link_code: str
    journey_share_link_uid: Optional[str]
    user_sub: Optional[str]
    visitor: Optional[str]
    visitor_was_unique: Optional[bool]
    clicked_at: float
    confirmed_at: Optional[float]


@dataclass
class _DatabaseWriteQuery:
    write_query: str
    write_check_query: str


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Takes values from the left of the View To Log Queue, removes them from
    the View Pseudo-Set and Unconfirmed Views Sorted Set, and persists the
    real views to the database table `journey_share_link_views`

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"journey_share_links:views_log_job:lock", gd=gd, spin=False
    ):
        stop_reason: Optional[str] = None
        stats = _RunStats()

        redis = await itgs.redis()
        await redis.hset(
            b"stats:journey_share_links:log_job",  # type: ignore
            mapping={
                b"started_at": str(started_at).encode("ascii"),
            },
        )

        conn = await itgs.conn()
        cursor = conn.cursor("strong")
        _full_batch_database_write_query: Optional[_DatabaseWriteQuery] = None

        while True:
            if gd.received_term_signal:
                logging.info("Received term signal; stopping early")
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.info("Reached max job time; stopping early")
                stop_reason = "time_exhausted"
                break

            move_result = await move_to_set_purgatory_safe(
                itgs,
                b"journey_share_links:views_to_log",
                b"journey_share_links:views_log_purgatory",
                REDIS_MOVE_BATCH_SIZE,
            )
            if move_result.category == "moved_items" and move_result.num_popped == 0:
                logging.info("No more items in queue")
                stop_reason = "list_exhausted"
                break

            if move_result.category == "had_items_in_purgatory":
                await handle_warning(
                    f"{__name__}:recovering_from_purgatory",
                    f"Recovering {move_result.num_items_in_purgatory} items from purgatory",
                )
            else:
                logging.debug(
                    f"Popped {move_result.num_popped} from the To Log queue and "
                    f"added {move_result.num_added} to purgatory"
                )

            view_uids: List[str] = []
            scan_cursor = 0
            while True:
                (scan_cursor, keys) = await redis.sscan(
                    b"journey_share_links:views_log_purgatory",
                    scan_cursor,
                    count=REDIS_READ_COUNT,
                )
                for key in keys:
                    view_uids.append(str(key, "utf-8"))
                if scan_cursor == 0:
                    break

            if len(view_uids) == 0:
                await handle_contextless_error(
                    extra_info=f"Despite adding to or checking purgatory, found no view uids in scan"
                )
                stop_reason = "list_exhausted"
                break

            logging.debug(f"Found {len(view_uids)} view uids to handle, retrieving...")
            views_to_persist: List[_ViewToPersist] = []
            views_to_drop: List[str] = []
            for start_idx in range(0, len(view_uids), REDIS_READ_COUNT):
                end_idx_excl = min(start_idx + REDIS_READ_COUNT, len(view_uids))

                async with redis.pipeline() as pipe:
                    for idx in range(start_idx, end_idx_excl):
                        await pipe.hgetall(
                            f"journey_share_links:views:{view_uids[idx]}".encode(  # type: ignore
                                "utf-8"
                            )
                        )
                    res = await pipe.execute()

                assert isinstance(res, list), res
                assert len(res) == end_idx_excl - start_idx, res

                for idx in range(start_idx, end_idx_excl):
                    view_info = RedisHash(res[idx - start_idx])
                    if len(view_info) == 0:
                        logging.info(
                            f"Dropping view uid {view_uids[idx]}, not in view pseudoset"
                        )
                        views_to_drop.append(view_uids[idx])
                        continue

                    try:
                        visitor_was_unique_raw = view_info.get_int(
                            b"visitor_was_unique", default=None
                        )
                        view = _ViewToPersist(
                            view_uid=view_info.get_str(b"uid"),
                            journey_share_link_code=view_info.get_str(
                                b"journey_share_link_code"
                            ),
                            journey_share_link_uid=view_info.get_str(
                                b"journey_share_link_uid", default=None
                            ),
                            user_sub=view_info.get_str(b"user_sub", default=None),
                            visitor=view_info.get_str(b"visitor", default=None),
                            visitor_was_unique=(
                                None
                                if visitor_was_unique_raw is None
                                else bool(visitor_was_unique_raw)
                            ),
                            clicked_at=view_info.get_float(b"clicked_at"),
                            confirmed_at=view_info.get_float(
                                b"confirmed_at", default=None
                            ),
                        )
                    except Exception as e:
                        await handle_warning(
                            f"{__name__}:invalid_view",
                            f"```\n{res[idx - start_idx]=}\n````",
                            exc=e,
                        )
                        views_to_drop.append(view_uids[idx])
                        continue

                    if view.journey_share_link_uid is None:
                        logging.info(
                            f"Dropping view uid {view_uids[idx]}, was a view for a non-existent code"
                        )
                        views_to_drop.append(view_uids[idx])
                    else:
                        views_to_persist.append(view)

            stats.attempted += len(views_to_persist) + len(views_to_drop)
            stats.failed += len(views_to_drop)

            for start_idx in range(0, len(views_to_drop), REDIS_DELETE_BATCH_SIZE):
                end_idx_excl = min(
                    start_idx + REDIS_DELETE_BATCH_SIZE, len(views_to_drop)
                )
                async with redis.pipeline() as pipe:
                    pipe.multi()
                    for idx in range(start_idx, end_idx_excl):
                        view_uid = views_to_drop[idx]
                        await pipe.delete(
                            f"journey_share_links:views:{view_uid}".encode("utf-8")
                        )
                        await pipe.zrem(
                            b"journey_share_links:views_unconfirmed",
                            view_uid.encode("utf-8"),
                        )
                        await pipe.srem(b"journey_share_links:views_log_purgatory", view_uid.encode("utf-8"))  # type: ignore
                    await pipe.execute()

            for start_idx in range(0, len(views_to_persist), DATABASE_WRITE_BATCH_SIZE):
                end_idx_excl = min(
                    start_idx + DATABASE_WRITE_BATCH_SIZE, len(views_to_persist)
                )
                batch_size = end_idx_excl - start_idx

                if (
                    batch_size == DATABASE_WRITE_BATCH_SIZE
                    and _full_batch_database_write_query is not None
                ):
                    query = _full_batch_database_write_query
                else:
                    query_writer = io.StringIO()
                    query_writer.write(
                        "WITH batch(uid, journey_share_link_uid, user_sub, visitor, visitor_was_unique, clicked_at, confirmed_at)"
                        " AS (VALUES (?, ?, ?, ?, ?, ?, ?)"
                    )
                    for _ in range(1, batch_size):
                        query_writer.write(", (?, ?, ?, ?, ?, ?, ?)")
                    query_writer.write(
                        ") INSERT INTO journey_share_link_views ("
                        " uid,"
                        " journey_share_link_id,"
                        " user_id,"
                        " visitor_id,"
                        " user_set,"
                        " visitor_set,"
                        " visitor_was_unique,"
                        " created_at,"
                        " confirmed_at"
                        ") SELECT"
                        " batch.uid,"
                        " journey_share_links.id,"
                        " users.id,"
                        " visitors.id,"
                        " batch.user_sub IS NOT NULL,"
                        " batch.visitor IS NOT NULL,"
                        " batch.visitor_was_unique,"
                        " batch.clicked_at,"
                        " batch.confirmed_at "
                        "FROM batch, journey_share_links "
                        "LEFT JOIN users ON (batch.user_sub IS NOT NULL AND users.sub = batch.user_sub) "
                        "LEFT JOIN visitors ON (batch.visitor IS NOT NULL AND visitors.uid = batch.visitor) "
                        "WHERE"
                        " batch.journey_share_link_uid = journey_share_links.uid"
                        " AND NOT EXISTS (SELECT 1 FROM journey_share_link_views AS jslv WHERE jslv.uid = batch.uid)"
                    )
                    _db_write_query = query_writer.getvalue()
                    query_writer = io.StringIO()
                    query_writer.write("WITH batch(uid) AS (VALUES (?)")
                    for _ in range(1, batch_size):
                        query_writer.write(", (?)")
                    query_writer.write(
                        ") SELECT "
                        " COUNT(*) "
                        "FROM journey_share_link_views, batch "
                        "WHERE"
                        " journey_share_link_views.uid = batch.uid"
                        " AND ("
                        "  (journey_share_link_views.user_id IS NULL) <> journey_share_link_views.user_set"
                        "  OR (journey_share_link_views.visitor_id IS NULL) <> journey_share_link_views.visitor_set"
                        " )"
                    )
                    _db_check_query = query_writer.getvalue()
                    query = _DatabaseWriteQuery(
                        write_query=_db_write_query, write_check_query=_db_check_query
                    )
                    if batch_size == DATABASE_WRITE_BATCH_SIZE:
                        _full_batch_database_write_query = query

                write_query_args = []
                for idx in range(start_idx, end_idx_excl):
                    view = views_to_persist[idx]
                    write_query_args.extend(
                        [
                            view.view_uid,
                            view.journey_share_link_uid,
                            view.user_sub,
                            view.visitor,
                            view.visitor_was_unique,
                            view.clicked_at,
                            view.confirmed_at,
                        ]
                    )

                check_query_args = []
                for idx in range(start_idx, end_idx_excl):
                    check_query_args.append(views_to_persist[idx].view_uid)

                response = await cursor.executeunified3(
                    [
                        (query.write_query, write_query_args),
                        (query.write_check_query, check_query_args),
                    ],
                )
                assert response[1].results, response

                num_attempted_in_batch = end_idx_excl - start_idx
                num_stored_in_batch = response[0].rows_affected or 0

                # CORRECTNESS:
                #   This partial count isn't exactly right, since if the view was already
                #   persisted and when it was last persisted it was partial, it will
                #   be included in this count. still good enough for this value though, which
                #   is just to get a rough idea of how the job is doing
                num_partially_persisted_in_batch = response[1].results[0][0]

                stats.persisted += (
                    num_stored_in_batch - num_partially_persisted_in_batch
                )
                stats.partially_persisted += num_partially_persisted_in_batch
                stats.failed += num_attempted_in_batch - num_stored_in_batch

                for start_sub_idx in range(
                    start_idx, end_idx_excl, REDIS_DELETE_BATCH_SIZE
                ):
                    end_sub_idx_excl = min(
                        start_sub_idx + REDIS_DELETE_BATCH_SIZE, end_idx_excl
                    )

                    async with redis.pipeline() as pipe:
                        pipe.multi()
                        for idx in range(start_sub_idx, end_sub_idx_excl):
                            view = views_to_persist[idx]
                            await pipe.delete(
                                f"journey_share_links:views:{view.view_uid}".encode(
                                    "utf-8"
                                )
                            )
                            await pipe.zrem(
                                b"journey_share_links:views_unconfirmed",
                                view.view_uid.encode("utf-8"),
                            )
                            await pipe.srem(b"journey_share_links:views_log_purgatory", view.view_uid.encode("utf-8"))  # type: ignore
                        await pipe.execute()

        assert stop_reason is not None

        finished_at = time.time()
        await redis.hset(
            b"stats:journey_share_links:log_job",  # type: ignore
            mapping={
                b"finished_at": str(finished_at).encode("ascii"),
                b"running_time": str(finished_at - started_at).encode("ascii"),
                b"attempted": str(stats.attempted).encode("ascii"),
                b"persisted": str(stats.persisted).encode("ascii"),
                b"partially_persisted": str(stats.partially_persisted).encode("ascii"),
                b"failed": str(stats.failed).encode("ascii"),
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.journey_share_links.log")

    asyncio.run(main())
