"""Processes the journey share links raced confirmations hash"""
from typing import Literal, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
from dataclasses import dataclass
from pydantic import BaseModel, Field
import logging
import time

from lib.basic_redis_lock import basic_redis_lock
from redis_helpers.journey_share_links_sweep_raced_confirmations import (
    journey_share_links_sweep_raced_confirmations_safe,
)

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""The maximum amount of time this job will run before yielding to other jobs"""

REDIS_READ_BATCH_SIZE = 10
"""How many items are retrieved from redis at a time, i.e., before yielding to
other commands
"""

# PERF: we don't batch database writes to simplify the job, and also
#   because we don't expect significant sized batches assuming we tune the
#   view to log job properly


@dataclass
class _RunStats:
    attempted: int = 0
    not_ready: int = 0
    persisted: int = 0
    partially_persisted: int = 0
    failed_did_not_exist: int = 0
    failed_already_confirmed: int = 0


class _ViewToConfirm(BaseModel):
    uid: str = Field()
    user_sub: Optional[str] = Field(None)
    visitor: Optional[str] = Field(None)
    visitor_was_unique: Optional[bool] = Field(None)
    confirmed_at: float = Field()


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Sweeps through the raced confirmations hash, which contains views which were
    confirmed while the view was in the view to log purgatory, and persists the
    updated information if the log job has finished persisting the view.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"journey_share_links:raced_confirmations_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:journey_share_links:raced_confirmations_job",  # type: ignore
            mapping={
                b"started_at": str(started_at).encode("ascii"),
            },
        )

        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        scan_cursor: Optional[int] = None

        stats = _RunStats()
        stop_reason: Optional[
            Literal["list_exhausted", "time_exhausted", "signal"]
        ] = None

        while stop_reason is None:
            loop_at = time.time()

            if scan_cursor == 0:
                logging.info("No more items to scan, stopping")
                stop_reason = "list_exhausted"
                continue

            if gd.received_term_signal:
                logging.info("Received term signal, stopping early")
                stop_reason = "signal"
                continue

            if loop_at - started_at > MAX_JOB_TIME_SECONDS:
                logging.info("Reached max job time, stopping early")
                stop_reason = "time_exhausted"
                continue

            iter_result = await journey_share_links_sweep_raced_confirmations_safe(
                itgs,
                scan_cursor if scan_cursor is not None else 0,
                REDIS_READ_BATCH_SIZE,
                loop_at,
            )

            scan_cursor = iter_result.cursor
            stats.attempted += iter_result.found
            stats.not_ready += iter_result.found - len(iter_result.result)

            for raw_view_to_confirm in iter_result.result:
                view_to_confirm = _ViewToConfirm.model_validate_json(
                    raw_view_to_confirm
                )
                logging.debug(f"Processing {view_to_confirm=}")

                response = await cursor.executeunified3(
                    (
                        (
                            "SELECT confirmed_at FROM journey_share_link_views WHERE uid=?",
                            (view_to_confirm.uid,),
                        ),
                        (
                            "WITH faked(v) AS (VALUES (1)) "
                            "UPDATE journey_share_link_views "
                            "SET"
                            " user_id=users.id,"
                            " visitor_id=visitors.id,"
                            " user_set=?,"
                            " visitor_set=?,"
                            " visitor_was_unique=?,"
                            " confirmed_at=? "
                            "FROM faked "
                            "LEFT OUTER JOIN users ON users.sub = ? "
                            "LEFT OUTER JOIN visitors ON visitors.uid = ? "
                            "WHERE"
                            " journey_share_link_views.uid=?"
                            " AND journey_share_link_views.confirmed_at IS NULL",
                            (
                                view_to_confirm.user_sub is not None,
                                view_to_confirm.visitor is not None,
                                None
                                if view_to_confirm.visitor_was_unique is None
                                else int(view_to_confirm.visitor_was_unique),
                                view_to_confirm.confirmed_at,
                                view_to_confirm.user_sub,
                                view_to_confirm.visitor,
                                view_to_confirm.uid,
                            ),
                        ),
                        (
                            "SELECT"
                            " user_id IS NOT NULL AS b1,"
                            " visitor_id IS NOT NULL AS b2 "
                            "FROM journey_share_link_views "
                            "WHERE uid=?",
                            (view_to_confirm.uid,),
                        ),
                    )
                )

                view_existed: bool = False
                view_was_confirmed: bool = False
                view_updated: bool = False
                view_now_has_user: bool = False
                view_now_has_visitor: bool = False

                if response[0].results:
                    view_existed = True
                    view_was_confirmed = response[0].results[0][0] is not None

                if (
                    response[1].rows_affected is not None
                    and response[1].rows_affected > 0
                ):
                    if response[1].rows_affected != 1:
                        await handle_warning(
                            f"{__name__}:multiple_rows_affected",
                            f"expected `response[1]` had one row affected!\n\n```\n{response=}\n```",
                        )
                    view_updated = True

                if response[2].results:
                    view_now_has_user = bool(response[2].results[0][0])
                    view_now_has_visitor = bool(response[2].results[0][1])

                logging.debug(
                    f"{view_to_confirm} -> {view_existed=} {view_was_confirmed=} {view_updated=} {view_now_has_user=} {view_now_has_visitor=}"
                )

                if not view_existed:
                    assert not view_updated, response
                    stats.failed_did_not_exist += 1

                if view_was_confirmed:
                    assert view_existed, response
                    assert not view_updated, response
                    stats.failed_already_confirmed += 1

                if view_existed and not view_was_confirmed:
                    assert view_updated, response
                    expected_user = view_to_confirm.user_sub is not None
                    expected_visitor = view_to_confirm.visitor is not None
                    if (expected_user and not view_now_has_user) or (
                        expected_visitor and not view_now_has_visitor
                    ):
                        stats.partially_persisted += 1
                    else:
                        stats.persisted += 1

                await redis.hdel(
                    b"journey_share_links:views_to_confirm",  # type: ignore
                    view_to_confirm.uid.encode("utf-8"),  # type: ignore
                )

        finished_at = time.time()
        logging.debug(
            f"Finished sweeping raced confirmations; {stats=} in {finished_at - started_at:.3f}s"
        )
        await redis.hset(
            b"stats:journey_share_links:raced_confirmations_job",  # type: ignore
            mapping={
                b"finished_at": str(finished_at).encode("ascii"),
                b"running_time": str(finished_at - started_at).encode("ascii"),
                b"attempted": str(stats.attempted).encode("ascii"),
                b"not_ready": str(stats.not_ready).encode("ascii"),
                b"persisted": str(stats.persisted).encode("ascii"),
                b"partially_persisted": str(stats.partially_persisted).encode("ascii"),
                b"failed_did_not_exist": str(stats.failed_did_not_exist).encode(
                    "ascii"
                ),
                b"failed_already_confirmed": str(stats.failed_already_confirmed).encode(
                    "ascii"
                ),
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.journey_share_links.raced_confirmations")

    asyncio.run(main())
