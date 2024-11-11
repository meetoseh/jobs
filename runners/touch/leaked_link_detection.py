"""Touch Leaked Link Detection"""

from typing import Literal, Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.durations import format_duration
from lib.redis_stats_preparer import RedisStatsPreparer
from lib.touch.link_info import TouchLink, TouchLinkBufferedClick
from runners.touch.stale_detection import STALE_AGE_SECONDS as TOUCH_STALE_AGE_SECONDS
from lib.basic_redis_lock import basic_redis_lock
import time
import dataclasses
import unix_dates
import pytz
import json

category = JobCategory.LOW_RESOURCE_COST

LEAKED_AGE_SECONDS = TOUCH_STALE_AGE_SECONDS + 60 * 60
"""The amount of time, in seconds, that a touch link can be in the Buffered Link sorted set
before this job assumes it has leaked. Must be less than 48 hours (or stats won't be updated
correctly) and longer than the touch stale age (as that timeout triggering should cleanup the 
link without this job needing to interfere)
"""

MAX_JOB_TIME_SECONDS = 50
"""If the leaked link detection job runs for longer than this amount in seconds it
stops itself to let other jobs run
"""

tz = pytz.timezone("America/Los_Angeles")


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Checks for old entries in the buffered link sorted set that have probably
    leaked and persists them (if possible) then deletes them to free memory.
    Since this is not expected to be a common occurrence, this is implemented
    without batching and without too much concern about the leaked link being
    changed while this is running.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"touch_links:leaked_link_detection_job:lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_links:leaked_link_detection_job",  # type: ignore
            b"started_at",  # type: ignore
            str(started_at).encode("ascii"),  # type: ignore
        )

        stats = RunStats()
        stop_reason: Optional[str] = None

        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        while True:
            if gd.received_term_signal:
                logging.warning(
                    "Leaked link detection job received term signal, stopping early"
                )
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                logging.warning(
                    "Leaked link detection job ran for too long, stopping early"
                )
                stop_reason = "time_exhausted"
                break

            next_leaked_link = await redis.zrange(
                b"touch_links:buffer",
                b"-inf",  # type: ignore
                started_at - LEAKED_AGE_SECONDS,  # type: ignore
                byscore=True,
                withscores=True,
                offset=0,
                num=1,
            )

            assert isinstance(next_leaked_link, list)
            if not next_leaked_link:
                logging.debug("Leaked link detection job found no more leaks")
                stop_reason = "list_exhausted"
                break

            assert len(next_leaked_link) == 1
            code: bytes = next_leaked_link[0][0]
            score: float = next_leaked_link[0][1]
            assert isinstance(code, bytes)
            assert isinstance(score, float)

            await handle_warning(
                f"{__name__}:leaked_link",
                f"Detected link with code {code.decode('utf-8')} has leaked; it was inserted "
                f"at {unix_dates.unix_timestamp_to_datetime(score, tz=tz).isoformat()}, i.e., "
                f"{format_duration(time.time() - score)} ago",
            )

            data_key = f"touch_links:buffer:{code.decode('utf-8')}".encode("utf-8")
            clicks_key = f"touch_links:buffer:clicks:{code.decode('utf-8')}".encode(
                "utf-8"
            )

            await redis.zrem(b"touch_links:buffer", code)
            await redis.zrem(b"touch_links:to_persist", code)
            data_raw = await redis.hgetall(data_key)  # type: ignore
            await redis.delete(data_key)
            clicks_raw = await redis.lrange(clicks_key, 0, -1)  # type: ignore
            await redis.delete(clicks_key)

            clicks_all_cleaned = True

            # even if some clicks fail to parse we want to try to cleanup as many as possible
            for click in clicks_raw:
                try:
                    click_info = await json.loads(click)
                    assert isinstance(click_info, dict)
                    assert "uid" in click_info
                    assert isinstance(click_info["uid"], str)
                except:
                    # we'll deal with this later
                    clicks_all_cleaned = False
                    continue

                await redis.delete(
                    f"touch_links:buffer:on_clicks_by_uid:{click_info['uid']}".encode(
                        "utf-8"
                    )
                )

            logging.debug(
                f"Cleaned up code {code=} with {data_raw=} and {clicks_raw=}; {clicks_all_cleaned=}; "
                "proceeding with parsing and handling"
            )
            data = (
                TouchLink.from_redis_mapping(data_raw) if data_raw is not None else None
            )
            clicks = [
                TouchLinkBufferedClick.model_validate_json(click_raw)
                for click_raw in clicks_raw
            ]

            if not clicks_all_cleaned:
                await handle_warning(
                    f"{__name__}:corrupted_click",
                    f"Did not properly cleanup {code=} clicks but successfully parsed all of them? "
                    f"{data=}, {clicks=}",
                )

            response = (
                await cursor.execute(
                    """
                    INSERT INTO user_touch_links (
                        uid, user_touch_id, code, page_identifier, page_extra, 
                        preview_identifier, preview_extra
                    )
                    SELECT
                        ?, user_touches.id, ?, ?, ?, ?, ?
                    FROM user_touches
                    WHERE 
                        user_touches.send_uid = ? 
                        AND NOT EXISTS (
                            SELECT 1 FROM user_touch_links AS utl 
                            WHERE utl.uid=? OR utl.code=?
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM user_touches AS ut
                            WHERE ut.send_uid = user_touches.send_uid
                              AND ut.id < user_touches.id
                        )
                    """,
                    (
                        data.uid,
                        data.code,
                        data.page_identifier,
                        json.dumps(data.page_extra, sort_keys=True),
                        data.preview_identifier,
                        json.dumps(data.preview_extra, sort_keys=True),
                        data.touch_uid,
                        data.uid,
                        data.code,
                    ),
                )
                if data is not None
                else None
            )

            if (
                data is not None
                and response is not None
                and response.rows_affected == 1
            ):
                logging.debug("Recovered leaked link, persisting clicks now")
                parents = set()
                for click in clicks:
                    if click.parent_uid is not None:
                        parents.add(click.parent_uid)

                num_clicks_persisted = 0
                for click in clicks:
                    response = await cursor.execute(
                        """
                        INSERT INTO user_touch_link_clicks (
                            uid, user_touch_link_id, track_type, parent_id, user_id,
                            visitor_id, parent_known, user_known, visitor_known, child_known,
                            clicked_at, created_at
                        )
                        SELECT
                            ?, user_touch_links.id, ?, parents.id, users.id,
                            visitors.id, ?, ?, ?, ?,
                            ?, ?
                        FROM user_touch_links
                        LEFT JOIN users ON users.sub = ?
                        LEFT JOIN visitors ON visitors.uid = ?
                        LEFT JOIN user_touch_link_clicks AS parents ON parents.uid = ?
                        WHERE
                            user_touch_links.uid = ?
                            AND NOT EXISTS (
                                SELECT 1 FROM user_touch_link_clicks AS utlc
                                WHERE utlc.uid=?
                            )
                        """,
                        (
                            click.uid,
                            click.track_type,
                            int(click.parent_uid is not None),
                            int(click.user_sub is not None),
                            int(click.visitor_uid is not None),
                            int(click.uid in parents),
                            click.clicked_at,
                            time.time(),
                            click.user_sub,
                            click.visitor_uid,
                            click.parent_uid,
                            data.uid,
                            click.uid,
                        ),
                    )
                    if response.rows_affected == 1:
                        num_clicks_persisted += 1

                if num_clicks_persisted != len(clicks):
                    await handle_warning(
                        f"{__name__}:clicks_not_persisted",
                        f"Persisted {num_clicks_persisted} of {len(clicks)} clicks for {data=}",
                    )

                stats.leaked += 1
                stats.recovered += 1

                redis_stats = MyRedisStats()
                redis_stats.incr_leaked(result=b"recovered", created_at=score)
                await redis_stats.store(itgs)
            else:
                logging.debug(
                    "Abandoned leaked link, not persisting clicks. Checking if duplicate"
                )

                if data is not None:
                    response = await cursor.execute(
                        "SELECT 1 FROM user_touch_links WHERE uid=? OR code=? LIMIT 1",
                        (data.uid, data.code),
                    )
                    duplicate = not not response.results
                else:
                    duplicate = False

                logging.debug(f"{code=} {duplicate=}")

                stats.leaked += 1
                stats.abandoned += 1

                redis_stats = MyRedisStats()
                redis_stats.incr_leaked(
                    result=b"duplicate" if duplicate else b"abandoned",
                    created_at=score,
                )
                await redis_stats.store(itgs)

        assert stop_reason is not None
        finished_at = time.time()
        logging.info(
            f"Touch Leaked Link Detection Job Finished:\n"
            f"- Started At: {started_at:.3f}\n"
            f"- Finished At: {finished_at:.3f}\n"
            f"- Running Time: {finished_at - started_at:.3f}\n"
            f"- Stop Reason: {stop_reason}\n"
            f"- Leaked: {stats.leaked}\n"
            f"- Recovered: {stats.recovered}\n"
            f"- Abandoned: {stats.abandoned}\n"
        )

        redis = await itgs.redis()
        await redis.hset(
            b"stats:touch_links:leaked_link_detection_job",  # type: ignore
            mapping={
                b"finished_at": finished_at,
                b"running_time": finished_at - started_at,
                b"leaked": stats.leaked,
                b"recovered": stats.recovered,
                b"abandoned": stats.abandoned,
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


@dataclasses.dataclass
class RunStats:
    leaked: int = 0
    recovered: int = 0
    abandoned: int = 0


class MyRedisStats(RedisStatsPreparer):
    def __init__(self):
        super().__init__()

    def incr_leaked(
        self,
        *,
        result: Literal[b"recovered", b"abandoned", b"duplicate"],
        created_at: float,
    ):
        self.incrby(
            unix_date=unix_dates.unix_timestamp_to_unix_date(created_at, tz=tz),
            basic_key_format="stats:touch_links:daily:{unix_date}",
            earliest_key=b"stats:touch_links:daily:earliest",
            event="leaked",
            event_extra_format="stats:touch_links:daily:{unix_date}:extra:{event}",
            event_extra=result,
            amt=1,
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.touch.leaked_link_detection")

    asyncio.run(main())
