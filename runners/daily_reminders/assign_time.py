"""Daily Reminders Assign Time Job"""
import json
import random
import time
from typing import Dict, List, Literal, Optional, Set, Tuple
from error_middleware import handle_error, handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
from dataclasses import dataclass
import pytz
from lib.daily_reminders.stats import DailyReminderStatsPreparer
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.set_if_lower import ensure_set_if_lower_script_exists, set_if_lower
import unix_dates

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""The maximum duration in seconds this job can run before stopping itself to allow
other jobs to run
"""

QUEUE_BACKPRESSURE_LENGTH = 25_000
"""If there are more than this number of reminders queued before starting a batch
we stop with a warning to avoid overloading redis as something likely has gone wrong.
This number should be increased as the number of users increases.
"""

TARGET_END_ITERATION_OFFSET_SECONDS = 60 * 35
"""The number of seconds ahead of when the job starts that we try to end iteration
at. For example, at 50m, at 9AM we try to assign times to reminders that might be
sent between 9AM and 9:50AM.

35m was chosen to ensure one job can be skipped without any issues. Jobs occur
once every 15m, so if one job fails, the gap between successful jobs will be 30m,
but the job will need a bit of time to actually run.
"""

DATABASE_WRITE_BATCH_SIZE = 100
"""The maximum number of items to write to the database within a single transaction
"""

DATABASE_READ_BATCH_SIZE = 100
"""The maximum number of items to read from the database within a single transaction
"""

REDIS_WRITE_BATCH_SIZE = 12
"""The maximum number of items to write to redis at a time"""

STALE_THRESHOLD_SECONDS = 60 * 60
"""How long after the end of a users requested reminder timerange we consider
too stale to send, in seconds
"""

DEFAULT_TIMEZONE = "America/Los_Angeles"
"""The timezone used if the user does not have a timezone set"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Continues iterating over the user daily reminders table, assigning a time
    to daily reminders whose time range is coming up.

    Specifically, this progresses one cursor per timezone, since time ranges
    cannot be compared except in timezones, and iteration requires a
    deterministic sort order that can be represented within the database.

    When a user is assigned a timezone no other user has ever been assigned,
    without any additional intervention, this job will initialize the timezone
    within 26 hours, which means they may not receive a notification for 72
    hours in the worst case. For now this seems like a reasonable tradeoff for
    simplicity, but the delay can be reduced by adding the timezone to
    `daily_reminders:progress:timezones:{unix_date}` for the current unix date
    in that timezone if the key already exists when the users timezone is set if
    the delay becomes a problem.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"daily_reminders:assign_time_job_lock", gd=gd, spin=False
    ):
        redis = await itgs.redis()
        await redis.hset(
            b"stats:daily_reminders:assign_time_job",
            b"started_at",
            str(started_at).encode("ascii"),
        )

        start_unix_date_raw: Optional[bytes] = await redis.get(
            b"daily_reminders:progress:earliest"
        )

        if start_unix_date_raw is None:
            start_unix_date = unix_dates.unix_timestamp_to_unix_date(
                started_at - 60 * 60 * 12, tz=pytz.utc
            )
            logging.info(
                "Daily Reminder Assign Time cursors not initialized, starting iteration on "
                f"{start_unix_date=}"
            )
        else:
            start_unix_date = int(start_unix_date_raw)

        stopper = Stopper(started_at, gd)
        stats = RunStats()
        timezones: Optional[List[str]] = None
        unique_timezones: Set[str] = set()
        unix_date = start_unix_date
        iterating_to_timestamp = started_at + TARGET_END_ITERATION_OFFSET_SECONDS

        earliest_unix_date = start_unix_date

        while not await stopper.check_backpressure(itgs) and not stopper.should_stop():
            timezones_for_date = await get_timezones_from_redis_for_date(
                itgs, unix_date
            )
            if not timezones_for_date:
                if timezones is None:
                    fetch_started_at = time.time()
                    timezones = await get_timezones_from_db(itgs)
                    logging.info(
                        f"Daily Reminders Assign Time Job get_timezones_from_db {len(timezones)=} in {time.time() - fetch_started_at:.3f}s"
                    )
                    if not timezones:
                        logging.info(
                            "Daily Reminders Assign Time job detected there are no timezones, "
                            "which means there are no user daily reminders to assign times for"
                        )
                        stop_reason = "list_exhausted"
                        break

                await initialize_timezones(itgs, unix_date, timezones)
                logging.info(
                    f"Daily Reminders Assign Time Job initialized {len(timezones)} timezones for {unix_date=}"
                )
                timezones_for_date = timezones

            has_pending_timezones = False
            timezone_needs_next_date = False
            for timezone in timezones_for_date:
                if stopper.should_stop():
                    break
                tz = pytz.timezone(timezone)
                unix_date_in_timezone = unix_dates.unix_timestamp_to_unix_date(
                    iterating_to_timestamp, tz=tz
                )
                if unix_date > unix_date_in_timezone:
                    continue

                if unix_date < unix_date_in_timezone:
                    logging.debug(
                        f"{timezone=} is on {unix_dates.unix_date_to_date(unix_date_in_timezone).isoformat()=} at "
                        f"{iterating_to_timestamp=}, so we'll need to check the next date"
                    )
                    timezone_needs_next_date = True

                unique_timezones.add(timezone)
                pair_stats, pair_is_finished = await progress_pair(
                    itgs,
                    unix_date=unix_date,
                    timezone=timezone,
                    tz=tz,
                    stopper=stopper,
                    job_started_at=started_at,
                    iterating_to_timestamp=iterating_to_timestamp,
                )
                if stopper.cache:
                    break
                if not pair_is_finished:
                    has_pending_timezones = True
                stats.add(pair_stats)

            if stopper.cache:
                continue

            if not has_pending_timezones and earliest_unix_date == unix_date:
                logging.debug(
                    f"Daily Reminders Assign Time Job finished all reminders for {unix_date=} ("
                    f"{unix_dates.unix_date_to_date(unix_date).isoformat()})"
                )
                await increment_earliest(itgs, unix_date, timezones_for_date)
                earliest_unix_date += 1

            if not timezone_needs_next_date:
                logging.debug(
                    f"Daily Reminders Assign Time Job checked {unix_date=} ("
                    f"{unix_dates.unix_date_to_date(unix_date).isoformat()}) "
                    f"and none of the {len(timezones_for_date)=} need the next date, "
                    "so cursors have been successfully advanced"
                )
                stopper.on_list_exhausted()
                break

            unix_date += 1

        if stopper.reason == "backpressure":
            await handle_warning(
                f"{__name__}:backpressure",
                f"Daily Reminders Assign Time Job stopping due to backpressure",
            )

        finished_at = time.time()
        running_time = finished_at - started_at
        logging.info(
            f"Daily Reminders Assign Time Job Finished:\n"
            f"- Started At: {started_at}\n"
            f"- Finished At: {finished_at}\n"
            f"- Running Time: {running_time}\n"
            f"- Start Unix Date: {start_unix_date}\n"
            f"- End Unix Date: {unix_date}\n"
            f"- Unique Timezones: {len(unique_timezones)}\n"
            f"- Pairs: {stats.pairs}\n"
            f"- Queries: {stats.queries}\n"
            f"- Attempted: {stats.attempted}\n"
            f"- Overdue: {stats.overdue}\n"
            f"- Stale: {stats.stale}\n"
            f"- SMS Queued: {stats.sms_queued}\n"
            f"- Push Queued: {stats.push_queued}\n"
            f"- Email Queued: {stats.email_queued}\n"
            f"- Stop Reason: {stopper.reason}\n"
        )
        await redis.hset(
            b"stats:daily_reminders:assign_time_job",
            mapping={
                b"finished_at": str(finished_at).encode("ascii"),
                b"running_time": str(running_time).encode("ascii"),
                b"start_unix_date": str(start_unix_date).encode("ascii"),
                b"end_unix_date": str(unix_date).encode("ascii"),
                b"unique_timezones": str(len(unique_timezones)).encode("ascii"),
                b"pairs": str(stats.pairs).encode("ascii"),
                b"queries": str(stats.queries).encode("ascii"),
                b"attempted": str(stats.attempted).encode("ascii"),
                b"overdue": str(stats.overdue).encode("ascii"),
                b"stale": str(stats.stale).encode("ascii"),
                b"sms_queued": str(stats.sms_queued).encode("ascii"),
                b"push_queued": str(stats.push_queued).encode("ascii"),
                b"email_queued": str(stats.email_queued).encode("ascii"),
                b"stop_reason": stopper.reason.encode("ascii"),
            },
        )


@dataclass
class RunStats:
    pairs: int = 0
    queries: int = 0
    attempted: int = 0
    overdue: int = 0
    stale: int = 0
    sms_queued: int = 0
    push_queued: int = 0
    email_queued: int = 0

    def add(self, other: "RunStats") -> None:
        self.pairs += other.pairs
        self.queries += other.queries
        self.attempted += other.attempted
        self.overdue += other.overdue
        self.stale += other.stale
        self.sms_queued += other.sms_queued
        self.push_queued += other.push_queued
        self.email_queued += other.email_queued


class Stopper:
    def __init__(self, started_at: float, gd: GracefulDeath):
        self.started_at = started_at
        self.gd = gd
        self.cache = False
        self.reason: Optional[
            Literal["time_exhausted", "list_exhausted", "signal", "backpressure"]
        ] = None

    def should_stop(self) -> bool:
        """Checks if we should stop either because we've already detected a reason
        to stop or one of the fast to check reasons (time, signal) is true.

        NOTE: this does not check backpressure as that requires querying redis;
        call `check_backpressure` explicitly to check backpressure, or if a
        faster means is available (e.g., combining it with another request), call
        `on_backpressure` if it's detected

        NOTE: this cannot check if the list is exhausted; call `on_list_exhausted`
        when that is detected
        """
        if self.cache:
            return True

        if self.gd.received_term_signal:
            self.reason = "signal"
            self.cache = True
            return True

        if time.time() - self.started_at > MAX_JOB_TIME_SECONDS:
            self.reason = "time_exhausted"
            self.cache = True
            return True

        return self.cache

    def on_backpressure(self):
        if self.cache:
            return

        self.cache = True
        self.reason = "backpressure"

    def on_list_exhausted(self):
        if self.cache:
            return

        self.cache = True
        self.reason = "list_exhausted"

    async def check_backpressure(self, itgs: Itgs) -> bool:
        if self.cache:
            return True

        redis = await itgs.redis()
        queue_length = await redis.zcard(b"daily_reminders:queued")
        if queue_length > QUEUE_BACKPRESSURE_LENGTH:
            self.on_backpressure()


async def get_timezones_from_db(itgs: Itgs) -> List[str]:
    """Fetches a list IANA timezone names from the database such that the list
    at minimum contains all the timezones a user could receive a daily reminder
    in right now
    """
    # PERF: this query is O(n); maintaining a redis set of timezones might be
    # worth it to reduce the cost to O(1), but the factors on this query should
    # be quite small and it should only be called once per day.

    conn = await itgs.conn()
    cursor = conn.cursor("none")
    response = await cursor.execute(
        "SELECT DISTINCT timezone FROM users WHERE timezone IS NOT NULL"
    )

    result = []
    seen_default = False
    for row in response.results:
        timezone = row[0]
        seen_default = seen_default or timezone == DEFAULT_TIMEZONE
        try:
            pytz.timezone(timezone)
            result.append(timezone)
        except Exception as e:
            await handle_error(e, extra_info=f"users table has improper {timezone=}")
    if not seen_default:
        result.append(DEFAULT_TIMEZONE)
    return result


async def get_timezones_from_redis_for_date(itgs: Itgs, unix_date: int) -> List[str]:
    """Fetches the timezones that are being iterated over for the given unix
    date. The list is empty if the timezones have not been initialized yet
    for that date.
    """
    redis = await itgs.redis()
    timezones_raw: Optional[List[bytes]] = await redis.zrange(
        f"daily_reminders:progress:timezones:{unix_date}".encode("ascii"),
        0,
        -1,
    )

    if timezones_raw is None:
        return None

    return [timezone.decode("utf-8") for timezone in timezones_raw]


async def initialize_timezones(itgs: Itgs, unix_date: int, timezones: List[str]):
    """Initializes the timezones list for the given unix date to the specified timezones,
    specified using IANA timezone names, e.g., "America/Los_Angeles".
    """

    redis = await itgs.redis()

    async def prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"daily_reminders:progress:earliest", unix_date)
            await pipe.zadd(
                f"daily_reminders:progress:timezones:{unix_date}".encode("ascii"),
                mapping=dict(
                    (timezone.encode("utf-8"), i)
                    for i, timezone in enumerate(timezones)
                ),
            )
            await pipe.execute()

    await run_with_prep(prep, func)


async def progress_pair(
    itgs: Itgs,
    *,
    unix_date: int,
    timezone: str,
    tz: pytz.BaseTzInfo,
    stopper: Stopper,
    job_started_at: float,
    iterating_to_timestamp: float,
) -> Tuple[RunStats, bool]:
    """Progresses the (unix_date, timezone) pair, assigning times to user
    daily reminder rows that are iterated over.

    This function cannot be parallelized for the same pair, though in theory it
    could be parallelized for different pairs.

    Args:
        itgs (Itgs): the integrations to (re)use
        unix_date (int): the unix date to progress
        timezone (str): the timezone to progress
        tz (pytz.BaseTzInfo): the pytz timezone object for the timezone
        stopper (Stopper): iteration for this function can take a long time, so
            it's necessary to recheck stop reasons at safe points within the
            iteration. if we stop the result will be None and the callee should
            check the stopper for the reason
        job_started_at (float): the job started timestamp, for determining overdue/stale
            entries
        iterating_to_timestamp (float): the unix timestamp that we iterating up
            to.

    Returns:
        Tuple[RunStats, bool]: the run stats for this pair and whether
            the pair is finished
    """
    stats = RunStats(pairs=1)

    redis = await itgs.redis()
    progress_key = f"daily_reminders:progress:{timezone}:{unix_date}".encode("utf-8")
    cursor_info_raw = await redis.hmget(
        progress_key,
        b"start_time",
        b"uid",
        b"finished",
    )

    start_time: Optional[int] = (
        int(cursor_info_raw[0]) if cursor_info_raw[0] is not None else None
    )
    uid: Optional[str] = (
        str(cursor_info_raw[1], "utf-8") if cursor_info_raw[1] is not None else None
    )
    finished: bool = (
        bool(int(cursor_info_raw[2])) if cursor_info_raw[2] is not None else False
    )

    if finished:
        return (stats, True)

    midnight_timestamp = unix_dates.unix_date_to_timestamp(unix_date, tz=tz)

    day_of_week_06_zero_is_monday = unix_dates.unix_date_to_date(unix_date).weekday()
    day_of_week_06_zero_is_sunday = (day_of_week_06_zero_is_monday + 1) % 7
    day_of_week_as_mask = 1 << day_of_week_06_zero_is_sunday

    iterating_to_start_time = iterating_to_timestamp - midnight_timestamp
    if iterating_to_start_time < 0:
        return (stats, False)

    if start_time is not None and start_time > iterating_to_start_time:
        return (stats, False)

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    default_timezone_clause = ""
    if timezone == DEFAULT_TIMEZONE:
        default_timezone_clause = " OR users.timezone IS NULL"

    while not finished and not stopper.should_stop():
        response = await cursor.execute(
            f"""
            SELECT uid, channel, start_time, end_time
            FROM user_daily_reminders
            WHERE
                EXISTS (
                    SELECT 1 FROM users WHERE users.id = user_daily_reminders.user_id
                    AND (users.timezone = ?{default_timezone_clause})
                )
                AND (day_of_week_mask & ?) != 0
                AND (start_time > ? OR (start_time = ? AND uid > ?))
                AND start_time <= ?
            ORDER BY start_time, uid
            LIMIT ?
            """,
            (
                timezone,
                day_of_week_as_mask,
                start_time if start_time is not None else -1,
                start_time if start_time is not None else -1,
                uid if uid is not None else "",  # avoids needing to do substitution
                iterating_to_start_time,
                DATABASE_READ_BATCH_SIZE,
            ),
        )
        stats.queries += 1

        rows = response.results or []

        for redis_batch_start_idx in range(0, len(rows), REDIS_WRITE_BATCH_SIZE):
            redis_batch_end_idx = min(
                redis_batch_start_idx + REDIS_WRITE_BATCH_SIZE, len(rows)
            )

            items: Dict[bytes, float] = dict()
            redis_stats = DailyReminderStatsPreparer()

            for item_index in range(redis_batch_start_idx, redis_batch_end_idx):
                row = rows[item_index]
                uid: str = row[0]
                channel: str = row[1]
                start_time: int = row[2]
                end_time: int = row[3]

                start_time_as_timestamp = midnight_timestamp + start_time
                end_time_as_timestamp = midnight_timestamp + end_time

                redis_stats.incr_attempted(unix_date)
                stats.attempted += 1
                if start_time_as_timestamp < job_started_at:
                    redis_stats.incr_overdue(unix_date)
                    stats.overdue += 1

                    if end_time_as_timestamp < job_started_at - STALE_THRESHOLD_SECONDS:
                        redis_stats.incr_skipped_assigning_time(
                            unix_date, channel=channel
                        )
                        stats.stale += 1
                        continue

                assigned_timestamp = int(
                    max(
                        job_started_at,
                        (
                            start_time_as_timestamp
                            + (
                                (end_time_as_timestamp - start_time_as_timestamp)
                                * random.random()
                            )
                        ),
                    )
                )
                redis_stats.incr_time_assigned(unix_date, channel=channel)
                if channel == "sms":
                    stats.sms_queued += 1
                elif channel == "push":
                    stats.push_queued += 1
                elif channel == "email":
                    stats.email_queued += 1
                items[
                    json.dumps(
                        {"uid": uid, "unix_date": unix_date}, sort_keys=True
                    ).encode("utf-8")
                ] = assigned_timestamp

            async def _prep(force: bool):
                await ensure_set_if_lower_script_exists(redis, force=force)

            async def _func():
                async with redis.pipeline() as pipe:
                    pipe.multi()
                    await redis_stats.write_earliest(pipe)
                    if items:
                        await pipe.zadd(b"daily_reminders:queued", mapping=items)
                        await pipe.hset(
                            progress_key,
                            mapping={
                                b"start_time": str(start_time).encode("ascii"),
                                b"uid": uid.encode("utf-8"),
                            },
                        )
                    await redis_stats.write_increments(pipe)
                    await pipe.execute()

            await run_with_prep(_prep, _func)

        if len(rows) < DATABASE_READ_BATCH_SIZE:
            if iterating_to_start_time > 86400:
                finished = True
                await redis.hset(
                    progress_key,
                    mapping={
                        b"finished": str(int(finished)).encode("ascii"),
                    },
                )
            break

    return (stats, finished)


async def increment_earliest(itgs: Itgs, from_unix_date: int, timezones: List[str]):
    """Assuming that daily_reminders:progress:earliest is currently set to from_unix_date,
    and that daily_reminders:progress:timezones:{from_unix_date} contains the given
    list of timezones, this function increments daily_reminders:progress:earliest
    and deletes the data for the from_unix_date.
    """
    redis = await itgs.redis()

    async with redis.pipeline() as pipe:
        pipe.multi()
        for timezone in timezones:
            await pipe.delete(
                f"daily_reminders:progress:{timezone}:{from_unix_date}".encode("utf-8")
            )
        await pipe.delete(
            f"daily_reminders:progress:timezones:{from_unix_date}".encode("utf-8")
        )
        await pipe.set(b"daily_reminders:progress:earliest", from_unix_date + 1)
        await pipe.execute()


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.daily_reminders.assign_time")

    asyncio.run(main())
