"""Daily Reminders Send Job"""

import io
import json
import os
import time
from typing import Any, Dict, List, Literal, Optional, Protocol, Sequence, Tuple, cast
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from dataclasses import dataclass

from lib.basic_redis_lock import basic_redis_lock
from lib.daily_reminders.stats import DailyReminderStatsPreparer
from lib.shared.job_callback import JobCallback
from lib.touch.link_info import TouchLink
from lib.touch.links import abandon_link, create_buffered_link
from lib.touch.send import (
    encode_touch,
    initialize_touch,
    prepare_send_touch,
    send_touch_in_pipe,
)
from lib.touch.touch_info import TouchToSend
from lib.users.streak import UserStreak, read_user_streak, days_of_week
from lib.users.time_of_day import get_time_of_day
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.zshift import ensure_zshift_script_exists, zshift
import unix_dates
import pytz

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 50
"""The maximum duration in seconds this job can run before stopping itself to allow
other jobs to run
"""

JOB_BATCH_SIZE = 400
"""How many items we move to purgatory at a time. This is done without
actually reading the items, so it's a pretty cheap operation.
"""

REDIS_READ_BATCH_SIZE = 12
"""How many items we read from redis at a time"""

DATABASE_READ_BATCH_SIZE = 100
"""How many items we read from the database at a time"""

REDIS_WRITE_BATCH_SIZE = 12
"""How many items we write to redis at a time"""

STALE_THRESHOLD_SECONDS = 60 * 60
"""How long after the assigned time in seconds before we drop the reminder
to help catch up / avoid sending messages way outside the users' preferred
time
"""


@dataclass
class DailyRemindersSwap:
    slug: str
    """A unique slug, which is used for building the key in redis which
    contains the users which have already received the swap on the channel.
    Ex: 'oseh_30_launch'
    """

    start_unix_date: int
    """The earliest unix date which we are allowed to swap. Should be at least
    2 days ahead of when you configure the swap to allow for different timezones
    """

    end_unix_date: int
    """The final unix date which we are done trying to swap. Should not be more than
    7 days after. We will expire the redis key based on this date
    """

    touch_point_event_slug: str
    """The touch point event slug we send instead of the daily reminder"""

    max_created_at: Optional[int] = None
    """If specified, users created at strictly after this number of seconds since the
    unix epoch will be excluded from the swap
    """

    # we always include unsubscribe_url for the email channel


CURRENT_SWAP: Optional[DailyRemindersSwap] = cast(Optional[DailyRemindersSwap], None)


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Pulls batches off the queued sorted set and moves them to purgatory. The
    items are processed by using the corresponding row in user daily reminders
    to send the appropriate reminder. Finally, the batch is removed from
    purgatory.

    PERF:
        This performs the steps within the batch in a sequence; it would be
        possible to e.g., start fetching the first subbatch from the database
        while still retrieving the remainder of the batch from redis

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at = time.time()
    async with basic_redis_lock(
        itgs, b"daily_reminders:send_job_lock", gd=gd, spin=False
    ):
        await (await itgs.redis()).hset(  # type: ignore
            b"stats:daily_reminders:send_job",  # type: ignore
            mapping={b"started_at": str(started_at).encode("ascii")},
        )

        stats = RunStats()
        stop_reason: Optional[str] = None
        expect_empty_purgatory: bool = False
        is_last_iteration: bool = False
        root_frontend_url = os.environ["ROOT_FRONTEND_URL"]

        while True:
            if is_last_iteration:
                stop_reason = "list_exhausted"
                break

            if gd.received_term_signal:
                stop_reason = "signal"
                break

            if time.time() - started_at > MAX_JOB_TIME_SECONDS:
                stop_reason = "time_exhausted"
                break

            await itgs.ensure_redis_liveliness()
            redis = await itgs.redis()

            purgatory_size = await redis.zcard(b"daily_reminders:send_purgatory")
            if purgatory_size != 0:
                assert (
                    not expect_empty_purgatory
                ), f"Found {purgatory_size} items in purgatory when not on first batch"
                await handle_warning(
                    f"{__name__}:recovery", f"Recovering {purgatory_size=} items"
                )
            else:
                purgatory_size = await run_with_prep(
                    lambda force: ensure_zshift_script_exists(redis, force=force),
                    lambda: zshift(
                        redis,
                        b"daily_reminders:queued",
                        b"daily_reminders:send_purgatory",
                        JOB_BATCH_SIZE,
                        started_at,
                    ),
                )
                assert purgatory_size is not None
                is_last_iteration = purgatory_size < JOB_BATCH_SIZE

            if purgatory_size == 0:
                stop_reason = "list_exhausted"
                break

            logging.debug(
                f"Daily Reminders Send Job received batch of {purgatory_size} items ({is_last_iteration=})"
            )
            expect_empty_purgatory = True

            batch: List[BatchItem] = []
            for start_index in range(0, purgatory_size, REDIS_READ_BATCH_SIZE):
                if gd.received_term_signal:
                    break
                end_index = min(start_index + REDIS_READ_BATCH_SIZE, purgatory_size)

                result = await redis.zrange(
                    b"daily_reminders:send_purgatory",
                    start_index,
                    end_index - 1,
                    withscores=True,
                )
                assert (
                    len(result) == end_index - start_index
                ), f"{len(result)=} != {end_index - start_index=}"

                for item, score in result:
                    parsed_item = json.loads(item)
                    assert isinstance(parsed_item, dict)
                    uid = parsed_item["uid"]
                    unix_date = parsed_item["unix_date"]
                    assert isinstance(uid, str)
                    assert isinstance(unix_date, int)
                    batch.append(
                        BatchItem(uid=uid, score=score, unix_date=unix_date, key=item)
                    )

            if gd.received_term_signal:
                stop_reason = "signal"
                break
            del redis

            logging.debug(
                f"Daily Reminders Send Job fetched {len(batch)} items from redis"
            )

            augmented_batch = await augment_batch(itgs, batch, gd=gd, now=time.time())
            if gd.received_term_signal:
                stop_reason = "signal"
                break

            lost = [itm for itm in augmented_batch if itm.info is None]
            found = (
                cast(List[RequiredAugmentedBatchItem], augmented_batch)
                if not lost
                else [
                    cast(RequiredAugmentedBatchItem, itm)
                    for itm in augmented_batch
                    if itm.info is not None
                ]
            )

            if lost:
                redis_stats = DailyReminderStatsPreparer()
                for itm in lost:
                    redis_stats.incr_sends_attempted(itm.item.unix_date)
                    redis_stats.incr_sends_lost(itm.item.unix_date)
                await redis_stats.store(itgs)
                stats.attempted += len(lost)
                stats.lost += len(lost)
                await zrem_in_batches(
                    itgs,
                    b"daily_reminders:send_purgatory",
                    [itm.item.key for itm in lost],
                )
                del lost

            stale = [
                itm
                for itm in found
                if itm.item.score < started_at - STALE_THRESHOLD_SECONDS
            ]
            sendable = (
                found
                if not stale
                else [
                    itm
                    for itm in found
                    if itm.item.score >= started_at - STALE_THRESHOLD_SECONDS
                ]
            )

            if stale:
                await handle_warning(
                    f"{__name__}:stale", f"Daily Reminders send job have {len(stale)=}"
                )
                redis_stats = DailyReminderStatsPreparer()
                for itm in stale:
                    redis_stats.incr_sends_attempted(itm.item.unix_date)
                    redis_stats.incr_skipped_sending(
                        itm.item.unix_date, channel=itm.info.channel
                    )
                await redis_stats.store(itgs)
                stats.attempted += len(stale)
                stats.stale += len(stale)
                await zrem_in_batches(
                    itgs,
                    b"daily_reminders:send_purgatory",
                    [itm.item.key for itm in stale],
                )
                del stale

            prepared: List[PreparedReminder] = []
            for itm in sendable:
                if gd.received_term_signal:
                    break

                swap: Optional[DailyRemindersSwap] = None
                if (
                    CURRENT_SWAP is not None
                    and CURRENT_SWAP.start_unix_date
                    <= itm.item.unix_date
                    <= CURRENT_SWAP.end_unix_date
                    and (
                        CURRENT_SWAP.max_created_at is None
                        or itm.info.user_created_at <= CURRENT_SWAP.max_created_at
                    )
                ):
                    swap_key = f"daily_reminders:swaps:{CURRENT_SWAP.slug}:{itm.info.channel}".encode(
                        "utf-8"
                    )
                    await itgs.ensure_redis_liveliness()
                    redis = await itgs.redis()
                    async with redis.pipeline() as pipe:
                        pipe.multi()
                        await pipe.sadd(
                            swap_key,  # type: ignore
                            itm.info.user_sub.encode("utf-8"),
                        )
                        await pipe.expireat(
                            swap_key,
                            int(
                                unix_dates.unix_date_to_timestamp(
                                    CURRENT_SWAP.end_unix_date + 2, tz=pytz.utc
                                )
                            ),
                        )
                        swap_redis_response = await pipe.execute()
                    del redis
                    if swap_redis_response[0] > 0:
                        swap = CURRENT_SWAP
                        stats.swaps += 1

                touch_at = time.time()
                touch_weekday = unix_dates.unix_date_to_date(
                    unix_dates.unix_timestamp_to_unix_date(
                        touch_at, tz=itm.info.timezone
                    )
                ).weekday()
                if swap is not None:
                    touch_point_event_slug = swap.touch_point_event_slug
                elif (
                    itm.info.streak.goal_days_per_week is not None
                    and len(itm.info.streak.days_of_week)
                    == itm.info.streak.goal_days_per_week - 1
                    and days_of_week[touch_weekday] not in itm.info.streak.days_of_week
                ):
                    if touch_weekday == 6:
                        touch_point_event_slug = "daily_reminder_almost_miss_goal"
                    else:
                        touch_point_event_slug = "daily_reminder_almost_goal"
                elif itm.info.engaged:
                    touch_point_event_slug = "daily_reminder_engaged"
                else:
                    touch_point_event_slug = "daily_reminder_disengaged"

                touch = initialize_touch(
                    user_sub=itm.info.user_sub,
                    touch_point_event_slug=touch_point_event_slug,
                    channel=itm.info.channel,
                    event_parameters={
                        "name": itm.info.name,
                        "time_of_day": get_time_of_day(
                            touch_at, itm.info.timezone
                        ).value,
                        "streak": f"{itm.info.streak.streak} day{itm.info.streak.streak != 1 and 's' or ''}",
                        "goal": (
                            f"{len(itm.info.streak.days_of_week)} of {itm.info.streak.goal_days_per_week}"
                            if itm.info.streak.goal_days_per_week is not None
                            else "Not set"
                        ),
                        "goal_simple": (
                            f"{itm.info.streak.goal_days_per_week}"
                            if itm.info.streak.goal_days_per_week is not None
                            else "0"
                        ),
                        "goal_badge_url": (
                            f"{root_frontend_url}/goalBadge/{min(len(itm.info.streak.days_of_week), itm.info.streak.goal_days_per_week or 3)}of{itm.info.streak.goal_days_per_week or 3}-192h.png"
                        ),
                        **(
                            {"ss_reset": True}
                            if itm.info.engaged
                            and touch_point_event_slug == "daily_reminder_disengaged"
                            else {}
                        ),
                    },
                    success_callback=JobCallback(
                        name="runners.touch.persist_links", kwargs={"codes": []}
                    ),
                    failure_callback=JobCallback(
                        name="runners.touch.abandon_links", kwargs={"codes": []}
                    ),
                )
                assert touch.success_callback is not None
                assert touch.failure_callback is not None

                link: Optional[TouchLink] = None
                if touch.channel != "push":
                    link = await create_buffered_link(
                        itgs,
                        touch_uid=touch.uid,
                        page_identifier="home",
                        page_extra={},
                        preview_identifier="default",
                        preview_extra={},
                        now=touch.queued_at,
                        code_style="short" if touch.channel == "sms" else "normal",
                    )
                    main_url = f"{root_frontend_url}/a/{link.code}"
                    if touch.channel == "sms" and main_url.startswith("https://"):
                        main_url = main_url[len("https://") :]
                    touch.event_parameters["url"] = main_url
                    touch.success_callback.kwargs["codes"].append(link.code)
                    touch.failure_callback.kwargs["codes"].append(link.code)

                unsubscribe_link: Optional[TouchLink] = None
                if touch.channel == "email":
                    unsubscribe_link = await create_buffered_link(
                        itgs,
                        touch_uid=touch.uid,
                        page_identifier="unsubscribe",
                        page_extra={},
                        preview_identifier="unsubscribe",
                        preview_extra={"list": "daily reminders"},
                        now=touch.queued_at,
                        code_style="long",
                    )
                    touch.event_parameters["unsubscribe_url"] = (
                        f"{root_frontend_url}/l/{unsubscribe_link.code}"
                    )
                    touch.success_callback.kwargs["codes"].append(unsubscribe_link.code)
                    touch.failure_callback.kwargs["codes"].append(unsubscribe_link.code)

                if link is None and unsubscribe_link is None:
                    touch.success_callback = None
                    touch.failure_callback = None

                prepared.append(
                    PreparedReminder(
                        item=itm.item,
                        info=itm.info,
                        touch=touch,
                        link=link,
                        unsubscribe_link=unsubscribe_link,
                        enc_touch=encode_touch(touch),
                    )
                )

            if gd.received_term_signal:
                for itm in prepared:
                    if itm.link is not None:
                        await abandon_link(itgs, code=itm.link.code)
                    if itm.unsubscribe_link is not None:
                        await abandon_link(itgs, code=itm.unsubscribe_link.code)
                stop_reason = "signal"
                break

            logging.debug(f"Daily Reminders Send Job prepared {len(prepared)} items")

            await itgs.ensure_redis_liveliness()
            redis = await itgs.redis()

            written_to_idx = 0
            got_backpressure = False
            redis_stats = DailyReminderStatsPreparer()
            user_subs_to_mark_engaged: List[str] = []
            user_subs_to_mark_disengaged: List[str] = []
            for start_idx in range(0, len(prepared), REDIS_WRITE_BATCH_SIZE):
                if gd.received_term_signal or got_backpressure:
                    break
                end_idx = min(start_idx + REDIS_WRITE_BATCH_SIZE, len(prepared))

                async def prep(force: bool):
                    await prepare_send_touch(redis, force=force)

                async def func():
                    async with redis.pipeline() as pipe:
                        pipe.multi()
                        for idx in range(start_idx, end_idx):
                            await send_touch_in_pipe(
                                pipe, prepared[idx].touch, prepared[idx].enc_touch
                            )
                        return await pipe.execute()

                pipe_result = await run_with_prep(prep, func)
                assert isinstance(pipe_result, list)
                assert len(pipe_result) == end_idx - start_idx

                pipe_succeeded = [bool(r) for r in pipe_result]
                keys_to_zrem: List[bytes] = []
                for itm, succeeded in zip(
                    (prepared[i] for i in range(start_idx, end_idx)), pipe_succeeded
                ):
                    if not succeeded:
                        got_backpressure = True
                        if itm.link is not None:
                            await abandon_link(itgs, code=itm.link.code)
                        if itm.unsubscribe_link is not None:
                            await abandon_link(itgs, code=itm.unsubscribe_link.code)
                    else:
                        num_links = 0
                        num_links += 1 if itm.link is not None else 0
                        num_links += 1 if itm.unsubscribe_link is not None else 0

                        stats.attempted += 1
                        stats.links += num_links
                        stats.sms += 1 if itm.info.channel == "sms" else 0
                        stats.push += 1 if itm.info.channel == "push" else 0
                        stats.email += 1 if itm.info.channel == "email" else 0
                        redis_stats.incr_sends_attempted(itm.item.unix_date)
                        redis_stats.incr_links(itm.item.unix_date, amt=num_links)
                        redis_stats.incr_sent(
                            itm.item.unix_date, channel=itm.info.channel
                        )

                        keys_to_zrem.append(itm.item.key)

                        if (
                            not itm.info.was_engaged
                            and itm.touch.touch_point_event_slug
                            == "daily_reminder_engaged"
                        ):
                            user_subs_to_mark_engaged.append(itm.info.user_sub)
                        elif (
                            itm.info.was_engaged
                            and itm.touch.touch_point_event_slug
                            == "daily_reminder_disengaged"
                        ):
                            user_subs_to_mark_disengaged.append(itm.info.user_sub)

                written_to_idx = end_idx + 1
                await itgs.ensure_redis_liveliness()
                await (await itgs.redis()).zrem(
                    b"daily_reminders:send_purgatory", *keys_to_zrem
                )

            del redis

            await redis_stats.store(itgs)

            update_engaged_queries: List[Tuple[str, Sequence[Any]]] = []
            if query_and_qargs := create_update_user_subs_engaged_query(
                user_subs_to_mark_engaged, True
            ):
                stats.marked_engaged += len(user_subs_to_mark_engaged)
                update_engaged_queries.append(query_and_qargs)
            if query_and_qargs := create_update_user_subs_engaged_query(
                user_subs_to_mark_disengaged, False
            ):
                stats.marked_disengaged += len(user_subs_to_mark_disengaged)
                update_engaged_queries.append(query_and_qargs)
            if update_engaged_queries:
                conn = await itgs.conn()
                cursor = conn.cursor()
                await cursor.executemany3(update_engaged_queries)

            if gd.received_term_signal or got_backpressure:
                for itm in prepared[written_to_idx:]:
                    if itm.link is not None:
                        await abandon_link(itgs, code=itm.link.code)
                    if itm.unsubscribe_link is not None:
                        await abandon_link(itgs, code=itm.unsubscribe_link.code)
                stop_reason = "backpressure" if got_backpressure else "signal"
                break

        if stop_reason == "backpressure":
            await handle_warning(
                f"{__name__}:backpressure",
                f"Daily Reminders Send Job stopping due to backpressure",
            )

        finished_at = time.time()
        running_time = finished_at - started_at
        logging.info(
            f"Daily Reminders Send Job Finished:\n"
            f"- Started At: {started_at}\n"
            f"- Finished At: {finished_at}\n"
            f"- Running Time: {running_time}\n"
            f"- Attempted: {stats.attempted}\n"
            f"- Lost: {stats.lost}\n"
            f"- Stale: {stats.stale}\n"
            f"- Links: {stats.links}\n"
            f"- SMS: {stats.sms}\n"
            f"- Push: {stats.push}\n"
            f"- Email: {stats.email}\n"
            f"- Swaps: {stats.swaps}\n"
            f"- Marked Engaged: {stats.marked_engaged}\n"
            f"- Marked Disengaged: {stats.marked_disengaged}\n"
            f"- Stop Reason: {stop_reason}\n"
        )
        await itgs.ensure_redis_liveliness()
        await (await itgs.redis()).hset(
            b"stats:daily_reminders:send_job",  # type: ignore
            mapping={
                b"finished_at": str(finished_at).encode("ascii"),
                b"running_time": str(running_time).encode("ascii"),
                b"attempted": str(stats.attempted).encode("ascii"),
                b"lost": str(stats.lost).encode("ascii"),
                b"stale": str(stats.stale).encode("ascii"),
                b"links": str(stats.links).encode("ascii"),
                b"sms": str(stats.sms).encode("ascii"),
                b"push": str(stats.push).encode("ascii"),
                b"email": str(stats.email).encode("ascii"),
                b"swaps": str(stats.swaps).encode("ascii"),
                b"marked_engaged": str(stats.marked_engaged).encode("ascii"),
                b"marked_disengaged": str(stats.marked_disengaged).encode("ascii"),
                b"stop_reason": stop_reason.encode("ascii"),
            },
        )


@dataclass
class RunStats:
    attempted: int = 0
    lost: int = 0
    stale: int = 0
    links: int = 0
    sms: int = 0
    push: int = 0
    email: int = 0
    swaps: int = 0
    marked_engaged: int = 0
    marked_disengaged: int = 0


@dataclass
class BatchItem:
    uid: str
    score: float
    unix_date: int
    key: bytes


@dataclass
class _Phase1BatchItemAugmentedInfo:
    user_sub: str
    name: str
    channel: Literal["push", "sms", "email"]
    user_created_at: float
    timezone: pytz.BaseTzInfo
    engaged: bool
    was_engaged: bool
    is_very_disengaged: bool
    goal_days_per_week: Optional[int]


@dataclass
class BatchItemAugmentedInfo:
    user_sub: str
    name: str
    channel: Literal["push", "sms", "email"]
    user_created_at: float
    timezone: pytz.BaseTzInfo
    engaged: bool
    was_engaged: bool
    streak: UserStreak


@dataclass
class AugmentedBatchItem:
    item: BatchItem
    info: Optional[BatchItemAugmentedInfo]


class RequiredAugmentedBatchItem(Protocol):
    item: BatchItem
    info: BatchItemAugmentedInfo


@dataclass
class PreparedReminder:
    item: BatchItem
    info: BatchItemAugmentedInfo
    touch: TouchToSend
    link: Optional[TouchLink]
    """sms, email"""
    unsubscribe_link: Optional[TouchLink]
    """only for email"""
    enc_touch: bytes


def _create_augment_query(length: int) -> str:
    assert length >= 1

    result = io.StringIO()
    result.write("WITH batch(uid, unix_date) AS (VALUES (?, ?)")
    for _ in range(length - 1):
        result.write(", (?, ?)")
    result.write(
        ") SELECT"
        " batch.uid,"
        " users.sub,"
        " users.given_name,"
        " user_daily_reminders.channel,"
        " users.created_at,"
        " users.timezone,"
        " (CASE"
        " WHEN users.created_at < ? THEN"
        " EXISTS ("
        "SELECT 1 FROM user_journeys "
        "WHERE"
        " user_journeys.user_id = users.id"
        " AND user_journeys.created_at_unix_date >= batch.unix_date - 3"
        " )"
        " ELSE"
        " EXISTS ("
        "SELECT 1 FROM user_journeys "
        "WHERE"
        " user_journeys.user_id = users.id"
        " AND user_journeys.created_at_unix_date >= batch.unix_date - 1"
        " )"
        " END) AS engaged,"
        " user_daily_reminder_engaged.engaged,"
        " NOT EXISTS ("
        "SELECT 1 FROM user_journeys "
        "WHERE"
        " user_journeys.user_id = users.id"
        " AND user_journeys.created_at_unix_date >= batch.unix_date - 10"
        " ) AS is_very_disengaged,"
        " user_goals.days_per_week "
        "FROM batch "
        "JOIN user_daily_reminders ON user_daily_reminders.uid = batch.uid "
        "JOIN users ON users.id = user_daily_reminders.user_id "
        "LEFT JOIN user_daily_reminder_engaged ON user_daily_reminder_engaged.user_id = users.id "
        "LEFT JOIN user_goals ON user_goals.user_id = users.id"
    )
    return result.getvalue()


_max_length_query: Optional[str] = None


def _get_or_create_augment_query(length: int) -> str:
    global _max_length_query

    if length == DATABASE_READ_BATCH_SIZE:
        if _max_length_query is not None:
            return _max_length_query
        _max_length_query = _create_augment_query(length)
        return _max_length_query

    return _create_augment_query(length)


async def augment_batch(
    itgs: Itgs, batch: List[BatchItem], *, gd: GracefulDeath, now: float
) -> List[AugmentedBatchItem]:
    if not batch:
        return []

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    result: List[AugmentedBatchItem] = []
    for start_idx in range(0, len(batch), DATABASE_READ_BATCH_SIZE):
        if gd.received_term_signal:
            return result

        end_idx = min(start_idx + DATABASE_READ_BATCH_SIZE, len(batch))

        qargs = []
        for idx in range(start_idx, end_idx):
            item = batch[idx]
            qargs.append(item.uid)
            qargs.append(item.unix_date)
        qargs.append(now - 60 * 60 * 24 * 7)

        response = await cursor.execute(
            _get_or_create_augment_query(end_idx - start_idx),
            qargs,
        )

        info_by_uid: Dict[str, _Phase1BatchItemAugmentedInfo] = dict()
        for row in response.results or []:
            if gd.received_term_signal:
                return result
            try:
                user_tz = pytz.timezone(
                    row[5] if row[5] is not None else "America/Los_Angeles"
                )
            except:
                user_tz = pytz.timezone("America/Los_Angeles")

            info_by_uid[row[0]] = _Phase1BatchItemAugmentedInfo(
                user_sub=row[1],
                name=row[2] if row[2] is not None else "there",
                channel=row[3],
                user_created_at=row[4],
                timezone=user_tz,
                engaged=bool(row[6]),
                was_engaged=bool(row[7]),
                is_very_disengaged=bool(row[8]),
                goal_days_per_week=row[9],
            )

        # you could try to pipeline cached streaks, but we don't expect many to be
        # in the cache here at this time since user streak caches are very short

        await itgs.ensure_redis_liveliness()

        for idx in range(start_idx, end_idx):
            if gd.received_term_signal:
                return result
            item = batch[idx]
            info_p1 = info_by_uid.get(item.uid)
            if info_p1 is None:
                result.append(AugmentedBatchItem(item=item, info=None))
                continue
            if not info_p1.is_very_disengaged:
                streak = await read_user_streak(
                    itgs,
                    gd,
                    sub=info_p1.user_sub,
                    prefer="model",
                    user_tz=info_p1.timezone,
                )
            else:
                streak = UserStreak(
                    streak=0,
                    days_of_week=[],
                    goal_days_per_week=info_p1.goal_days_per_week,
                    journeys=0,
                    prev_best_all_time_streak=0,
                    checked_at=int(now),
                )
            result.append(
                AugmentedBatchItem(
                    item=item,
                    info=BatchItemAugmentedInfo(
                        user_sub=info_p1.user_sub,
                        name=info_p1.name,
                        channel=info_p1.channel,
                        user_created_at=info_p1.user_created_at,
                        timezone=info_p1.timezone,
                        engaged=info_p1.engaged,
                        was_engaged=info_p1.was_engaged,
                        streak=streak,
                    ),
                )
            )

    return result


async def zrem_in_batches(itgs: Itgs, key: bytes, members: List[bytes]):
    redis = await itgs.redis()

    for start_idx in range(0, len(members), REDIS_WRITE_BATCH_SIZE):
        end_idx = min(start_idx + REDIS_WRITE_BATCH_SIZE, len(members))
        await redis.zrem(key, *members[start_idx:end_idx])


def create_update_user_subs_engaged_query(
    user_subs: List[str], engaged: bool
) -> Optional[Tuple[str, Sequence[Any]]]:
    if not user_subs:
        return

    query = io.StringIO()
    query.write("WITH batch(sub) AS (VALUES (?)")
    for _ in range(1, len(user_subs)):
        query.write(", (?)")

    query.write(
        ") INSERT INTO user_daily_reminder_engaged (user_id, engaged) "
        "SELECT"
        f" users.id, {int(engaged)} "
        "FROM batch, users "
        "WHERE"
        " batch.sub = users.sub"
        f"ON CONFLICT (user_id) DO UPDATE SET engaged={int(engaged)}"
    )

    return (query.getvalue(), user_subs)


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.daily_reminders.send")

    asyncio.run(main())
