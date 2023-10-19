"""Daily Reminders Send Job"""
import io
import json
import os
import time
from typing import Dict, List, Literal, Optional
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
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.zshift import ensure_zshift_script_exists, zshift

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
        redis = await itgs.redis()
        await redis.hset(
            b"stats:daily_reminders:send_job",
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

            logging.debug(
                f"Daily Reminders Send Job fetched {len(batch)} items from redis"
            )

            augmented_batch = await augment_batch(itgs, batch, gd=gd)
            if gd.received_term_signal:
                stop_reason = "signal"
                break

            lost = [itm for itm in augmented_batch if itm.info is None]
            found = (
                augmented_batch
                if not lost
                else [itm for itm in augmented_batch if itm.info is not None]
            )

            if lost:
                await handle_warning(
                    f"{__name__}:lost", f"Daily Reminders send job have {len(lost)=}"
                )
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
                touch = initialize_touch(
                    user_sub=itm.info.user_sub,
                    touch_point_event_slug="daily_reminder",
                    channel=itm.info.channel,
                    event_parameters={},
                    success_callback=JobCallback(
                        name="runners.touch.persist_links", kwargs={"codes": []}
                    ),
                    failure_callback=JobCallback(
                        name="runners.touch.abandon_links", kwargs={"codes": []}
                    ),
                )

                if touch.channel == "email":
                    touch.event_parameters["name"] = itm.info.name

                link: Optional[TouchLink] = None
                if touch.channel in ("email", "sms"):
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
                    touch.event_parameters[
                        "unsubscribe_url"
                    ] = f"{root_frontend_url}/l/{unsubscribe_link.code}"
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

            written_to_idx = 0
            got_backpressure = False
            redis_stats = DailyReminderStatsPreparer()
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

                written_to_idx = end_idx + 1
                await redis.zrem(b"daily_reminders:send_purgatory", *keys_to_zrem)

            await redis_stats.store(itgs)

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
            f"- Stop Reason: {stop_reason}\n"
        )
        await redis.hset(
            b"stats:daily_reminders:send_job",
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


@dataclass
class BatchItem:
    uid: str
    score: float
    unix_date: int
    key: bytes


@dataclass
class BatchItemAugmentedInfo:
    user_sub: str
    name: str
    channel: Literal["push", "sms", "email"]


@dataclass
class AugmentedBatchItem:
    item: BatchItem
    info: Optional[BatchItemAugmentedInfo]


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
    result.write("WITH batch(uid) AS (VALUES (?)")
    for _ in range(length - 1):
        result.write(", (?)")
    result.write(
        ") SELECT batch.uid, users.sub, users.given_name, user_daily_reminders.channel "
        "FROM batch "
        "JOIN user_daily_reminders ON user_daily_reminders.uid = batch.uid "
        "JOIN users ON users.id = user_daily_reminders.user_id"
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
    itgs: Itgs, batch: List[BatchItem], *, gd: GracefulDeath
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

        response = await cursor.execute(
            _get_or_create_augment_query(end_idx - start_idx),
            tuple(batch[idx].uid for idx in range(start_idx, end_idx)),
        )

        info_by_uid: Dict[str, BatchItemAugmentedInfo] = dict()
        for row in response.results or []:
            info_by_uid[row[0]] = BatchItemAugmentedInfo(
                user_sub=row[1],
                name=row[2] if row[2] is not None else "there",
                channel=row[3],
            )

        for idx in range(start_idx, end_idx):
            item = batch[idx]
            info = info_by_uid.get(item.uid)
            result.append(AugmentedBatchItem(item=item, info=info))

    return result


async def zrem_in_batches(itgs: Itgs, key: bytes, members: List[bytes]):
    redis = await itgs.redis()

    for start_idx in range(0, len(members), REDIS_WRITE_BATCH_SIZE):
        end_idx = min(start_idx + REDIS_WRITE_BATCH_SIZE, len(members))
        await redis.zrem(key, *members[start_idx:end_idx])


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.daily_reminders.send")

    asyncio.run(main())
