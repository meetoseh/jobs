"""Checks for any journal chat jobs in purgatory too long"""

from typing import Awaitable, Dict, List, Union, cast

import pytz
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import time

from lib.journals.journal_chat_job_stats import JobType, JournalChatJobStats
from lib.journals.journal_chat_redis_packet import (
    EventBatchPacketDataItemDataError,
    JournalChatRedisPacketPassthrough,
)
from lib.journals.journal_chat_task import JournalChatTask
from lib.journals.master_keys import (
    get_journal_master_key_for_decryption,
    get_journal_master_key_for_encryption,
)
from lib.journals.serialize_journal_chat_event import serialize_journal_chat_event
from lib.redis_stats_preparer import RedisStatsPreparer
from lib.shared.redis_hash import RedisHash
from redis_helpers.journal_chat_job_finish import safe_journal_chat_job_finish
import unix_dates

category = JobCategory.LOW_RESOURCE_COST

MAX_TIME_IN_PURGATORY = 3600
"""The maximum time a job can be in purgatory before we purge it and report to slack"""


MAX_JOB_TIME_SECONDS = 10
"""The maximum duration in seconds this job can run before stopping itself to allow
other jobs to run
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Checks for journal chat jobs in purgatory too long and removes them

    PERF: currently assumes relatively low throughput

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    started_at_wall = time.time()
    started_at_perf = time.perf_counter()
    redis = await itgs.redis()

    while True:
        if time.perf_counter() - started_at_perf > MAX_JOB_TIME_SECONDS:
            await handle_warning(
                f"{__name__}:job_took_too_long",
                f"Job took longer than {MAX_JOB_TIME_SECONDS} seconds, ending early!",
            )
            break

        response = await redis.zrange(
            b"journals:journal_chat_jobs:purgatory",
            b"-inf",  # type: ignore
            str(int(started_at_wall - MAX_TIME_IN_PURGATORY)).encode("ascii"),  # type: ignore
            byscore=True,
            offset=0,
            num=1,
        )

        if not response:
            logging.debug("no more journal chat jobs in purgatory")
            break

        jc_uid = cast(bytes, response[0]).decode("utf-8")
        hash_info_raw = await cast(
            Awaitable[
                Union[
                    List[Union[str, bytes]],
                    Dict[Union[str, bytes], Union[str, bytes]],
                ]
            ],
            redis.hgetall(
                f"journals:journal_chat_jobs:{jc_uid}".encode("utf-8")  # type: ignore
            ),
        )
        hash_info = RedisHash(hash_info_raw)

        user_sub = hash_info.get_str(b"user_sub")
        master_key = await get_journal_master_key_for_encryption(
            itgs, user_sub=user_sub, now=time.time()
        )
        if master_key.type != "success":
            await handle_warning(
                f"{__name__}:master_key_not_found",
                f"Failed to delete stuck job for `{user_sub}`; master key not found",
            )
            return

        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=jc_uid.encode("utf-8"),
            log_id=hash_info.get_bytes(b"log_id"),
            last_event=serialize_journal_chat_event(
                journal_master_key=master_key,
                event=JournalChatRedisPacketPassthrough(
                    counter=-1,
                    type="passthrough",
                    event=EventBatchPacketDataItemDataError(
                        type="error",
                        message="Failed to create response",
                        detail="timeout",
                    ),
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        await handle_warning(
            f"{__name__}:job_purged",
            f"Purged stuck job for `{user_sub}`\n\n```\n{dict(hash_info.items_bytes())}\n```\n",
        )

        journal_master_key_uid = hash_info.get_str(b"journal_master_key_uid")
        user_sub = hash_info.get_str(b"user_sub")

        journal_master_key = await get_journal_master_key_for_decryption(
            itgs, user_sub=user_sub, journal_master_key_uid=journal_master_key_uid
        )

        encrypted_task = hash_info.get_bytes(b"encrypted_task")
        queued_at = int(hash_info.get_str(b"queued_at"))
        requested_at_unix_date = unix_dates.unix_timestamp_to_unix_date(
            queued_at, tz=pytz.timezone("America/Los_Angeles")
        )

        stats = JournalChatJobStats(RedisStatsPreparer())
        if journal_master_key.type != "success":
            stats.incr_failed(
                requested_at_unix_date=requested_at_unix_date,
                type=b"unknown",
                reason=b"timed_out",
            )
        else:
            decrypted_task = journal_master_key.journal_master_key.decrypt(
                encrypted_task, ttl=None
            )
            parsed_task = JournalChatTask.model_validate_json(decrypted_task)
            stats.incr_failed(
                requested_at_unix_date=requested_at_unix_date,
                type=cast(JobType, parsed_task.type.encode("ascii")),
                reason=b"timed_out",
            )
        await stats.stats.store(itgs)


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.sweep_stuck_journal_chat_jobs")

    asyncio.run(main())
