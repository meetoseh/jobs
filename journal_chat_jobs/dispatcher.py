from typing import Literal
from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from journal_chat_jobs.runners.chat_embeddings_rank_and_pluck import handle_chat
from journal_chat_jobs.runners.greeting import handle_greeting
from journal_chat_jobs.runners.reflection_question import handle_reflection
from journal_chat_jobs.runners.summarize import handle_summarize
from journal_chat_jobs.runners.sync import handle_sync
from lib.journals.journal_chat_job_stats import JobType, JournalChatJobStats
from lib.journals.journal_chat_task import JournalChatTask
from lib.journals.master_keys import GetJournalMasterKeyForEncryptionResultSuccess
import logging
import pytz
from lib.redis_stats_preparer import RedisStatsPreparer
from lib.users.timezones import get_user_timezone
import unix_dates

stats_tz = pytz.timezone("America/Los_Angeles")


def get_stat_job_type_from_task_type(
    task_type: Literal["greeting", "chat", "reflection-question", "sync", "summarize"]
) -> JobType:
    if task_type == "greeting":
        return b"greeting"
    if task_type == "chat":
        return b"system_chat"
    if task_type == "reflection-question":
        return b"reflection_question"
    if task_type == "sync":
        return b"sync"
    if task_type == "summarize":
        return b"summarize"
    raise ValueError(f"Unknown or unsupported task type: {task_type}")


async def handle_journal_chat_job(
    *,
    journal_chat_uid: str,
    journal_master_key: GetJournalMasterKeyForEncryptionResultSuccess,
    starts: int,
    start_time: int,
    log_id: str,
    queued_at: int,
    user_sub: str,
    journal_entry_uid: str,
    next_event_counter: int,
    task: JournalChatTask,
) -> None:
    """Manages selecting the appropriate runner from `journal_chat_jobs.runners` and
    delegating to it, combining the parameters into a JournalChatJobContext for
    convenience.

    Note that unlike regular job runners, this function may be entered concurrently
    and is canceled with standard asyncio cancellation, rather than assuming only
    one call per process and receiving a GracefulDeath to detect signals
    """
    async with Itgs() as itgs:
        logging.info(f"{log_id=} starting job for {user_sub=}...")
        await itgs.ensure_redis_liveliness()
        stats = JournalChatJobStats(RedisStatsPreparer())
        queued_at_unix_date_in_stats_tz = unix_dates.unix_timestamp_to_unix_date(
            queued_at, tz=stats_tz
        )
        stat_type = get_stat_job_type_from_task_type(task.type)
        stats.incr_started(
            requested_at_unix_date=queued_at_unix_date_in_stats_tz, type=stat_type
        )
        ctx = JournalChatJobContext(
            journal_chat_uid=journal_chat_uid,
            journal_master_key=journal_master_key,
            starts=starts,
            start_time=start_time,
            log_id=log_id,
            queued_at=queued_at,
            queued_at_unix_date_in_stats_tz=queued_at_unix_date_in_stats_tz,
            user_sub=user_sub,
            user_tz=await get_user_timezone(itgs, user_sub=user_sub),
            journal_entry_uid=journal_entry_uid,
            next_event_counter=next_event_counter,
            task=task,
            last_checked_redis=start_time,
            memory_cached_journeys={},
            has_pro=None,
            stats=stats,
            type=stat_type,
        )

        try:
            if task.type == "greeting":
                return await handle_greeting(itgs, ctx)
            elif task.type == "chat":
                return await handle_chat(itgs, ctx)
            elif task.type == "sync":
                return await handle_sync(itgs, ctx)
            elif task.type == "reflection-question":
                return await handle_reflection(itgs, ctx)
            elif task.type == "summarize":
                return await handle_summarize(itgs, ctx)
        finally:
            _sanity_check_stats(stats, ctx)
            await stats.stats.store(itgs)

        raise ValueError(f"Unknown or unsupported task type: {task.type}")


def _sanity_check_stats(stats: JournalChatJobStats, ctx: JournalChatJobContext) -> None:
    """Verifies that exactly one of completed, failed is set to 1, and the other is set to 0.
    If both are zero, sets failed to 1. For all other issues, raises a ValueError
    """

    changes = stats.stats.get_for_key(
        f"stats:journal_chat_jobs:daily:{ctx.queued_at_unix_date_in_stats_tz}".encode(
            "ascii"
        )
    )
    completed = changes.get(b"completed", 0)
    failed = changes.get(b"failed", 0)
    if completed == 0 and failed == 0:
        stats.incr_failed(
            requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz,
            reason=b"no_completion",
            type=ctx.type,
        )
        assert changes.get(b"failed", 0) == 1
        return
    if completed + failed != 1 or completed not in (0, 1) or failed not in (0, 1):
        raise ValueError(
            f"Expected exactly one completion or failure, got {completed} completed and {failed} failed"
        )


# WARN: be careful not to log `task` or any of its constituent parts
