from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from journal_chat_jobs.runners.chat_embeddings_rank_and_pluck import handle_chat
from journal_chat_jobs.runners.greeting import handle_greeting
from lib.journals.journal_chat_task import JournalChatTask
from lib.journals.master_keys import GetJournalMasterKeyForEncryptionResultSuccess
import logging
import pytz
import unix_dates

stats_tz = pytz.timezone("America/Los_Angeles")


async def handle_journal_chat_job(
    itgs: Itgs,
    /,
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
    logging.info(f"{log_id=} starting job for {user_sub=}...")
    await itgs.ensure_redis_liveliness()
    ctx = JournalChatJobContext(
        journal_chat_uid=journal_chat_uid,
        journal_master_key=journal_master_key,
        starts=starts,
        start_time=start_time,
        log_id=log_id,
        queued_at=queued_at,
        queued_at_unix_date_in_stats_tz=unix_dates.unix_timestamp_to_unix_date(
            queued_at, tz=stats_tz
        ),
        user_sub=user_sub,
        journal_entry_uid=journal_entry_uid,
        next_event_counter=next_event_counter,
        task=task,
        last_checked_redis=start_time,
    )

    if task.type == "greeting":
        return await handle_greeting(itgs, ctx)
    elif task.type == "chat":
        return await handle_chat(itgs, ctx)

    raise ValueError(f"Unknown or unsupported task type: {task.type}")


# WARN: be careful not to log `task` or any of its constituent parts
