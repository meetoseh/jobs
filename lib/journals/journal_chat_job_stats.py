from typing import Literal, Optional
from lib.redis_stats_preparer import RedisStatsPreparer


JobType = Literal[
    b"greeting",
    b"system_chat",
    b"sync",
    b"reflection_question",
    b"summarize",
    b"unknown",
]


class JournalChatJobStats:
    def __init__(self, stats: RedisStatsPreparer):
        self.stats: RedisStatsPreparer = stats
        """The underlying object we are storing stats in"""

    def incr_journal_chat_job_stat(
        self,
        *,
        unix_date: int,
        event: str,
        event_extra: Optional[bytes] = None,
        amt: int = 1,
    ) -> None:
        self.stats.incrby(
            unix_date=unix_date,
            event=event,
            event_extra=event_extra,
            amt=amt,
            basic_key_format="stats:journal_chat_jobs:daily:{unix_date}",
            earliest_key=b"stats:journal_chat_jobs:daily:earliest",
            event_extra_format="stats:journal_chat_jobs:daily:{unix_date}:extra:{event}",
        )

    def incr_requested(self, *, unix_date: int, type: JobType, amt: int = 1) -> None:
        self.incr_journal_chat_job_stat(
            unix_date=unix_date,
            event="requested",
            event_extra=type,
            amt=amt,
        )

    def incr_failed_to_queue_simple(
        self,
        *,
        requested_at_unix_date: int,
        type: JobType,
        reason: Literal[
            b"locked",
            b"user_not_found",
            b"encryption_failed",
            b"journal_entry_not_found",
            b"journal_entry_item_not_found",
            b"decryption_failed",
            b"bad_state",
        ],
        amt: int = 1,
    ) -> None:
        self.incr_journal_chat_job_stat(
            unix_date=requested_at_unix_date,
            event="failed_to_queue",
            event_extra=type + b":" + reason,
            amt=amt,
        )

    def incr_failed_to_queue_ratelimited(
        self,
        *,
        requested_at_unix_date: int,
        type: JobType,
        resource: Literal[b"user_queued_jobs", b"total_queued_jobs"],
        at: int,
        limit: int,
        amt: int = 1,
    ):
        self.incr_journal_chat_job_stat(
            unix_date=requested_at_unix_date,
            event="failed_to_queue",
            event_extra=type
            + b":ratelimited:"
            + resource
            + b":"
            + str(at).encode("ascii")
            + b":"
            + str(limit).encode("ascii"),
            amt=amt,
        )

    def incr_queued(
        self,
        *,
        requested_at_unix_date: int,
        type: JobType,
        amt: int = 1,
    ):
        self.incr_journal_chat_job_stat(
            unix_date=requested_at_unix_date,
            event="queued",
            event_extra=type,
            amt=amt,
        )

    def incr_started(
        self,
        *,
        requested_at_unix_date: int,
        type: JobType,
        amt: int = 1,
    ):
        self.incr_journal_chat_job_stat(
            unix_date=requested_at_unix_date,
            event="started",
            event_extra=type,
            amt=amt,
        )

    def incr_completed(
        self,
        *,
        requested_at_unix_date: int,
        type: JobType,
        amt: int = 1,
    ):
        self.incr_journal_chat_job_stat(
            unix_date=requested_at_unix_date,
            event="completed",
            event_extra=type,
            amt=amt,
        )

    def incr_failed(
        self,
        *,
        requested_at_unix_date: int,
        type: JobType,
        reason: bytes,
        amt: int = 1,
    ):
        self.incr_journal_chat_job_stat(
            unix_date=requested_at_unix_date,
            event="failed",
            event_extra=type + b":" + reason,
            amt=amt,
        )
