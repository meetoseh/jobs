from typing import Literal, Optional
from lib.redis_stats_preparer import RedisStatsPreparer


class JournalStats:
    def __init__(self, stats: RedisStatsPreparer):
        self.stats: RedisStatsPreparer = stats
        """The underlying object we are storing stats in"""

    def incr_journal_stat(
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
            basic_key_format="stats:journals:daily:{unix_date}",
            earliest_key=b"stats:journals:daily:earliest",
            event_extra_format="stats:journals:daily:{unix_date}:extra:{event}",
        )

    def incr_greetings_requested(self, *, unix_date: int, amt: int = 1) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="greetings_requested",
            amt=amt,
        )

    def incr_greetings_succeeded(
        self, *, unix_date: int, technique: str, amt: int = 1
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="greetings_succeeded",
            event_extra=technique.encode("utf-8"),
            amt=amt,
        )

    def incr_greetings_failed_before_queued(
        self,
        *,
        unix_date: int,
        extra: Literal[
            b"queue:ratelimited:pro",
            b"queue:ratelimited:free",
            b"queue:backpressure:pro",
            b"queue:backpressure:free",
            b"queue:user_not_found",
            b"queue:encryption_failed",
            b"queue:journal_entry_not_found",
            b"queue:journal_entry_item_not_found",
            b"queue:decryption_failed",
            b"queue:bad_state",
        ],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="greetings_failed",
            event_extra=extra,
            amt=amt,
        )

    def incr_greetings_failed(
        self, *, unix_date: int, technique: str, reason: str, amt: int = 1
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="greetings_failed",
            event_extra=f"{technique}:{reason}".encode("utf-8"),
            amt=amt,
        )

    def incr_user_chats(self, *, unix_date: int, word_count: int, amt: int = 1) -> None:
        if word_count < 10:
            extra = b"1-9 words"
        elif word_count < 20:
            extra = b"10-19 words"
        elif word_count < 100:
            extra = b"20-99 words"
        elif word_count < 500:
            extra = b"100-499 words"
        else:
            extra = b"500+ words"

        self.incr_journal_stat(
            unix_date=unix_date,
            event="user_chats",
            event_extra=extra,
            amt=amt,
        )

    def incr_system_chats_requested(
        self, *, unix_date: int, extra: Literal[b"initial", b"refresh"], amt: int = 1
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_requested",
            event_extra=extra,
            amt=amt,
        )

    def incr_system_chats_succeeded(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_succeeded",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_system_chats_failed_before_queued(
        self,
        *,
        unix_date: int,
        extra: Literal[
            b"queue:ratelimited:pro",
            b"queue:ratelimited:free",
            b"queue:backpressure:pro",
            b"queue:backpressure:free",
            b"queue:user_not_found",
            b"queue:encryption_failed",
            b"queue:journal_entry_not_found",
            b"queue:journal_entry_item_not_found",
            b"queue:decryption_failed",
            b"queue:bad_state",
        ],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_failed",
            event_extra=extra,
            amt=amt,
        )

    def incr_system_chats_failed_net_http(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["net"],
        status_code: int,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{status_code}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_system_chats_failed_net_timeout(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["net"],
        detail: Literal["timeout"],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{detail}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_system_chats_failed_net_unknown(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["net"],
        detail: Literal["unknown"],
        error_name: str,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{detail}:{error_name}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_system_chats_failed_llm(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["llm"],
        detail: str,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{detail}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_system_chats_failed_internal(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["internal", "encryption"],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="system_chats_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_user_chat_actions(
        self,
        *,
        unix_date: int,
        interactable: Literal["journey"],
        journey_type: Literal["free", "paid"],
        result: Literal["regular", "paywall"],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="user_chat_actions",
            event_extra=f"{interactable}:{journey_type}:{result}".encode("utf-8"),
            amt=amt,
        )

    def incr_reflection_questions_requested(
        self, *, unix_date: int, extra: Literal[b"initial", b"refresh"], amt: int = 1
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_requested",
            event_extra=extra,
            amt=amt,
        )

    def incr_reflection_questions_succeeded(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_succeeded",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_eflection_questions_failed_before_queued(
        self,
        *,
        unix_date: int,
        extra: Literal[
            b"queue:ratelimited:pro",
            b"queue:ratelimited:free",
            b"queue:backpressure:pro",
            b"queue:backpressure:free",
            b"queue:user_not_found",
            b"queue:encryption_failed",
            b"queue:journal_entry_not_found",
            b"queue:journal_entry_item_not_found",
            b"queue:decryption_failed",
            b"queue:bad_state",
        ],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_failed",
            event_extra=extra,
            amt=amt,
        )

    def incr_reflection_questions_failed_net_http(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["net"],
        status_code: int,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{status_code}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_reflection_questions_failed_net_timeout(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["net"],
        detail: Literal["timeout"],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{detail}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_reflection_questions_failed_net_unknown(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["net"],
        detail: Literal["unknown"],
        error_name: str,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{detail}:{error_name}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_reflection_questions_failed_llm(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["llm"],
        detail: str,
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}:{detail}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_reflection_questions_failed_internal(
        self,
        *,
        unix_date: int,
        type: Literal["llm"],
        platform: Literal["openai"],
        model: str,
        prompt_identifier: str,
        category: Literal["internal", "encryption"],
        amt: int = 1,
    ) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_failed",
            event_extra=f"{type}:{platform}:{model}:{prompt_identifier}:{category}".encode(
                "utf-8"
            ),
            amt=amt,
        )

    def incr_reflection_questions_edited(self, *, unix_date: int, amt: int = 1) -> None:
        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_questions_edited",
            amt=amt,
        )

    def incr_reflection_responses(
        self, *, unix_date: int, word_count: int, amt: int = 1
    ) -> None:
        if word_count == 0:
            extra = b"skip"
        elif word_count < 10:
            extra = b"1-9 words"
        elif word_count < 20:
            extra = b"10-19 words"
        elif word_count < 100:
            extra = b"20-99 words"
        elif word_count < 500:
            extra = b"100-499 words"
        else:
            extra = b"500+ words"

        self.incr_journal_stat(
            unix_date=unix_date,
            event="reflection_responses",
            event_extra=extra,
            amt=amt,
        )
