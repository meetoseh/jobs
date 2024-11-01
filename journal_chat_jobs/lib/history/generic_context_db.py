import asyncio
from dataclasses import dataclass
from typing import List, Literal, Optional, Protocol, cast

from itgs import Itgs
from lib.journals.user_master_keys_memory_cache import UserMasterKeysMemoryCache
from lib.users.time_of_day import get_time_of_day_from_hour
import unix_dates


@dataclass
class UserLLMContextItem:
    """A decrypted llm context item sent to the processor"""

    uid: str
    """The unique identifier for the llm context item"""
    type: str
    """The type of item; see the database documentation for the user_llm_context table"""
    unstructured_data: str
    """The decrypted unstructured data"""
    created_at: float
    """When this item was created in seconds since the epoch"""
    created_unix_date: int
    """The canonical date associated with when this item was created in days since
    the epoch using the timezone of the user at the time the item was created;
    generally used for streaks
    """
    created_local_time: float
    """The number of seconds from midnight in the users local time at the time
    this context item was created
    """


@dataclass
class CorruptedUserLLMContextItem:
    """A corrupted llm context item sent to the processor"""

    uid: str
    """The uid of the journal entry item"""
    journal_master_key_uid: str
    """The journal master key uid that the encrypted data was supposed to use"""
    master_encrypted_unstructured_data: str
    """The encrypted journal entry item data"""
    created_at: float
    """When this item was created in seconds since the epoch"""
    created_unix_date: int
    """The canonical date associated with when this item was created in days since
    the epoch using the timezone of the user at the time the item was created;
    generally used for streaks
    """
    created_local_time: float
    """The number of seconds from midnight in the users local time at the time
    this context item was created
    """


class StreamingUserLLMContextProcessor(Protocol):
    async def process_generic_context(self, item: UserLLMContextItem) -> None:
        """Called to process an item in the user's llm context"""

    async def process_corrupted_generic_context(
        self, item: CorruptedUserLLMContextItem
    ) -> None:
        """Called to process a corrupted item in the user's llm context (one where
        it's in the db but we can't decrypt the unstructured data)
        """


class StandardStreamingUserLLMContextProcessor:
    def __init__(self, max_items: int, max_characters: int, canceler: asyncio.Event):
        self.items: List[str] = []
        """The items we've loaded so far"""
        self.characters: int = 0
        """The number of characters we've loaded so far"""
        self.max_items = max_items
        """The maximum number of items to load"""
        self.max_characters = max_characters
        """The maximum number of characters to load"""
        self.canceler = canceler
        """The event to set to cancel the load"""

    async def process_generic_context(self, item: UserLLMContextItem) -> None:
        if self.canceler.is_set():
            return

        date_for_user = unix_dates.unix_date_to_date(item.created_unix_date).strftime(
            "%A, %b %d, %Y"
        )
        time_of_day_for_user = get_time_of_day_from_hour(
            int(item.created_local_time / 3600)
        )

        wrapped_item = f'<interaction date="{date_for_user}" time_of_day="{time_of_day_for_user}">{item.unstructured_data}</interaction>'
        if self.max_characters + len(wrapped_item) > self.max_characters:
            self.canceler.set()
            return

        self.items.append(wrapped_item)
        self.characters += len(wrapped_item)
        if len(self.items) >= self.max_items:
            self.canceler.set()

    async def process_corrupted_generic_context(
        self, item: CorruptedUserLLMContextItem
    ) -> None: ...


@dataclass
class _IterKey:
    uid: Optional[str]
    created_at: Optional[float]


async def load_user_llm_context(
    itgs: Itgs,
    /,
    *,
    user_sub: str,
    master_keys: UserMasterKeysMemoryCache,
    processor: StreamingUserLLMContextProcessor,
    read_consistency: Literal["none", "weak", "strong"] = "weak",
    per_query_limit: int = 20,
) -> None:
    """Iterates the users LLM context from newest to oldest via the processor.
    Iteration can be cancelled via standard asyncio mechanisms (i.e.,
    this can handle asyncio.CancelledError and will reraise safely)

    See also: `journal_chat_jobs.lib.history.db#load_user_journal_history`
    """
    conn = await itgs.conn()
    cursor = conn.cursor(read_consistency)

    iter_info = _IterKey(None, None)

    while True:
        response = await cursor.execute(
            """
SELECT
    user_llm_context.uid,
    user_llm_context.type,
    user_journal_master_keys.uid,
    user_llm_context.encrypted_unstructured_data,
    user_llm_context.created_at,
    user_llm_context.created_unix_date,
    user_llm_context.created_local_time
FROM user_llm_context, user_journal_master_keys
WHERE
    user_llm_context.user_id = (
        SELECT users.id FROM users WHERE users.sub = ?
    )
    AND user_llm_context.user_journal_master_key_id = user_journal_master_keys.id
    AND user_journal_master_keys.user_id = user_llm_context.user_id
    AND (
        ? IS NULL 
        OR (
            user_llm_context.created_at < ?
            OR (
                user_llm_context.created_at = ?
                AND ? IS NOT NULL
                AND user_llm_context.uid < ?
            )
        )
    )
ORDER BY
    user_llm_context.created_at DESC,
    user_llm_context.uid DESC
LIMIT ?
            """,
            (
                user_sub,
                iter_info.created_at,
                iter_info.created_at,
                iter_info.created_at,
                iter_info.uid,
                iter_info.uid,
                per_query_limit,
            ),
        )
        if not response.results:
            break

        for row in response.results:
            master_keys.ensure_loading(itgs, key_uid=row[2])

        for row in response.results:
            row_uid = cast(str, row[0])
            row_type = cast(str, row[1])
            row_master_key_uid = cast(str, row[2])
            row_encrypted_unstructured_data = cast(str, row[3])
            row_created_at = cast(float, row[4])
            row_created_unix_date = cast(int, row[5])
            row_created_local_time = cast(float, row[6])

            master_key = await master_keys.get(itgs, key_uid=row_master_key_uid)
            if master_key.type != "success":
                await processor.process_corrupted_generic_context(
                    CorruptedUserLLMContextItem(
                        uid=row_uid,
                        journal_master_key_uid=row_master_key_uid,
                        master_encrypted_unstructured_data=row_encrypted_unstructured_data,
                        created_at=row_created_at,
                        created_unix_date=row_created_unix_date,
                        created_local_time=row_created_local_time,
                    )
                )
                continue

            try:
                unstructured_data = master_key.journal_master_key.decrypt(
                    row_encrypted_unstructured_data
                ).decode("utf-8")
            except:
                await processor.process_corrupted_generic_context(
                    CorruptedUserLLMContextItem(
                        uid=row_uid,
                        journal_master_key_uid=row_master_key_uid,
                        master_encrypted_unstructured_data=row_encrypted_unstructured_data,
                        created_at=row_created_at,
                        created_unix_date=row_created_unix_date,
                        created_local_time=row_created_local_time,
                    )
                )
                continue

            await processor.process_generic_context(
                UserLLMContextItem(
                    uid=row_uid,
                    type=row_type,
                    unstructured_data=unstructured_data,
                    created_at=row_created_at,
                    created_unix_date=row_created_unix_date,
                    created_local_time=row_created_local_time,
                )
            )

        if len(response.results) < per_query_limit:
            break
