"""This module is intended to help with loading a users (recent) chat histories
from the database, quickly. Typically, this would be used to bring in context to
the LLM for e.g. the greeting question
"""

import gzip
from typing import Literal, Optional, Protocol, cast
from dataclasses import dataclass

from itgs import Itgs
from lib.journals.journal_entry_item_data import JournalEntryItemData
from lib.journals.user_master_keys_memory_cache import UserMasterKeysMemoryCache


@dataclass
class JournalEntryMetadata:
    """The metadata about a journal entry sent to the processor"""

    uid: str
    """The unique identifier for the journal entry"""
    canonical_at: float
    """
    The canonical time associated with the journal entry in seconds since the
    epoch; generally this corresponds with the last item in the entry an is
    how we sort entries
    """
    canonical_unix_date: int
    """
    The canonical date associated with the journal entry in days since the
    epoch with days delineated using the users timezone at the time the canonical
    timestamp was last updated; generally used for streaks
    """


@dataclass
class JournalEntryItem:
    """A decrypted entry item sent to the processor"""

    uid: str
    """The uid of the journal entry item"""
    entry_counter: int
    """Orders the entry item within the journal entry, where 0 is the first item"""
    data: JournalEntryItemData
    """The decrypted journal entry item data"""
    created_at: float
    """When this item was created in seconds since the epoch"""
    created_unix_date: int
    """The canonical date associated with when this item was created in days since
    the epoch using the timezone of the user at the time the item was created;
    generally used for streaks
    """


@dataclass
class CorruptedJournalEntryItem:
    """A decrypted entry item sent to the processor"""

    uid: str
    """The uid of the journal entry item"""
    entry_counter: int
    """Orders the entry item within the journal entry, where 0 is the first item"""
    journal_master_key_uid: str
    """The journal master key uid that the encrypted data was supposed to use"""
    master_encrypted_data: str
    """The encrypted journal entry item data"""
    created_at: float
    """When this item was created in seconds since the epoch"""
    created_unix_date: int
    """The canonical date associated with when this item was created in days since
    the epoch using the timezone of the user at the time the item was created;
    generally used for streaks
    """


class StreamingUserJournalHistoryProcessor(Protocol):
    """The minimal protocol for processing a users journal history; process_item
    is insufficient without making the item optional to handle empty items, hence
    the start/end entry methods.

    It is intended that there are classes that adapt this to other protocols for
    convenience, for example, a version which just takes the entry and the list
    of items in the entry could be made out of this protocol for those that want
    to process all the items at once
    """

    async def start_entry(self, entry: JournalEntryMetadata) -> None:
        """Called to indicate we detected a new journal entry in the user's history"""

    async def process_item(
        self, entry: JournalEntryMetadata, item: JournalEntryItem
    ) -> None:
        """Called to process an item in the user's journal history

        It is guarranteed that `start_entry` will be called before the first
        item for a given entry, and that `end_entry` will be called after the
        last item for a given entry
        """

    async def process_corrupted_item(
        self, entry: JournalEntryMetadata, item: CorruptedJournalEntryItem
    ) -> None:
        """Called to process a corrupted item in the user's journal history

        It is guarranteed that `start_entry` will be called before the first
        item for a given entry, and that `end_entry` will be called after the
        last item for a given entry
        """

    async def end_entry(self, entry: JournalEntryMetadata) -> None:
        """Called to indicate we have finished processing a journal entry"""


@dataclass
class _IterKey:
    entry_canonical_at: Optional[float]
    entry_uid: Optional[str]
    item_entry_counter: Optional[int]


async def load_user_journal_history(
    itgs: Itgs,
    /,
    *,
    user_sub: str,
    master_keys: UserMasterKeysMemoryCache,
    processor: StreamingUserJournalHistoryProcessor,
    read_consistency: Literal["none", "weak", "strong"] = "weak",
) -> None:
    """Iterates through the users history from newest to oldest, with items from
    oldest (lowest entry counter) to newest (highest entry counter), via the
    processor. Iteration can be cancelled via standard asyncio mechanisms (i.e.,
    this can handle asyncio.CancelledError and will reraise safely)

    This can be forwarded to the processor as follows:

    ```py
    canceler = asyncio.Event()
    processor = MyProcessor(canceler)
    task = asyncio.create_task(load_user_journal_history(itgs, user_sub=user_sub, processor=processor))
    cancel_task = asyncio.create_task(canceler.wait())
    await asyncio.wait([task, cancel_task], return_when=asyncio.FIRST_COMPLETED)
    if not task.cancel():
        await task
    if not cancel_task.cancel():
        await cancel_task
    ...
    ```
    """
    conn = await itgs.conn()
    cursor = conn.cursor(read_consistency)

    iter_info = _IterKey(None, None, None)
    current_entry = cast(Optional[JournalEntryMetadata], None)
    while True:
        response = await cursor.execute(
            """
SELECT
    journal_entries.uid,
    journal_entries.canonical_at,
    journal_entries.canonical_unix_date,
    journal_entry_items.uid,
    journal_entry_items.entry_counter,
    user_journal_master_keys.uid,
    journal_entry_items.master_encrypted_data,
    journal_entry_items.created_at,
    journal_entry_items.created_unix_date
FROM journal_entries
LEFT OUTER JOIN journal_entry_items ON (
    journal_entries.id = journal_entry_items.journal_entry_id
)
LEFT OUTER JOIN user_journal_master_keys ON (
    user_journal_master_keys.id = journal_entry_items.user_journal_master_key_id
)
WHERE
    journal_entries.user_id = (
        SELECT users.id FROM users WHERE users.sub = ?
    )
    AND user_journal_master_keys.user_id = journal_entries.user_id
    AND (
        ? IS NULL
        OR (
            journal_entries.canonical_at < ?
            OR (
                journal_entries.canonical_at = ?
                AND (
                    journal_entries.uid < ?
                    OR (
                        journal_entries.uid = ?
                        AND ? IS NOT NULL
                        AND journal_entry_items.entry_counter > ?
                    )
                )
            )
        )
    )
ORDER BY
    journal_entries.canonical_at DESC,
    journal_entries.uid DESC,
    journal_entry_items.entry_counter ASC
LIMIT 100
            """,
            (
                user_sub,
                iter_info.entry_canonical_at,
                iter_info.entry_canonical_at,
                iter_info.entry_canonical_at,
                iter_info.entry_uid,
                iter_info.entry_uid,
                iter_info.item_entry_counter,
                iter_info.item_entry_counter,
            ),
        )

        if not response.results:
            break

        iter_info = _IterKey(
            response.results[-1][1],
            response.results[-1][0],
            response.results[-1][4],
        )

        for row in response.results:
            row_master_key_uid = cast(Optional[str], row[5])
            if row_master_key_uid is not None:
                master_keys.ensure_loading(itgs, key_uid=row_master_key_uid)

        for row in response.results:
            row_entry_uid = cast(str, row[0])
            row_entry_canonical_at = cast(float, row[1])
            row_entry_canonical_unix_date = cast(int, row[2])
            row_item_uid = cast(Optional[str], row[3])
            row_item_entry_counter = cast(Optional[int], row[4])
            row_master_key_uid = cast(Optional[str], row[5])
            row_encrypted_data = cast(Optional[str], row[6])
            row_item_created_at = cast(float, row[7])
            row_item_created_unix_date = cast(int, row[8])

            if current_entry is not None and current_entry.uid != row_entry_uid:
                await processor.end_entry(current_entry)
                current_entry = None

            if current_entry is None:
                current_entry = JournalEntryMetadata(
                    uid=row_entry_uid,
                    canonical_at=row_entry_canonical_at,
                    canonical_unix_date=row_entry_canonical_unix_date,
                )
                await processor.start_entry(current_entry)

            if row_item_uid is not None:
                assert row_item_entry_counter is not None, row
                assert row_master_key_uid is not None, row
                assert row_encrypted_data is not None, row
                corrupted_item = CorruptedJournalEntryItem(
                    uid=row_item_uid,
                    entry_counter=row_item_entry_counter,
                    journal_master_key_uid=row_master_key_uid,
                    master_encrypted_data=row_encrypted_data,
                    created_at=row_item_created_at,
                    created_unix_date=row_item_created_unix_date,
                )
                master_key = await master_keys.get(itgs, key_uid=row_master_key_uid)
                if master_key.type != "success":
                    await processor.process_corrupted_item(
                        current_entry, corrupted_item
                    )
                    continue

                try:
                    decrypted = master_key.journal_master_key.decrypt(
                        row_encrypted_data
                    )
                    decompressed = gzip.decompress(decrypted)
                    parsed = JournalEntryItemData.model_validate_json(decompressed)
                except:
                    await processor.process_corrupted_item(
                        current_entry, corrupted_item
                    )
                    continue

                item = JournalEntryItem(
                    uid=row_item_uid,
                    entry_counter=row_item_entry_counter,
                    data=parsed,
                    created_at=row_item_created_at,
                    created_unix_date=row_item_created_unix_date,
                )
                await processor.process_item(current_entry, item)

    if current_entry is not None:
        await processor.end_entry(current_entry)
        current_entry = None
