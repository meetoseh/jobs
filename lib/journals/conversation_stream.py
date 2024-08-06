import asyncio
import gzip
import logging
from typing import Dict, List, Literal, Optional, Union, cast

from error_middleware import handle_error
from itgs import Itgs
from lib.journals.journal_entry_item_data import JournalEntryItemData
from dataclasses import dataclass

from lib.journals.master_keys import (
    GetJournalMasterKeyForEncryptionResultSuccess,
    get_journal_master_key_from_s3,
)


@dataclass
class JournalEntryItem:
    """Sensitive: the decrypted contents of a journal entry item within a journal entry"""

    uid: str
    """The uid of the journal entry item"""

    entry_counter: int
    """The canonical sort value, where the first item is 1, the next is 2, etc"""

    data: JournalEntryItemData
    """The decrypted data associated with this item"""


@dataclass
class ConversationStreamLoadNextItemResultTimeout:
    type: Literal["timeout"]
    """
    - `timeout`: the item was not available within the timeout
    """


@dataclass
class ConversationStreamLoadNextItemResultItem:
    type: Literal["item"]
    """
    - `item`: an item was found
    """
    item: JournalEntryItem
    """The item that was found"""


@dataclass
class ConversationStreamLoadNextItemResultError:
    type: Literal["error"]
    """
    - `error`: an error occurred while loading the item
    """
    error: BaseException
    """The error that occurred"""


@dataclass
class ConversationStreamLoadNextItemResultFinished:
    type: Literal["finished"]
    """
    - `finished`: the stream has finished loading items
    """


ConversationStreamLoadNextItemResult = Union[
    ConversationStreamLoadNextItemResultTimeout,
    ConversationStreamLoadNextItemResultItem,
    ConversationStreamLoadNextItemResultError,
    ConversationStreamLoadNextItemResultFinished,
]


MAX_ITEMS_PER_QUERY = 10
"""The maximum number of journal entry items we try to load from the database at a time"""

MAX_QUEUE_SIZE = 10
"""The maximum number of items we allow to be queued up before we start blocking"""


class JournalChatJobConversationStream:
    """Basic object responsible for fetching the state of the a journal entry. Keeps
    the state in memory, but allows accessing it before it's all available
    """

    def __init__(self, journal_entry_uid: str, user_sub: str) -> None:
        self.journal_entry_uid: str = journal_entry_uid
        """The journal entry that we are streaming"""

        self.user_sub: str = user_sub
        """The sub of the user the journal entry belongs to; we will ignore
        journal entries for other users as an additional sanity check
        """

        self.loaded: List[JournalEntryItem] = []
        """The items that have already been loaded, in the order they were written
        to the journal entry. We only add items to this list after they have been
        returned from load_next_item or load_next_item_immediate
        """

        self.started: bool = False
        """True if we have started loading items in the journal entry, False if we
        have not.
        """

        self.errored: bool = False
        """True if we have encountered an error while loading items in the journal,
        false if we have not. Only set after the error has been pulled from the queue
        """

        self.task: Optional[asyncio.Task] = None
        """The task that is currently loading items in the journal entry"""

        self._queue: Optional[
            asyncio.Queue[Optional[Union[JournalEntryItem, BaseException]]]
        ] = None
        """The queue used by the task to pass items to the stream. Passed `None` to
        indicate the last item has been loaded. Passed an exception if one occurs,
        which also indicates we finished unsuccessfuly.
        """

        self._finished_event: asyncio.Event = asyncio.Event()
        """An event which is set to ensure no additional tasks are waiting on the
        queue. Set when we load the last item off the queue.
        """

    async def start(self) -> None:
        """Starts loading items in the background"""
        assert not self.started, "Already started"
        self.started = True
        self._queue = asyncio.Queue(MAX_QUEUE_SIZE)
        self.task = asyncio.create_task(self._load_items())

    @property
    def finished(self) -> bool:
        """True if we have started and finished loading items. Note that this only
        gets set to True after load_next_item or load_next_item_immediate returns
        None
        """
        return self.started and self.task is None

    async def load_next_item(
        self, /, *, timeout: Optional[float]
    ) -> ConversationStreamLoadNextItemResult:
        """Loads the next item if it is available within the timeout. A timeout
        of None means wait indefinitely. The timeout must be strictly positive if
        set.

        It rarely makes sense to concurrently call this, but if you do, then it
        will respect the order of the calls, so the first item will go to the
        first caller, the next item to the next caller. This means that it's
        possible for there to be a delay before raising error: finished if there
        aren't enough items to satisfy all the concurrent calls.
        """
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be strictly positive or None")

        if not self.started:
            raise RuntimeError("closed: not started")

        if self.errored:
            raise RuntimeError("closed: errored")

        if self.finished:
            raise RuntimeError("closed: finished")

        task = self.task
        queue = self._queue
        assert task is not None, "task not set"
        assert queue is not None, "queue not set"

        queue_get_task = asyncio.create_task(queue.get())
        another_popped_final_item = asyncio.create_task(self._finished_event.wait())
        timeout_task: Optional[asyncio.Task] = (
            None if timeout is None else asyncio.create_task(asyncio.sleep(timeout))
        )

        await asyncio.wait(
            [
                t
                for t in (queue_get_task, another_popped_final_item, timeout_task)
                if t is not None
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        timed_out = False if timeout_task is None else not timeout_task.cancel()
        another_popped_final = not another_popped_final_item.cancel()

        if not queue_get_task.cancel():
            assert not another_popped_final
            item = await queue_get_task

            if item is None:
                self._queue = None
                self.task = None
                self._finished_event.set()
                await task
                return ConversationStreamLoadNextItemResultFinished(type="finished")

            if isinstance(item, BaseException):
                self.errored = True
                self._queue = None
                self.task = None
                self._finished_event.set()
                await task
                return ConversationStreamLoadNextItemResultError(
                    type="error", error=item
                )

            self.loaded.append(item)
            return ConversationStreamLoadNextItemResultItem(type="item", item=item)

        if another_popped_final:
            if self.errored:
                raise RuntimeError("closed: errored")

            raise RuntimeError("closed: finished")

        assert timed_out
        return ConversationStreamLoadNextItemResultTimeout(type="timeout")

    def load_next_item_immediate(self) -> ConversationStreamLoadNextItemResult:
        """Like `load_next_item`, but this only loads the next item if it can be
        retrieved without yielding to the event loop. This MUST still be called on
        the event loop thread.

        This may be slower than `load_next_item`, as more work is required to
        coordinate.

        If called while load_next_item is running, this will steal the next item
        (as if it was queued the earliest), potentially raising errors on those
        tasks in the next iteration of the event loop. This is not recommended,
        but should be safe.
        """
        if not self.started:
            raise RuntimeError("closed: not started")

        if self.errored:
            raise RuntimeError("closed: errored")

        if self.finished:
            raise RuntimeError("closed: finished")

        task = self.task
        queue = self._queue
        assert task is not None, "task not set"
        assert queue is not None, "queue not set"

        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            return ConversationStreamLoadNextItemResultTimeout(type="timeout")

        if item is None:
            self._queue = None
            self.task = None
            asyncio.get_running_loop().call_soon(self._finished_event.set)
            self._cleanup_task_sync(task)
            return ConversationStreamLoadNextItemResultFinished(type="finished")

        if isinstance(item, BaseException):
            self.errored = True
            self._queue = None
            self.task = None
            asyncio.get_running_loop().call_soon(self._finished_event.set)
            self._cleanup_task_sync(task)
            return ConversationStreamLoadNextItemResultError(type="error", error=item)

        self.loaded.append(item)
        return ConversationStreamLoadNextItemResultItem(type="item", item=item)

    def _cleanup_task_sync(self, task: asyncio.Task) -> None:
        if task.done():
            task.result()
            return

        _task: Optional[asyncio.Task] = task
        del task

        def on_finished(fut: asyncio.Task):
            nonlocal _task

            if _task is not fut:
                # primarily, this is to keep a strong reference to the task
                print("task != fut?")

            _task = None

            exc = fut.exception()
            if exc is not None:
                new_task = asyncio.create_task(
                    handle_error(exc, extra_info=f"{__name__}: cleaning up final task")
                )

                def on_new_task_done(v: asyncio.Task):
                    nonlocal new_task
                    if new_task != v:
                        # primarily, this is to keep a strong reference to the task
                        print("new_task != v?")

                    new_task = None
                    v.result()

                new_task.add_done_callback(on_new_task_done)

        _task.add_done_callback(on_finished)

    async def _load_items(self) -> None:
        """Target for the task"""
        queue = self._queue
        if queue is None:
            raise RuntimeError("queue not set")

        try:
            async with Itgs() as itgs:
                conn = await itgs.conn()
                cursor = conn.cursor("weak")

                last_entry_counter: Optional[int] = None
                master_keys_by_uid: Dict[
                    str, GetJournalMasterKeyForEncryptionResultSuccess
                ] = {}

                while True:
                    response = await cursor.execute(
                        """
SELECT
    journal_entry_items.uid,
    journal_entry_items.entry_counter,
    journal_entry_items.master_encrypted_data,
    user_journal_master_keys.uid,
    s3_files.key
FROM users, journal_entries, journal_entry_items, user_journal_master_keys, s3_files
WHERE
    users.sub = ?
    AND users.id = journal_entries.user_id
    AND journal_entries.uid = ?
    AND journal_entry_items.journal_entry_id = journal_entries.id
    AND user_journal_master_keys.user_id = users.id
    AND user_journal_master_keys.id = journal_entry_items.user_journal_master_key_id
    AND s3_files.id = user_journal_master_keys.s3_file_id
    AND (? IS NULL OR journal_entry_items.entry_counter > ?)
ORDER BY journal_entry_items.entry_counter ASC
LIMIT ?
                        """,
                        (
                            self.user_sub,
                            self.journal_entry_uid,
                            last_entry_counter,
                            last_entry_counter,
                            MAX_ITEMS_PER_QUERY,
                        ),
                    )

                    for row in response.results or []:
                        row_uid = cast(str, row[0])
                        row_entry_counter = cast(int, row[1])
                        row_master_encrypted_data_base64url = cast(str, row[2])
                        row_master_key_uid = cast(str, row[3])
                        row_s3_key = cast(str, row[4])

                        master_key = master_keys_by_uid.get(row_master_key_uid)
                        if master_key is None:
                            master_key_raw = None
                            for attempt in range(3):
                                if attempt > 0:
                                    await asyncio.sleep(2**attempt)
                                master_key_raw = await get_journal_master_key_from_s3(
                                    itgs,
                                    user_journal_master_key_uid=row_master_key_uid,
                                    user_sub=self.user_sub,
                                    s3_key=row_s3_key,
                                )
                                if master_key_raw.type not in ("s3_error", "lost"):
                                    break

                            if (
                                master_key_raw is None
                                or master_key_raw.type != "success"
                            ):
                                raise Exception(
                                    f"failed to get journal master key for decryption for {self.user_sub}: {master_key_raw.type if master_key_raw is not None else None}"
                                )

                            master_key = master_key_raw
                            master_keys_by_uid[row_master_key_uid] = master_key_raw

                        item = JournalEntryItem(
                            uid=row_uid,
                            entry_counter=row_entry_counter,
                            data=JournalEntryItemData.model_validate_json(
                                gzip.decompress(
                                    master_key.journal_master_key.decrypt(
                                        row_master_encrypted_data_base64url, ttl=None
                                    )
                                )
                            ),
                        )
                        last_entry_counter = item.entry_counter
                        await queue.put(item)

                    if (
                        response.results is None
                        or len(response.results) < MAX_ITEMS_PER_QUERY
                    ):
                        await queue.put(None)
                        break
        except BaseException as e:
            logging.debug(
                f"conversation stream detected cancellation or raised unexpected error",
                exc_info=e,
            )
            await queue.put(e)

    async def cancel(self) -> None:
        """Forcibly cancels the loading of item, ensuring the load items task
        is properly cleaned up.
        """
        task = self.task
        if task is None:
            return

        task.cancel()
        while not self._finished_event.is_set():
            result = await self.load_next_item(timeout=1)
            if result.type == "timeout":
                raise Exception(
                    f"task taking too long to cancel - likely stuck - {task.done()=}"
                )
