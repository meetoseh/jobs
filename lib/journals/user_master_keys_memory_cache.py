import asyncio
from typing import Dict
from itgs import Itgs
from lib.journals.master_keys import (
    GetJournalMasterKeyForDecryptionResult,
    get_journal_master_key_for_decryption,
)
import logging


class UserMasterKeysMemoryCache:
    """Acts as an async context manager for loading master keys for a single
    user for decryption. This is typically used when you are decrypting multiple
    entries at once and don't want to refetch from s3 unnecessarily.

    This should be a short-lived object to avoid keeping the keys around for
    longer than necessary.
    """

    def __init__(self, user_sub: str) -> None:
        self.user_sub = user_sub
        """The user whose master keys we are fetching"""

        self.running: Dict[
            str, asyncio.Task[GetJournalMasterKeyForDecryptionResult]
        ] = dict()
        """The currently running tasks to fetch keys"""

        self.ready: Dict[str, GetJournalMasterKeyForDecryptionResult] = dict()
        """The loaded keys"""

        self._entered = False

    async def __aenter__(self):
        assert self._entered is False, "not reentrant"
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._entered:
            return
        self._entered = False

        to_cancel = list(self.running.values())
        self.running.clear()
        self.ready.clear()

        for task in to_cancel:
            task.cancel()

    def ensure_loading(self, itgs: Itgs, /, *, key_uid: str) -> None:
        """If the given key is not already loading, start loading it, in a fire
        and forget manner. This can only be called on the event loop thread.
        This will essentially "eat" the error if a top-level error occurs
        """
        if key_uid in self.running or key_uid in self.ready:
            return

        def on_done(task: asyncio.Task[GetJournalMasterKeyForDecryptionResult]) -> None:
            try:
                result = task.result()
                self.ready[key_uid] = result
            except Exception as e:
                logging.error(
                    f"Error fetching journal master key {key_uid} for decryption (actual raised error) "
                    "via the user master keys memory cache ensure_loading function, so error cannot be raised",
                    exc_info=e,
                )
            finally:
                del self.running[key_uid]

        task = asyncio.create_task(self._fetch(itgs, key_uid=key_uid))
        self.running[key_uid] = task
        task.add_done_callback(on_done)

    async def get(
        self, itgs: Itgs, /, *, key_uid: str
    ) -> GetJournalMasterKeyForDecryptionResult:
        """Gets the master key for the given key uid, fetching it if necessary"""
        if ready_result := self.ready.get(key_uid):
            return ready_result

        if running_task := self.running.get(key_uid):
            return await running_task

        running_task = asyncio.create_task(self._fetch(itgs, key_uid=key_uid))
        self.running[key_uid] = running_task
        try:
            result = await running_task
            self.ready[key_uid] = result
        finally:
            del self.running[key_uid]
        return result

    async def _fetch(
        self, itgs: Itgs, /, *, key_uid: str
    ) -> GetJournalMasterKeyForDecryptionResult:
        """Fetches the master key for the given key uid"""
        return await get_journal_master_key_for_decryption(
            itgs,
            user_sub=self.user_sub,
            journal_master_key_uid=key_uid,
        )
