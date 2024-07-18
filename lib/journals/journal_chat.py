"""This module describes the JournalChat object which is contstructed
on the client from a series of mutations. Because these mutations may
be error-prone to apply, to improve debuggability there are integrity checks
that ensure the object is being formed precisely as desired.
"""

import io
from pydantic import BaseModel, Field, TypeAdapter
from typing import cast, List
from lib.journals.journal_entry_item_data import (
    JournalEntryItemData,
    JournalEntryItemDataClient,
)
import hashlib


internal_journal_entry_item_data_adapter = cast(
    TypeAdapter[List[JournalEntryItemData]], TypeAdapter(List[JournalEntryItemData])
)


class JournalChat(BaseModel):
    uid: str = Field(description="The journal chat uid")
    integrity: str = Field(
        description="hex SHA256 of the sorted keys serialization of the data, usually computed via compute_integrity"
    )
    data: List[JournalEntryItemDataClient] = Field(
        description="The data for the journal chat"
    )

    def compute_integrity(self) -> str:
        """Computes what the correct integrity value for the current data is"""
        raw = io.BytesIO()
        raw.write(b"[")
        if self.data:
            self.data[0].model_dump_for_integrity(raw)
            for i in range(1, len(self.data)):
                raw.write(b", ")
                self.data[i].model_dump_for_integrity(raw)
        raw.write(b"]")
        return hashlib.sha256(raw.getvalue()).hexdigest()
