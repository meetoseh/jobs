from typing import List, Literal, Optional
from pydantic import BaseModel, Field

from lib.journals.journal_entry_item_data import JournalEntryItemData


class JournalChatTask(BaseModel):
    """Contains the conversation, which is sensitive, so should be encrypted"""

    type: Literal["greeting", "chat", "reflection-question"] = Field(
        description="The type of entry that should be produced"
    )
    conversation: List[JournalEntryItemData] = Field(
        description="The current state of the conversation"
    )
    replace_index: Optional[int] = Field(
        description=(
            "None if the task is to add a new item at the end, otherwise, the "
            "index of the item within the conversation to replace"
        )
    )

    def __str__(self) -> str:
        return f"JournalChatTask(OMITTED FOR PRIVACY)"

    def __repr__(self) -> str:
        return str(self)
