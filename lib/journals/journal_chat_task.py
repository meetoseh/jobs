from typing import Literal, Optional
from pydantic import BaseModel, Field


class JournalChatTask(BaseModel):
    """Describes a task that needs to be performed."""

    type: Literal["greeting", "chat", "reflection-question", "sync", "summarize"] = (
        Field(description="The type of entry that should be produced")
    )
    include_previous_history: bool = Field(
        description="True if the previous history needs to be posted, false otherwise. "
        "False is mostly for backwards compatibility with clients before we standardized on "
        "including the previous history"
    )
    replace_entry_item_uid: Optional[str] = Field(
        description=(
            "None if the task is to add a new item at the end, otherwise, the "
            "uid of the journal entry item to replace"
        )
    )

    def __str__(self) -> str:
        return f"JournalChatTask(OMITTED FOR PRIVACY)"

    def __repr__(self) -> str:
        return str(self)
