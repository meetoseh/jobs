from pydantic import BaseModel, Field, TypeAdapter
from typing import Optional, cast, Any, Literal
from typing import List, Union


class SegmentDataMutation(BaseModel):
    key: List[Union[int, str]] = Field(
        description="The path to the key to insert (if it does not exist) or update (if it does exist). An empty key list means replace the root object."
    )
    value: Any = Field(description="The value to set at the key")


class JournalChatRedisPacketMutations(BaseModel):
    counter: int = Field()
    type: Literal["mutations"] = Field()
    mutations: List[SegmentDataMutation] = Field(min_length=1)
    more: bool = Field()


class EventBatchPacketDataItemDataThinkingBar(BaseModel):
    type: Literal["thinking-bar"] = Field(
        description="A hint that the server is working and a progress bar could be used"
    )
    at: int = Field(
        description="The progress is being expressed as a fraction and this is the numerator"
    )
    of: int = Field(
        description="The progress is being expressed as a fraction and this is the denominator"
    )
    message: str = Field(
        description="Short text for what the server is doing, e.g., 'Waiting for worker'"
    )
    detail: Optional[str] = Field(
        None,
        description="Longer text for what the server is doing, e.g., 'Waiting in priority queue because you have Oseh+'",
    )


class EventBatchPacketDataItemDataThinkingSpinner(BaseModel):
    type: Literal["thinking-spinner"] = Field(
        description="A hint that the server is working and a spinner could be used"
    )
    message: str = Field(
        description="Short text for what the server is doing, e.g., 'Waiting for worker'"
    )
    detail: Optional[str] = Field(
        None,
        description="Longer text for what the server is doing, e.g., 'Waiting in priority queue because you have Oseh+'",
    )


class EventBatchPacketDataItemDataError(BaseModel):
    type: Literal["error"] = Field(
        description="A hint that the server encountered an error"
    )
    message: str = Field(
        description="Short text for what went wrong, e.g., 'Internal failure'"
    )
    detail: Optional[str] = Field(
        None,
        description="Longer text for what went wrong, e.g., 'Oseh is down for maintenance'",
    )


class JournalChatRedisPacketPassthrough(BaseModel):
    counter: int = Field()
    type: Literal["passthrough"] = Field()
    event: Union[
        EventBatchPacketDataItemDataThinkingBar,
        EventBatchPacketDataItemDataThinkingSpinner,
        EventBatchPacketDataItemDataError,
    ] = Field()


JournalChatRedisPacket = Union[
    JournalChatRedisPacketMutations, JournalChatRedisPacketPassthrough
]
journal_chat_redis_packet_adapter = cast(
    TypeAdapter[JournalChatRedisPacket], TypeAdapter(JournalChatRedisPacket)
)
