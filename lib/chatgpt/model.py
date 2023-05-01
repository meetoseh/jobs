from typing import Literal, TypedDict


class ChatCompletionMessage(TypedDict):
    role: Literal["user", "system", "assistant"]
    content: str
