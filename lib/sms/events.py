from typing import Literal, Optional
from itgs import Itgs
from pydantic import BaseModel, Field


class MessageResourceEvent(BaseModel):
    sid: str = Field(description="The MessageResource SID")
    status: Literal[
        "queued",
        "accepted",
        "scheduled",
        "canceled",
        "sending",
        "sent",
        "delivered",
        "undelivered",
        "failed",
        "lost",
    ] = Field(description="The new status of the MessageResource")
    error_code: Optional[str] = Field(
        None, description="The error code of the MessageResource"
    )
    error_message: Optional[str] = Field(
        None, description="The error message of the MessageResource"
    )
    date_updated: Optional[float] = Field(
        description="The date the MessageResource was updated, in seconds since the epoch, if available"
    )
    information_received_at: float = Field(
        description="When we retrieved this information, in seconds since the epoch"
    )
    received_via: Literal["webhook", "poll"] = Field(
        description="How we received this information"
    )

    @classmethod
    def from_webhook(cls, data: dict, request_at: float) -> "MessageResourceEvent":
        assert isinstance(data.get("MessageSid"), str)
        assert isinstance(data.get("MessageStatus"), str)
        assert isinstance(data.get("ErrorCode"), (str, type(None), int))
        return MessageResourceEvent(
            sid=data["MessageSid"],
            status=data["MessageStatus"],
            error_code=(
                str(data["ErrorCode"]) if data.get("ErrorCode") is not None else None
            ),
            error_message=None,
            date_updated=None,
            information_received_at=request_at,
            received_via="webhook",
        )


async def push_message_resource_event(itgs: Itgs, evt: MessageResourceEvent) -> None:
    """Pushes the given message resource event to the SMS event queue, to be
    processed by the receipt reconciliation job at the next opportunity.

    Args:
        itgs (Itgs): the integrations to (re)use
        evt (MessageResourceEvent): the event to push
    """
    redis = await itgs.redis()
    await redis.rpush(b"sms:event", evt.model_dump_json().encode("utf-8"))  # type: ignore
