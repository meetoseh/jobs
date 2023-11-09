import json
from pydantic import BaseModel, Field, validator
from typing import Any, Dict, List, Literal, Optional, Union
from lib.emails.email_info import EmailFailureInfo
from lib.push.message_attempt_info import MessageAttemptFailureInfo
from lib.shared.job_callback import JobCallback
from lib.shared.redis_hash import RedisHash

from lib.sms.sms_info import SMSFailureInfo


class TouchToSend(BaseModel):
    """A value within touch:to_send"""

    aud: Literal["send"] = Field(description="reserved for future use")
    uid: str = Field(
        description="the unique identifier for this touch attempt, i.e, send intent uid"
    )
    user_sub: str = Field(description="the sub of the user to contact")
    touch_point_event_slug: str = Field(
        description="the event slug on the touch point to emit"
    )
    channel: Literal["push", "sms", "email"] = Field(
        description="the channel to use to contact the user"
    )
    event_parameters: Dict[str, Any] = Field(
        description="the parameters for the event, which vary based on the event"
    )
    success_callback: Optional[JobCallback] = Field(
        description="the callback to call one time when any destination succeeds"
    )
    failure_callback: Optional[JobCallback] = Field(
        description="the callback to call once all destinations are abandoned or fail permanently"
    )
    queued_at: float = Field(
        description=(
            "when this was added to the send queue in seconds since the unix epoch"
        )
    )


class TouchPending(BaseModel):
    success_callback: Optional[JobCallback] = Field(
        description="the callback to call one time when any destination succeeds"
    )
    failure_callback: Optional[JobCallback] = Field(
        description="the callback to call once all destinations are abandoned or fail permanently"
    )

    @validator("failure_callback", pre=True, always=True)
    def at_least_one_callback(cls, v, values):
        """Ensures that at least one callback is provided"""
        if v is None and values.get("success_callback") is None:
            raise ValueError("at least one callback must be provided")
        return v

    @classmethod
    def from_redis_mapping(
        cls,
        mapping_raw: Union[
            List[Union[str, bytes]],
            Dict[Union[str, bytes], Union[str, bytes]],
        ],
    ) -> "TouchPending":
        """Parses the result from hgetall (or similar) for a pending touch into the
        corresponding object.
        """
        data = RedisHash(mapping_raw)
        success_callback_raw = data.get_bytes(b"success_callback", default=None)
        failure_callback_raw = data.get_bytes(b"failure_callback", default=None)
        return cls(
            success_callback=JobCallback.parse_raw(
                success_callback_raw, content_type="application/json"
            )
            if success_callback_raw is not None
            else None,
            failure_callback=JobCallback.parse_raw(
                failure_callback_raw, content_type="application/json"
            )
            if failure_callback_raw is not None
            else None,
        )

    def as_redis_mapping(self) -> Dict[bytes, bytes]:
        """Converts this touch pending into a dictionary that can be used
        as the mapping for hset
        """
        result = dict()
        if self.success_callback is not None:
            result[b"success_callback"] = self.success_callback.json().encode("utf-8")
        if self.failure_callback is not None:
            result[b"failure_callback"] = self.failure_callback.json().encode("utf-8")
        return result


UserTouchPointStateStateFixed = List[str]
UserTouchPointStateState = UserTouchPointStateStateFixed


class TouchLogUserTouchPointStateUpdateFields(BaseModel):
    user_sub: str = Field(
        description="The sub of the user in the relationship to update"
    )
    touch_point_uid: str = Field(
        description="The UID of the touch point in the relationship to update"
    )
    channel: Literal["sms", "email", "push"] = Field(
        description="the channel in the relationship to update"
    )
    state: UserTouchPointStateState = Field(
        description="the new state, which varies based on the selection strategy of the touch point"
    )


class TouchLogUserTouchPointStateUpdate(BaseModel):
    table: Literal["user_touch_point_states"] = Field(description="the table to update")
    action: Literal["update"] = Field(description="the action to perform")
    expected_version: int = Field(
        description="the expected version value before the update, to ensure no other updates have taken place "
        "between when the update was queued and when it was enacted"
    )
    fields: TouchLogUserTouchPointStateUpdateFields = Field(
        description="the fields to update in the table; the version will be incremented and updated_at will be set"
    )
    queued_at: float = Field(
        description="when this was added to the to_log queue in seconds since the epoch"
    )


class TouchLogUserTouchPointStateInsertFields(BaseModel):
    touch_point_uid: str = Field(
        description="The UID of the touch point in the relationship"
    )
    user_sub: str = Field(description="The sub of the user in the relationship")
    channel: Literal["sms", "email", "push"] = Field(
        description="which channel this state is for"
    )
    state: UserTouchPointStateState = Field(
        description="the initial state, which varies based on the selection strategy of the touch point"
    )


class TouchLogUserTouchPointStateInsert(BaseModel):
    table: Literal["user_touch_point_states"] = Field(description="the table to update")
    action: Literal["insert"] = Field(description="the action to perform")
    fields: TouchLogUserTouchPointStateInsertFields = Field(
        description="the fields to insert; the uid, version, created_at, and updated_at will also be set appropriately"
    )
    queued_at: float = Field(
        description="when this was added to the to_log queue in seconds since the epoch"
    )


class TouchLogUserTouchPushInsertMessage(BaseModel):
    title: str = Field(description="the title for the push notification")
    body: str = Field(description="the body for the push notification")
    channel_id: str = Field(description="the channel for Android recipients")


class TouchLogUserTouchPushInsertFields(BaseModel):
    uid: str = Field(description="the send_uid of the user touch to create")
    user_sub: str = Field(description="the sub of the user that was contacted")
    channel: Literal["push"] = Field(
        description="which channel was used to contact the user"
    )
    touch_point_uid: str = Field(
        description="the UID of the touch point that was used to contact the user"
    )
    destination: str = Field(
        description="the push token that was used to contact the user"
    )
    message: TouchLogUserTouchPushInsertMessage = Field(
        description="the message that was sent to the user"
    )
    created_at: float = Field(
        description="when the message was delivered (or the nearest equivalent)"
    )


class TouchLogUserTouchSMSMessage(BaseModel):
    body: str = Field(description="the body of the SMS")


class TouchLogUserTouchSMSInsertFields(BaseModel):
    uid: str = Field(description="the send_uid of the user touch to create")
    user_sub: str = Field(description="the sub of the user that was contacted")
    channel: Literal["sms"] = Field(
        description="which channel was used to contact the user"
    )
    touch_point_uid: str = Field(
        description="the UID of the touch point that was used to contact the user"
    )
    destination: str = Field(
        description="the phone number that was used to contact the user"
    )
    message: TouchLogUserTouchSMSMessage = Field(
        description="the message that was sent to the user"
    )
    created_at: float = Field(
        description="when the message was delivered (or the nearest equivalent)"
    )


class TouchLogUserTouchEmailMessage(BaseModel):
    subject: str = Field(description="the subject of the email")
    template: str = Field(description="the slug of the email template that was used")
    template_parameters: Dict[str, Any] = Field(
        description="the parameters that were passed to the email template"
    )


class TouchLogUserTouchEmailInsertFields(BaseModel):
    uid: str = Field(description="the send_uid of the user touch to create")
    user_sub: str = Field(description="the sub of the user that was contacted")
    channel: Literal["email"] = Field(
        description="which channel was used to contact the user"
    )
    touch_point_uid: str = Field(
        description="the UID of the touch point that was used to contact the user"
    )
    destination: str = Field(
        description="the email address that was used to contact the user"
    )
    message: TouchLogUserTouchEmailMessage = Field(
        description="the message that was sent to the user"
    )
    created_at: float = Field(
        description="when the message was delivered (or the nearest equivalent)"
    )


TouchLogUserTouchInsertFields = Union[
    TouchLogUserTouchPushInsertFields,
    TouchLogUserTouchSMSInsertFields,
    TouchLogUserTouchEmailInsertFields,
]


class TouchLogUserTouchInsert(BaseModel):
    table: Literal["user_touches"] = Field(description="the table to update")
    action: Literal["insert"] = Field(description="the action to perform")
    fields: TouchLogUserTouchInsertFields = Field(
        description="the fields to insert; the uid will be set appropriately"
    )
    queued_at: float = Field(
        description="when this was added to the to_log queue in seconds since the epoch"
    )


class UserTouchDebugLogEventSendAttempt(BaseModel):
    type: Literal["send_attempt"] = Field()
    queued_at: float = Field(description="when the send attempt was queued")
    channel: Literal["push", "sms", "email"] = Field(
        description="the channel used to contact the user"
    )
    event: str = Field(description="the event slug for the touch point")
    event_parameters: Dict[str, Any] = Field(
        description="the parameters for the event, which vary based on the event"
    )


class UserTouchDebugLogEventSendUnreachable(BaseModel):
    type: Literal["send_unreachable"] = Field()
    parent: str = Field(description="the UID of the send attempt that failed")


class UserTouchDebugLogEventSendStale(BaseModel):
    type: Literal["send_stale"] = Field()
    parent: str = Field(description="the UID of the send attempt that failed")


class UserTouchDebugLogEventSendReachable(BaseModel):
    type: Literal["send_reachable"] = Field()
    parent: str = Field(description="the UID of the send attempt that succeeded")
    message: Union[
        TouchLogUserTouchPushInsertMessage,
        TouchLogUserTouchSMSMessage,
        TouchLogUserTouchEmailMessage,
    ] = Field(description="the message that was sent to the user")
    destinations: List[str] = Field(description="the destinations that we found")


class UserTouchDebugLogEventSendRetry(BaseModel):
    type: Literal["send_retry"] = Field()
    parent: str = Field(
        description="the UID of the send attempt that we retried within the subqueue"
    )
    destination: str = Field(
        description="the destination that we retried within the subqueue"
    )
    info: Union[SMSFailureInfo, MessageAttemptFailureInfo, EmailFailureInfo] = Field(
        description="the failure info; varies by channel"
    )


class UserTouchDebugLogEventSendAbandon(BaseModel):
    type: Literal["send_abandon"] = Field()
    parent: str = Field(
        description="the UID of the send attempt that we abandoned within the subqueue"
    )
    destination: str = Field(
        description="the destination that we abandoned within the subqueue"
    )
    info: Union[SMSFailureInfo, MessageAttemptFailureInfo, EmailFailureInfo] = Field(
        description="the failure info; varies by channel"
    )


class UserTouchDebugLogEventSendUnretryable(BaseModel):
    type: Literal["send_unretryable"] = Field()
    parent: str = Field(
        description="the UID of the send attempt that failed permanently within the subqueue"
    )
    destination: str = Field(
        description="the destination that failed permanently within the subqueue"
    )
    info: Union[SMSFailureInfo, MessageAttemptFailureInfo, EmailFailureInfo] = Field(
        description="the failure info; varies by channel"
    )


class UserTouchDebugLogEventSendSuccess(BaseModel):
    type: Literal["send_success"] = Field()
    parent: str = Field(
        description="the UID of the send attempt that succeeded within the subqueue"
    )
    destination: str = Field(
        description="the destination that succeeded within the subqueue"
    )


class TouchLogUserTouchDebugLogInsertFields(BaseModel):
    uid: str = Field(description="the UID assigned to this debug event")
    user_sub: str = Field(description="the sub of the user the event is for")
    event: Union[
        UserTouchDebugLogEventSendAttempt,
        UserTouchDebugLogEventSendUnreachable,
        UserTouchDebugLogEventSendStale,
        UserTouchDebugLogEventSendReachable,
        UserTouchDebugLogEventSendRetry,
        UserTouchDebugLogEventSendAbandon,
        UserTouchDebugLogEventSendUnretryable,
        UserTouchDebugLogEventSendSuccess,
    ] = Field(description="the event that occurred")
    created_at: float = Field(description="when the event occurred")


class TouchLogUserTouchDebugLogInsert(BaseModel):
    table: Literal["user_touch_debug_log"] = Field(description="the table to update")
    action: Literal["insert"] = Field(description="the action to perform")
    fields: TouchLogUserTouchDebugLogInsertFields = Field(
        description="the fields to insert"
    )
    queued_at: float = Field(
        description="when this was added to the to_log queue in seconds since the epoch"
    )


class TouchLogUserPushTokenUpdateFields(BaseModel):
    token: str = Field(description="the push token that was confirmed")
    last_confirmed_at: float = Field(description="when the push token was confirmed")


class TouchLogUserPushTokenUpdate(BaseModel):
    table: Literal["user_push_tokens"] = Field(description="the table to update")
    action: Literal["update"] = Field(description="the action to perform")
    fields: TouchLogUserPushTokenUpdateFields = Field(
        description="the fields to identify the row to update and the updates to perform"
    )
    queued_at: float = Field(
        description="when this was added to the to_log queue in seconds since the epoch"
    )


TouchLog = Union[
    TouchLogUserTouchPointStateUpdate,
    TouchLogUserTouchPointStateInsert,
    TouchLogUserTouchInsert,
    TouchLogUserTouchDebugLogInsert,
    TouchLogUserPushTokenUpdate,
]


def touch_log_parse_raw(raw: Union[str, bytes, bytearray]):
    """Parses the given raw JSON into a TouchLog object. This is required
    since TouchLog is a meta-type, not an actual BaseModel, so there is no
    TouchLog.parse_raw method.

    Args:
        raw (str, bytes, bytearray): the raw json to parse

    Returns:
        TouchLog: the parsed TouchLog object

    Raises:
        ValueError, ValidationError: if the raw json is invalid
    """
    obj = json.loads(raw)

    if obj["table"] == "user_touch_point_states":
        if obj["action"] == "update":
            return TouchLogUserTouchPointStateUpdate.parse_obj(obj)
        return TouchLogUserTouchPointStateInsert.parse_obj(obj)
    if obj["table"] == "user_touches":
        return TouchLogUserTouchInsert.parse_obj(obj)
    if obj["table"] == "user_push_tokens":
        return TouchLogUserPushTokenUpdate.parse_obj(obj)
    return TouchLogUserTouchDebugLogInsert.parse_obj(obj)
