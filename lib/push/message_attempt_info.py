from pydantic import BaseModel, Field
from typing import Optional, Literal, Tuple, Union
from lib.shared.job_callback import JobCallback
import gzip
import base64


class MessageContents(BaseModel):
    """The contents we can send to the Expo Push API, except
    for the `to` field, simplified and converted to standard
    python conventions (e.g., snake case)

    https://docs.expo.dev/push-notifications/sending-notifications/#message-request-format
    """

    title: str = Field(description="The title of the notification")
    body: str = Field(description="The body of the notification")
    channel_id: str = Field(description="Which android channel to post the message on")


class MessageAttemptToSend(BaseModel):
    """A message attempt as encoded for the to send queue"""

    aud: Literal["send"] = Field(
        description="Where this is to be stored, also used as a discriminatory field when parsing"
    )
    uid: str = Field(description="The unique identifier for this message attempt")
    initially_queued_at: float = Field(
        description="When this first joined the to send queue in seconds since the epoch"
    )
    retry: int = Field(
        description="The number of failed attempts on this message attempt so far"
    )
    last_queued_at: float = Field(
        description="When this was last queued in seconds since the epoch"
    )
    push_token: str = Field(description="The push token to send to")
    contents: MessageContents = Field(description="The contents of the message")
    failure_job: JobCallback = Field(description="The job callback in case of failure")
    success_job: JobCallback = Field(description="The job callback in case of success")


class PushTicket(BaseModel):
    status: Literal["error", "ok"] = Field(
        description="If the Expo Push API was able to queue the message"
    )
    id: Optional[str] = Field(
        None, description="If a push ticket was generated, the id of the ticket"
    )
    message: Optional[str] = Field(
        None, description="If the status was error, the error message identifier"
    )
    details: Optional[Union[str, int, float, dict, list]] = Field(
        None, description="If the status was error, additional details about the error"
    )


class PushReceipt(BaseModel):
    status: Literal["error", "ok"] = Field(
        description="If the notification provider, e.g., FCMs or APNs, was able to queue the message"
    )
    message: Optional[str] = Field(
        None, description="If the status was error, the error message identifier"
    )
    details: Optional[Union[str, int, float, dict, list]] = Field(
        None, description="If the status was error, additional details about the error"
    )


class MessageAttemptToCheck(BaseModel):
    aud: Literal["check"] = Field(
        description="Where this is to be stored, also used as a discriminatory field when parsing"
    )
    uid: str = Field(description="The unique identifier for this message attempt")
    attempt_initially_queued_at: float = Field(
        description="When this first joined the send queue in seconds since the epoch"
    )
    initially_queued_at: float = Field(
        description="When this first joined the check cold set in seconds since the epoch"
    )
    retry: int = Field(
        description="The number of failed attempts to get the receipt on this message attempt so far"
    )
    last_queued_at: float = Field(
        description="When this was last queued to the check cold set in seconds since the epoch"
    )
    push_ticket: PushTicket = Field(description="The push ticket to check")
    push_ticket_created_at: float = Field(
        description="When the push ticket was created"
    )
    push_token: str = Field(description="The push token to send to")
    contents: MessageContents = Field(description="The contents of the message")
    failure_job: JobCallback = Field(description="The job callback in case of failure")
    success_job: JobCallback = Field(description="The job callback in case of success")


class MessageAttemptSuccess(BaseModel):
    aud: Literal["success"] = Field(
        description="Where this is to be stored, also used as a discriminatory field when parsing"
    )
    uid: str = Field(description="The unique identifier for this message attempt")
    initially_queued_at: float = Field(
        description="When this first joined the to send queue in seconds since the epoch"
    )
    push_token: str = Field(description="The push token to send to")
    contents: MessageContents = Field(description="The contents of the message")


MessageAttemptFailureInfoIdentifier = Literal[
    "DeviceNotRegistered",
    "ClientError429",
    "ClientErrorOther",
    "ServerError",
    "MessageTooBig",
    "MessageRateExceeded",
    "MismatchSenderId",
    "InvalidCredentials",
    "InternalError",
    "NetworkError",
    "NotReadyYet",
]


class MessageAttemptFailureInfo(BaseModel):
    """Information about where a message attempt is within the push notification
    send flow.
    """

    action: Literal["send", "check"] = Field(
        description=(
            "The action we were attempting when the failure ocurred-. Either "
            "send, for sending to the Expo Push API to get a ticket, or check, for "
            "requesting the receipt from the Expo Push API."
        )
    )

    ticket: Optional[PushTicket] = Field(
        description=(
            "If we managed to parse a push ticket from the Expo Push API, the ticket"
        )
    )

    receipt: Optional[PushReceipt] = Field(
        description=(
            "If we managed to parse a push receipt from the Expo Push API, the receipt"
        )
    )

    identifier: MessageAttemptFailureInfoIdentifier = Field(
        description=(
            "The cause of the last failure:\n"
            "- DeviceNotRegistered (send, check): The device token is no longer valid\n"
            "- ClientError429 (send, check): push api returned an unexpected 429 response\n"
            "- ClientErrorOther (send, check): push api returned an unexpected 4XX response\n"
            "- ServerError (send, check): push api returned an unexpected 5XX response\n"
            "- MessageTooBig (check): the payload was too large\n"
            "- MessageRateExceeded (check): the message rate was exceeded for the device\n"
            "- MismatchSenderId (check): bad FCMs push credentials\n"
            "- InvalidCredentials (check): bad FCMs/APNs push credentials\n"
            "- InternalError (send, check): we encountered an error\n"
            "- NetworkError (send, check): we couldn't connect with the Expo Push API\n"
            "- NotReadyYet (check): the push receipt is not yet available\n"
        )
    )

    retryable: bool = Field(
        description=(
            "Whether this failure category can sometimes be resolved by retrying. The "
            "job MUST NOT be retried if this is False, and it MAY be retried if this is "
            "True. If this is true and the failure job does not retry, it MUST increment "
            "the abandoned event counter for the day"
        )
    )

    extra: Optional[str] = Field(
        description=(
            "Additional information we managed to gather about the error, if any"
        )
    )


class MessageAttemptFailureData(BaseModel):
    attempt: Union[MessageAttemptToSend, MessageAttemptToCheck] = Field(
        description="The attempt that failed"
    )
    failure_info: MessageAttemptFailureInfo = Field(
        description="Information about the failure"
    )


class MessageAttemptSuccessResult(BaseModel):
    """Information provided when a message attempt made it all the way to the notification
    provider
    """

    ticket_created_at: float = Field(
        description=(
            "When the ticket was created in seconds since the epoch. "
            "In the case where retries were required, this is the time "
            "of the last attempt (i.e., the attempt that succeeded)"
        )
    )
    ticket: PushTicket = Field(
        description=(
            "The push ticket we got from the Expo Push API. In the "
            "case where retries were required, this is the ticket "
            "from the last attempt (i.e., the attempt that succeeded)"
        )
    )
    receipt_checked_at: float = Field(
        description=(
            "When the receipt was checked in seconds since the epoch. "
            "In the case where retries were required, this is the time "
            "of the last attempt (i.e., the attempt that succeeded)"
        )
    )
    receipt: PushReceipt = Field(
        description=(
            "The push receipt we got from the Expo Push API. In the "
            "case where retries were required, this is the receipt "
            "from the last attempt (i.e., the attempt that succeeded)"
        )
    )


class MessageAttemptSuccessData(BaseModel):
    attempt: MessageAttemptSuccess = Field(description="The attempt that succeeded")
    result: MessageAttemptSuccessResult = Field(
        description="Information about the success"
    )


def encode_data_for_failure_job(
    attempt: Union[MessageAttemptToSend, MessageAttemptToCheck],
    failure_info: MessageAttemptFailureInfo,
) -> str:
    """Encodes the information required for a failure jobs `data_raw` keyword
    argument.

    This is designed to avoid degenerate serialization, especially common when
    nesting json objects. For example, repeated json-encoding of the string
    including a quote character (such as most json) can lead to exponential
    growth in the size of the string.

    Args:
        attempt (MessageAttemptToSend, MessageAttemptToCheck): the attempt that
          failed on the most recent attempt. The failure job will retry the
          attempt if appropriate.
        failure_info (MessageAttemptFailureInfo): information about the failure
          so the failure job can decide what to do next.

    Returns:
        A trivially serializable string that can be passed to the failure job
    """
    return base64.urlsafe_b64encode(
        gzip.compress(
            MessageAttemptFailureData(attempt=attempt, failure_info=failure_info)
            .model_dump_json()
            .encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_failure_job(
    data_raw: str,
) -> Tuple[
    Union[MessageAttemptToSend, MessageAttemptToCheck], MessageAttemptFailureInfo
]:
    """Undoes the encoding done by encode_data_for_failure_job"""
    result = MessageAttemptFailureData.model_validate_json(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii")))
    )
    return (result.attempt, result.failure_info)


def encode_data_for_success_job(
    attempt: MessageAttemptSuccess, result: MessageAttemptSuccessResult
) -> str:
    """Encodes the information required for a success jobs `data_raw` keyword
    argument.

    This is designed to avoid degenerate serialization, especially common when
    nesting json objects. For example, repeated json-encoding of the string
    including a quote character (such as most json) can lead to exponential
    growth in the size of the string.

    Args:
        attempt (MessageAttemptSuccess): the attempt that succeeded
        result (MessageAttemptSuccessResult): information about the success

    Returns:
        A trivially serializable string that can be passed to the success job
    """
    return base64.urlsafe_b64encode(
        gzip.compress(
            MessageAttemptSuccessData(attempt=attempt, result=result)
            .model_dump_json()
            .encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_success_job(
    data_raw: str,
) -> Tuple[MessageAttemptSuccess, MessageAttemptSuccessResult]:
    """Undoes the encoding done by encode_data_for_success_job"""
    result = MessageAttemptSuccessData.model_validate_json(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii")))
    )
    return (result.attempt, result.result)
