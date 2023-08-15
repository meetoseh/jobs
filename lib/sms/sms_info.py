import base64
import gzip
from typing import Literal, Optional, Tuple, Union
from pydantic import BaseModel, Field
from lib.shared.job_callback import JobCallback


class SMSToSend(BaseModel):
    """The model stored in the to_send queue"""

    aud: Literal["send"] = Field(
        description="Where this is to be stored, also used as a discriminatory field when parsing"
    )
    uid: str = Field(description="Our unique identifier for this SMS")
    initially_queued_at: float = Field(
        description="When this first joined the to send queue in seconds since the epoch"
    )
    retry: int = Field(description="The number of failed attempts on this sms so far")
    last_queued_at: float = Field(
        description="When this was last queued in seconds since the epoch"
    )
    phone_number: str = Field(
        description="The phone number to send to, in E.164 format"
    )
    body: str = Field(description="The body of the message")
    failure_job: JobCallback = Field(description="The job callback in case of failure")
    success_job: JobCallback = Field(description="The job callback in case of success")


class MessageResource(BaseModel):
    """Our model for describing a twilio message resource"""

    sid: str = Field(description="Twilio's unique identifier for this message resource")
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
    ] = Field(
        description=(
            "The status of the message. This is any of the Twilio status values "
            "(https://www.twilio.com/docs/sms/api/message-resource#message-status-values) "
            "plus lost. Lost means that the message resource no longer exists. We also "
            "have the idea of an abandoned message resource, but that status appears within "
            "a failure job and never gets stored and thus is excluded here"
        )
    )
    error_code: Optional[str] = Field(
        description="If an error code is available, the error code"
    )
    date_updated: float = Field(
        description="When the message resource was last updated, in seconds since the epoch"
    )


class PendingSMS(BaseModel):
    """The additional information stored about an sms which we still think we
    may receive updates about. This means a message resource which is in a state
    it definitely shouldn't stay at (e.g., `sending`), but not those in a possibly
    terminal state (e.g., `sent`).
    """

    aud: Literal["pending"] = Field(
        description="Where this is to be stored, also used as a discriminatory field when parsing"
    )
    uid: str = Field(description="Our unique identifier for this SMS")
    send_initially_queued_at: float = Field(
        description="When this first joined the to send queue in seconds since the epoch"
    )
    message_resource_created_at: float = Field(
        description=(
            "When the message resource was created in seconds since the epoch, and "
            "thus when it joined the receipt pending set"
        )
    )
    message_resource_last_updated_at: float = Field(
        description="When the message resource was last updated in seconds since the epoch"
    )
    message_resource: MessageResource = Field(
        description="The current state of the message resource"
    )
    failure_job_last_called_at: Optional[float] = Field(
        description=(
            "The last time we queued the failure job for this SMS; the failure job "
            "is responsible for either abandoning the message or polling for stats."
        )
    )
    num_failures: int = Field(
        description="The number of times we have queued the failure job"
    )
    num_changes: int = Field(
        description=(
            "A value which is atomically incremented whenever the message resource is updated, "
            "used as a concurrency tool"
        )
    )
    phone_number: str = Field(
        description="The phone number to send to, in E.164 format"
    )
    body: str = Field(description="The body of the message")
    failure_job: JobCallback = Field(description="The job callback in case of failure")
    success_job: JobCallback = Field(description="The job callback in case of success")


class SMSFailureInfo(BaseModel):
    """Describes why the failure callback is being called"""

    action: Literal["send", "pending"] = Field(
        description=(
            "Where the SMS was when it failed. If the failure is retryable, the job "
            "is required to either abandon or retry the send, but the method to do "
            "this differs depending on the action. If the failure is not retryable, "
            "the job neither retries nor abandons."
        )
    )
    identifier: Literal[
        "ApplicationErrorRatelimit",
        "ApplicationErrorOther",
        "ClientError404",
        "ClientError429",
        "ClientErrorOther",
        "ServerError",
        "InternalError",
        "NetworkError",
    ] = Field(
        description=(
            "The cause of the most recent failure:\n"
            "- ApplicationErrorRatelimit: We received an ErrorCode from Twilio which we"
            " recognize as a ratelimit. Broken down by Twilio ErrorCode (e.g., 14107)\n"
            "- ApplicationErrorOther: We received an ErrorCode from Twilio we didn't"
            " recognize. Broken down by Twilio ErrorCode\n"
            "- ClientError404: We received an HTTP status code 404 when polling, meaning"
            " the MessageResource no longer exists\n"
            "- ClientError429: We received an HTTP status code 429 without a Twilio"
            " ErrorCode.\n"
            "- ClientErrorOther: We received an HTTP status code 4XX without a Twilio"
            " ErrorCode, and no more specific ClientError applied. Broken down by HTTP"
            " status code (e.g., 400)\n"
            "- ServerError: We received an HTTP status code 5XX without a Twilio ErrorCode."
            " Broken down by HTTP status code (e.g., 500)\n"
            "- InternalError: We encountered an error producing the request or processing"
            " the response from Twilio\n"
            "- NetworkError: We encountered an error connecting to Twilio"
        )
    )
    subidentifier: Optional[str] = Field(
        description=(
            "Some identifiers are further broken down on another dimension, e.g, "
            "ClientErrorOther is broken down by the actual HTTP status code returned. "
            "In such a case this is the key in the breakdown, e.g., the HTTP status code, "
            "otherwise None"
        )
    )
    retryable: bool = Field(
        description=(
            "True if the failure is retryable, meaning the job must either retry or "
            "abandon, False if the failure is not retryable, meaning the job neither "
            "retries nor abandons"
        )
    )
    extra: Optional[str] = Field(
        description=(
            "Additional information we managed to gather about the error, if any, intended "
            "for logging purposes only"
        )
    )


class SMSFailureData(BaseModel):
    """The information provided to the failure callback
    (retrieved via decode_data_for_failure_job)
    """

    sms: Union[SMSToSend, PendingSMS] = Field(description="The SMS which failed")
    failure_info: SMSFailureInfo = Field(description="Information about the failure")


class SMSSuccess(BaseModel):
    """The model passed to the success job (retrieved via decode_data_for_success_job) to
    identify the sms. The job is also bassed SMSSuccessResult
    """

    aud: Literal["success"] = Field(
        description="Where this is to be stored, also used as a discriminatory field when parsing"
    )
    uid: str = Field(description="The unique identifier for this sms")
    initially_queued_at: float = Field(
        description="When this first joined the to send queue in seconds since the epoch"
    )
    phone_number: str = Field(
        description="The phone number to send to, in E.164 format"
    )
    body: str = Field(description="The body of the message")


class SMSSuccessResult(BaseModel):
    """Information containing additional context about a succeeded SMS"""

    message_resource_created_at: float = Field(
        description="When the message resource was created in seconds since the epoch"
    )
    message_resource_succeeded_at: float = Field(
        description=(
            "When the message resource was first detected to be in a successful "
            "possibly-terminal or terminal state."
        )
    )
    message_resource: MessageResource = Field(
        description=(
            "The message resource when we were satisfied with the result; "
            "in a successful possibly-terminal or terminal state"
        )
    )


class SMSSuccessData(BaseModel):
    """The information provided to the success callback
    (retrieved via decode_data_for_success_job)
    """

    sms: SMSSuccess = Field(description="The SMS which succeeded")
    result: SMSSuccessResult = Field(description="Information about the success")


def encode_data_for_failure_job(
    sms: Union[SMSToSend, PendingSMS],
    failure_info: SMSFailureInfo,
) -> str:
    """Encodes the information required for a failure jobs `data_raw` keyword
    argument.

    This is designed to avoid degenerate serialization, especially common when
    nesting json objects. For example, repeated json-encoding of the string
    including a quote character (such as most json) can lead to exponential
    growth in the size of the string.

    NOTE:
        For retryable errors, the `PendingSMS` may have changed since the failure.
        The only time to know for sure you had the latest version is when you go
        to abandon/retry via `abandon_pending` and `retry_pending`, which both
        return a boolean if the operation succeeded, meaning you really did have the
        latest version.

    Args:
        sms (SMSToSend, PendingSMS): the sms that failed on the most recent attempt.
          For SMSToSend, this will have `retry=0` on the first failure. For PendingSMS,
          this will have `num_failures=1` and `failure_job_last_called_at` set. This
          is necessary so that we can correctly identify if the pending sms is still
          the most recent.
        failure_info (SMSFailureInfo): information about the failure
          so the failure job can decide what to do next.

    Returns:
        A trivially serializable string that can be passed to the failure job
    """
    return base64.urlsafe_b64encode(
        gzip.compress(
            SMSFailureData(sms=sms, failure_info=failure_info).json().encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_failure_job(
    data_raw: str,
) -> Tuple[Union[SMSToSend, PendingSMS], SMSFailureInfo]:
    """Undoes the encoding done by encode_data_for_failure_job"""
    result = SMSFailureData.parse_raw(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii"))),
        content_type="application/json",
    )
    return (result.sms, result.failure_info)


def encode_data_for_success_job(sms: SMSSuccess, result: SMSSuccessResult) -> str:
    """Encodes the information required for a success jobs `data_raw` keyword
    argument.

    This is designed to avoid degenerate serialization, especially common when
    nesting json objects. For example, repeated json-encoding of the string
    including a quote character (such as most json) can lead to exponential
    growth in the size of the string.

    Args:
        sms (SMSSuccess): the sms that succeeded
        result (SMSSuccessResult): information about the success

    Returns:
        A trivially serializable string that can be passed to the success job
    """
    return base64.urlsafe_b64encode(
        gzip.compress(
            SMSSuccessData(sms=sms, result=result).json().encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_success_job(
    data_raw: str,
) -> Tuple[SMSSuccess, SMSSuccessResult]:
    """Undoes the encoding done by encode_data_for_success_job"""
    result = SMSSuccessData.parse_raw(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii"))),
        content_type="application/json",
    )
    return (result.sms, result.result)
