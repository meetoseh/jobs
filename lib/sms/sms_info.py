import base64
import gzip
from typing import Dict, List, Literal, Optional, Tuple, Union, cast
from pydantic import BaseModel, Field
from lib.shared.job_callback import JobCallback
from lib.shared.redis_hash import RedisHash


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


MessageResourceStatus = Literal[
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
]


class MessageResource(BaseModel):
    """Our model for describing a twilio message resource"""

    sid: str = Field(description="Twilio's unique identifier for this message resource")
    status: MessageResourceStatus = Field(
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
    date_updated: Optional[float] = Field(
        description="When the message resource was last updated, in seconds since the epoch, if available"
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

    def as_redis_mapping(self) -> dict:
        return {
            b"aud": self.aud.encode("utf-8"),
            b"uid": self.uid.encode("utf-8"),
            b"send_initially_queued_at": self.send_initially_queued_at,
            b"message_resource_created_at": self.message_resource_created_at,
            b"message_resource_last_updated_at": self.message_resource_last_updated_at,
            b"message_resource_sid": self.message_resource.sid.encode("utf-8"),
            b"message_resource_status": self.message_resource.status.encode("utf-8"),
            b"message_resource_error_code": (
                self.message_resource.error_code.encode("utf-8")
                if self.message_resource.error_code is not None
                else b""
            ),
            b"message_resource_date_updated": (
                self.message_resource.date_updated
                if self.message_resource.date_updated is not None
                else b""
            ),
            b"failure_job_last_called_at": (
                self.failure_job_last_called_at
                if self.failure_job_last_called_at is not None
                else b""
            ),
            b"num_failures": self.num_failures,
            b"num_changes": self.num_changes,
            b"phone_number": self.phone_number.encode("utf-8"),
            b"body": self.body.encode("utf-8"),
            b"failure_job": self.failure_job.model_dump_json().encode("utf-8"),
            b"success_job": self.success_job.model_dump_json().encode("utf-8"),
        }

    @classmethod
    def from_redis_mapping(
        cls,
        mapping_raw: Union[
            List[Union[str, bytes]],
            Dict[Union[str, bytes], Union[str, bytes]],
        ],
    ) -> "PendingSMS":
        data = RedisHash(mapping_raw)
        return cls(
            aud=cast(Literal["pending"], data.get_str(b"aud")),
            uid=data.get_str(b"uid"),
            send_initially_queued_at=data.get_float(b"send_initially_queued_at"),
            message_resource_created_at=data.get_float(b"message_resource_created_at"),
            message_resource_last_updated_at=data.get_float(
                b"message_resource_last_updated_at"
            ),
            message_resource=MessageResource(
                sid=data.get_str(b"message_resource_sid"),
                status=cast(
                    MessageResourceStatus, data.get_str(b"message_resource_status")
                ),
                error_code=data.get_str(b"message_resource_error_code", default=None),
                date_updated=data.get_float(
                    b"message_resource_date_updated", default=None
                ),
            ),
            failure_job_last_called_at=data.get_float(
                b"failure_job_last_called_at", default=None
            ),
            num_failures=data.get_int(b"num_failures"),
            num_changes=data.get_int(b"num_changes"),
            phone_number=data.get_str(b"phone_number"),
            body=data.get_str(b"body"),
            failure_job=JobCallback.model_validate_json(data.get_bytes(b"failure_job")),
            success_job=JobCallback.model_validate_json(data.get_bytes(b"success_job")),
        )


SMSFailureInfoIdentifier = Literal[
    "ApplicationErrorRatelimit",
    "ApplicationErrorOther",
    "ClientError404",
    "ClientError429",
    "ClientErrorOther",
    "ServerError",
    "InternalError",
    "NetworkError",
    "StuckPending",
]


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
    identifier: SMSFailureInfoIdentifier = Field(
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
            "- NetworkError: We encountered an error connecting to Twilio\n"
            "- StuckPending: The message resource has been in a pending state for a while"
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
            SMSFailureData(sms=sms, failure_info=failure_info)
            .model_dump_json()
            .encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_failure_job(
    data_raw: str,
) -> Tuple[Union[SMSToSend, PendingSMS], SMSFailureInfo]:
    """Undoes the encoding done by encode_data_for_failure_job"""
    result = SMSFailureData.model_validate_json(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii")))
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
            SMSSuccessData(sms=sms, result=result).model_dump_json().encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_success_job(
    data_raw: str,
) -> Tuple[SMSSuccess, SMSSuccessResult]:
    """Undoes the encoding done by encode_data_for_success_job"""
    result = SMSSuccessData.model_validate_json(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii")))
    )
    return (result.sms, result.result)
