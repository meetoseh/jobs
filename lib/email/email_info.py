import base64
import gzip
import json
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from pydantic import BaseModel, Field

from lib.shared.job_callback import JobCallback
from lib.shared.redis_hash import RedisHash


class EmailAttempt(BaseModel):
    """Describes an email attempt, i.e., an email within the to_send queue"""

    aud: Literal["send"] = Field(
        description="a disambiguation field for failure callbacks"
    )
    uid: str = Field(
        description="the unique identifier for this attempt, assigned by us"
    )
    email: str = Field(description="the recipients email address")
    subject: str = Field(description="the subject line for the email")
    template: str = Field(
        description="the slug of the email template within the email-templates server"
    )
    template_parameters: Dict[str, Any] = Field(
        description="the template parameters for the email template"
    )
    initially_queued_at: float = Field(
        description="the time at which this email was first added to the to_send queue"
    )
    retry: int = Field(
        description="how many times this email has already transiently failed"
    )
    last_queued_at: float = Field(
        description="when this email attempt was most recently added to the to_send queue"
    )
    failure_job: JobCallback = Field(
        description=(
            "the job responsible for determining the retry strategy on transient failures "
            "and handling permanent failures. always passed `data_raw` which can be parsed "
            "via `decode_data_for_failure_job`"
        )
    )
    success_job: JobCallback = Field(
        description=(
            "the job to call if the email is successfully delivered. always passed `data_raw` "
            "which can be parsed via `decode_data_for_success_job`"
        )
    )


class EmailPending(BaseModel):
    """Describes a pending email receipt, i.e., an email attempt in the receipt pending set"""

    aud: Literal["pending"] = Field(
        description="a disambiguation field for failure callbacks"
    )
    uid: str = Field(
        description="the unique identifier for this attempt, assigned by us"
    )
    message_id: str = Field(description="the MessageId assigned by ses")
    email: str = Field(description="the recipients email address")
    subject: str = Field(description="the subject line for the email")
    template: str = Field(
        description="the slug of the email template within the email-templates server"
    )
    template_parameters: Dict[str, Any] = Field(
        description="the template parameters for the email template"
    )
    send_initially_queued_at: float = Field(
        description="the time at which this email was first added to the to_send queue"
    )
    send_accepted_at: float = Field(
        description="the time at which the email was accepted by ses and added to the receipt pending set"
    )
    failure_job: JobCallback = Field(
        description=(
            "the job to call if we get a bounce or complaint notification before a "
            "delivery notification, or if we don't get a delivery notification in an "
            "excessively long period of time"
        )
    )
    success_job: JobCallback = Field(
        description=(
            "the job to call if we get a delivery notification before a bounce or "
            "complaint notification and before an excessively long period of time has "
            "elapsed"
        )
    )

    def as_redis_mapping(self) -> Dict[bytes, bytes]:
        """Converts this email pending into a dictionary that can be used
        as the mapping for hset
        """
        return {
            b"aud": b"pending",
            b"uid": self.uid.encode("utf-8"),
            b"message_id": self.message_id.encode("utf-8"),
            b"email": self.email.encode("utf-8"),
            b"subject": self.subject.encode("utf-8"),
            b"template": self.template.encode("utf-8"),
            b"template_parameters": json.dumps(self.template_parameters).encode(
                "utf-8"
            ),
            b"send_initially_queued_at": str(self.send_initially_queued_at).encode(
                "utf-8"
            ),
            b"send_accepted_at": str(self.send_accepted_at).encode("utf-8"),
            b"failure_job": self.failure_job.json().encode("utf-8"),
            b"success_job": self.success_job.json().encode("utf-8"),
        }

    @classmethod
    def from_redis_mapping(
        cls,
        mapping_raw: Union[
            List[Union[str, bytes]],
            Dict[Union[str, bytes], Union[str, bytes]],
        ],
    ) -> "EmailPending":
        """Parses the result from hgetall (or similar) for a pending email into the
        corresponding object.
        """
        data = RedisHash(mapping_raw)
        return cls(
            aud=data.get_str(b"aud"),
            uid=data.get_str(b"uid"),
            message_id=data.get_str(b"message_id"),
            email=data.get_str(b"email"),
            subject=data.get_str(b"subject"),
            template=data.get_str(b"template"),
            template_parameters=json.loads(data.get_str(b"template_parameters")),
            send_initially_queued_at=data.get_float(b"send_initially_queued_at"),
            send_accepted_at=data.get_float(b"send_accepted_at"),
            failure_job=JobCallback.parse_raw(
                data.get_bytes(b"failure_job"), content_type="application/json"
            ),
            success_job=JobCallback.parse_raw(
                data.get_bytes(b"success_job"), content_type="application/json"
            ),
        )


class EmailSuccessInfo(BaseModel):
    delivery_received_at: float = Field(
        description="the time at which the delivery notification was received"
    )


class EmailSuccessData(BaseModel):
    """Data provided to the success job for an email attempt, encoded as
    if via encode_data_for_success_job
    """

    email: EmailPending = Field(
        description="the email attempt that was successfully delivered"
    )
    info: EmailSuccessInfo = Field(
        description="information about the successful delivery"
    )


class EmailFailureInfo(BaseModel):
    step: Literal["template", "send", "receipt"] = Field(
        description="The step that encountered an error"
    )
    error_identifier: Literal[
        "TemplateNotFound",
        "TemplateUnprocessable",
        "TemplateClientError",
        "TemplateServerError",
        "TemplateNetworkError",
        "TemplateInternalError",
        "SESTooManyRequestsException",
        "SESClientError",
        "SESServerError",
        "SESNetworkError",
        "SESInternalError",
        "Bounce",
        "Complaint",
        "ReceiptTimeout",
        "Suppressed",
    ] = Field(
        description=(
            "An identifier that categorizes the issue:\n"
            "- TemplateNotFound: 404 from email-templates\n"
            "- TemplateUnprocessable: 422 from email-templates\n"
            "- TemplateClientError: 4xx from email-templates\n"
            "- TemplateServerError: 5xx from email-templates\n"
            "- TemplateNetworkError: network error contacting email-templates\n"
            "- TemplateInternalError: internal error forming request to or processing the response from email-templates\n"
            "- SESTooManyRequestsException: Too many requests have been made to the operation.\n"
            "- SESClientError: 4xx from SES\n"
            "- SESServerError: 5xx from SES\n"
            "- SESNetworkError: network error contacting SES\n"
            "- SESInternalError: internal error forming request to or processing response from SES\n"
            "- Bounce: bounce notification from SES\n"
            "- Complaint: complaint notification from SES\n"
            "- ReceiptTimeout: no notification from SES in an excessively long period of time\n"
            "- Suppressed: The email was in our suppressed emails list\n"
        )
    )
    retryable: bool = Field(
        description=(
            "Describes both an ability to retry and a responsibility to report "
            "whether or not you will retry or abandon (via retry_send or abandon_send)."
        )
    )
    extra: Optional[str] = Field(
        description=(
            "Additional information we managed to gather about the error, if any, intended "
            "for logging purposes only"
        )
    )


class EmailFailureData(BaseModel):
    """Data provided to the failure job for an email attempt, encoded as
    if via encode_data_for_failure_job
    """

    email: Union[EmailAttempt, EmailPending] = Field(
        description="The email that failed, as it was just prior to failure"
    )
    info: EmailFailureInfo = Field(description="Information about the failure")


def encode_data_for_failure_job(
    email: Union[EmailAttempt, EmailPending],
    info: EmailFailureInfo,
) -> str:
    """Encodes the information required for a failure jobs `data_raw` keyword
    argument.

    This is designed to avoid degenerate serialization, especially common when
    nesting json objects. For example, repeated json-encoding of the string
    including a quote character (such as most json) can lead to exponential
    growth in the size of the string.

    Args:
        email (EmailAttempt, EmailPending): the email that failed to send
          For EmailAttempt, this will have `retry=0` on the first failure.
        info (EmailFailureInfo): information about the failure
          so the failure job can decide what to do next.

    Returns:
        A trivially serializable string that can be passed to the failure job
    """
    return base64.urlsafe_b64encode(
        gzip.compress(
            EmailFailureData(email=email, info=info).json().encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_failure_job(
    data_raw: str,
) -> Tuple[Union[EmailAttempt, EmailPending], EmailFailureInfo]:
    """Undoes the encoding done by encode_data_for_failure_job"""
    result = EmailFailureData.parse_raw(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii"))),
        content_type="application/json",
    )
    return (result.email, result.info)


def encode_data_for_success_job(email: EmailPending, info: EmailSuccessInfo) -> str:
    """Encodes the information required for a success jobs `data_raw` keyword
    argument.

    This is designed to avoid degenerate serialization, especially common when
    nesting json objects. For example, repeated json-encoding of the string
    including a quote character (such as most json) can lead to exponential
    growth in the size of the string.

    Args:
        email (EmailPending): the email that succeeded, as it was just before
            the delivery notification was received
        info (EmailSuccessInfo): information about the success

    Returns:
        A trivially serializable string that can be passed to the success job
    """
    return base64.urlsafe_b64encode(
        gzip.compress(
            EmailSuccessData(email=email, info=info).json().encode("utf-8"),
            compresslevel=6,
            mtime=0,
        )
    ).decode("ascii")


def decode_data_for_success_job(data_raw: str) -> Tuple[EmailPending, EmailSuccessData]:
    """Undoes the encoding done by encode_data_for_success_job"""
    result = EmailSuccessData.parse_raw(
        gzip.decompress(base64.urlsafe_b64decode(data_raw.encode("ascii"))),
        content_type="application/json",
    )
    return (result.email, result.info)
