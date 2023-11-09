from typing import List, Literal, Optional, Union
from itgs import Itgs
from pydantic import BaseModel, Field


class EmailDeliveryNotification(BaseModel):
    type: Literal["Delivery"] = Field(
        description="Indicates this is a delivery notification, i.e., the recipient received the email"
    )


class EmailBounceUndetermined(BaseModel):
    primary: Literal["Undetermined"] = Field(
        description="The recipient's email provider sent a bounce message. "
        "The bounce message didn't contain enough information for Amazon SES to determine "
        "the reason for the bounce. The bounce email, which was sent to the address in the "
        "Return-Path header of the email that resulted in the bounce, might contain additional "
        "information about the issue that caused the email to bounce."
    )
    secondary: Literal["Undetermined"] = Field(
        description="There are no subtypes for Undetermined bounces."
    )


class EmailBouncePermanent(BaseModel):
    primary: Literal["Permanent"] = Field(
        description="It's unlikely that you'll be able to send email to that recipient in the future. "
        "For this reason, you should immediately remove the recipient whose address produced the bounce "
        "from your mailing lists."
    )
    secondary: Literal[
        "General", "NoEmail", "Suppressed", "OnAccountSuppressionList"
    ] = Field(
        description="- General: The recipient's email provider sent a hard bounce message, but didn't specify the reason for the hard bounce.\n"
        "- NoEmail: The intended recipient's email provider sent a bounce message indicating that the email address doesn't exist.\n"
        "- Suppressed: The recipient's email address is on the Amazon SES suppression list because it has a recent history of producing hard bounces\n"
        "- OnAccountSuppressionList: Amazon SES has suppressed sending to this address because it is on the account-level suppression list."
    )


class EmailBounceTransient(BaseModel):
    primary: Literal["Transient"] = Field(
        description="You might be able to send email to that recipient in the future if the issue that caused the message to bounce is resolved. "
        "Note that Amazon SES attempts to redeliver some soft bounces; only after a certain period of time retrying "
        "does Amazon SES send this notification."
    )
    secondary: Literal[
        "General",
        "MailboxFull",
        "MessageTooLarge",
        "ContentRejected",
        "AttachmentRejected",
    ] = Field(
        description="- General: The recipient's email provider sent a general bounce message. You might be able to send a message to the same recipient in the future if the issue that caused the message to bounce is resolved.\n"
        "- MailboxFull: The recipient's email provider sent a bounce message because the recipient's inbox was full. You might be able to send to the same recipient in the future when the mailbox is no longer full.\n"
        "- MessageTooLarge: The recipient's email provider sent a bounce message because message you sent was too large. You might be able to send a message to the same recipient if you reduce the size of the message.\n"
        "- ContentRejected: The recipient's email provider sent a bounce message because the message you sent contains content that the provider doesn't allow. You might be able to send a message to the same recipient if you change the content of the message.\n"
        "- AttachmentRejected: The recipient's email provider sent a bounce message because the message contained an unacceptable attachment. For example, some email providers may reject messages with attachments of a certain file type, or messages with very large attachments. You might be able to send a message to the same recipient if you remove or change the content of the attachment."
    )


EmailBounceReason = Union[
    EmailBounceUndetermined, EmailBouncePermanent, EmailBounceTransient
]


class EmailBounceNotification(BaseModel):
    type: Literal["Bounce"] = Field(
        description="Indicates this is a bounce notification, i.e., the recipient did not receive the email"
    )
    reason: EmailBounceReason = Field(description="The reason the email bounced")
    destination: List[str] = Field(
        description="The email addresses the email was sent to. This should contain just one item in practice"
    )
    bounced_recipients: List[str] = Field(
        description="The email addresses of the recipients (within destination) that may have bounced"
    )


class EmailComplaintNotification(BaseModel):
    type: Literal["Complaint"] = Field(
        description="Indicates this is a complaint notification, i.e., the recipient marked the email as spam"
    )
    feedback_type: Optional[str] = Field(
        description="If the ISP reported a Feedback-Type, the type of feedback from the user, e.g., 'abuse', 'fraud', 'virus', 'other', etc."
    )
    destination: List[str] = Field(
        description="The email addresses the email was sent to. This should contain just one item in practice"
    )
    complained_recipients: List[str] = Field(
        description="The email addresses of the recipients (within destination) that may have made the report"
    )


class EmailEvent(BaseModel):
    message_id: str = Field(
        description="the MessageId assigned by ses of the email this event is for"
    )
    notification: Union[
        EmailDeliveryNotification, EmailBounceNotification, EmailComplaintNotification
    ] = Field(description="the notification received")
    received_at: float = Field(
        description="the time this event was received, in seconds since the epoch"
    )


async def push_email_event(itgs: Itgs, evt: EmailEvent) -> None:
    """Pushes the given email event to the email Event Queue, to be
    processed by the email reconciliation job at the next opportunity.

    Args:
        itgs (Itgs): the integrations to (re)use
        evt (EmailEvent): the event to push
    """
    redis = await itgs.redis()
    await redis.rpush(b"email:event", evt.json().encode("utf-8"))
