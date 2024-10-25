"""A self-contained module for describing users, generally for providing context
when sending slack messages
"""

import asyncio
import os
from typing import List, Literal, Optional, Tuple, cast as typing_cast
from pydantic import BaseModel, Field
from lib.image_files.auth import create_jwt
from itgs import Itgs
import urllib.parse
import pytz
from lib.shared.clean_for_slack import clean_for_non_code_slack, clean_for_slack
import datetime
import time


class Email(BaseModel):
    address: str = Field(
        description="The email address",
    )
    verified: bool = Field(
        description="Whether or not the email address is verified",
    )
    suppressed: bool = Field(
        description="True if this email is on the suppressed list, false otherwise"
    )


class Phone(BaseModel):
    number: str = Field(
        description="The phone number in E.164 format",
    )
    """Prefer number_pretty for logging"""
    verified: bool = Field(
        description="Whether or not the phone number is verified",
    )
    suppressed: bool = Field(
        description="True if this number is on the suppressed list, false otherwise"
    )


class Image(BaseModel):
    uid: str = Field(
        description="The unique identifier for the image",
    )


class Journey(BaseModel):
    uid: str = Field(description="The UID of the journey")
    title: str = Field(description="The title of the journey")
    code: str = Field(description="The public link code used to access the journey")


class Attribution(BaseModel):
    utm: Optional[str] = Field(
        description="The last utm (as a canonical query param string) the user clicked before signing up, if any"
    )
    journey: Optional[Journey] = Field(
        description="The last journey the user took before signing up, if any"
    )


class DescribedUser(BaseModel):
    sub: str = Field(
        description="The user's unique identifier",
    )
    given_name: Optional[str] = Field(description="The users given name")
    family_name: Optional[str] = Field(description="The users family name")
    emails: List[Email] = Field(
        description="The email addresses associated with the user"
    )
    phones: List[Phone] = Field(
        description="The phone numbers associated with the user"
    )
    profile_image: Optional[Image] = Field(
        description="The users profile image, if one is set, otherwise None"
    )
    attribution: Optional[Attribution] = Field(
        description="If attribution data is available, where the user came from"
    )
    identity_providers: List[
        Literal["Google", "SignInWithApple", "Direct", "Silent", "Passkey", "Dev"]
    ] = Field(
        description="The providers of the identities associated with the user",
        max_length=6,
    )
    timezone: Optional[str] = Field(
        description="The users timezone as an IANA timezone string, if set"
    )
    created_at: float = Field(
        description="When the users account was created in seconds since the epoch"
    )

    @property
    def name_equivalent(self) -> str:
        """Alias for get_name_equivalent(self)"""
        return get_name_equivalent(self)

    @property
    def identifier_for_slack(self) -> str:
        """Formats an identifier for the given user for slack. This is
        typically the users full name but falls back as necessary.
        """
        return wrap_in_link(self, get_name_equivalent(self))

    @property
    def details_for_slack(self) -> str:
        """Formats a list of details about the user for slack"""
        now_in_timezone = None
        if self.timezone is not None:
            try:
                tz = pytz.timezone(self.timezone)
                now_in_timezone = (
                    datetime.datetime.fromtimestamp(time.time(), tz=pytz.utc)
                    .astimezone(tz)
                    .strftime("%a, %I:%M %p")
                )
            except:
                now_in_timezone = "err: invalid timezone"

        return "\n •  " + "\n •  ".join(
            [
                *(
                    f"phone: `{clean_for_slack(phone.number)}`"
                    + ("" if phone.verified else " (unverified)")
                    + ("" if not phone.suppressed else " (suppressed)")
                    for phone in self.phones
                ),
                *(
                    f"email: `{clean_for_slack(email.address)}`"
                    + ("" if email.verified else " (unverified)")
                    + ("" if not email.suppressed else " (suppressed)")
                    for email in self.emails
                ),
                f"joined: `{self.pretty_joined}`",
                *(
                    [f"source: `{clean_for_slack(self.attribution.utm)}`"]
                    if self.attribution is not None and self.attribution.utm is not None
                    else []
                ),
                *(
                    [
                        f"source journey: {clean_for_non_code_slack(self.attribution.journey.title)} via `{clean_for_slack(self.attribution.journey.code)}`"
                    ]
                    if self.attribution is not None
                    and self.attribution.journey is not None
                    else []
                ),
                *(
                    [
                        f"timezone: `{clean_for_slack(self.timezone)}` (currently {clean_for_non_code_slack(now_in_timezone)})"
                    ]
                    if self.timezone is not None and now_in_timezone is not None
                    else []
                ),
                f"identity providers: `{', '.join(self.identity_providers) if self.identity_providers else 'none'}`",
            ]
        )

    @property
    def pretty_joined(self) -> str:
        """Represents when the user joined as a date in america/los_angeles"""
        our_time = (
            datetime.datetime.fromtimestamp(self.created_at, tz=pytz.utc).astimezone(
                pytz.timezone("America/Los_Angeles")
            )
        ).strftime("%a %b %d %Y, %I:%M%p")

        if self.timezone is None or self.timezone == "America/Los_Angeles":
            return our_time

        try:
            tz = pytz.timezone(self.timezone)
            their_time = (
                datetime.datetime.fromtimestamp(
                    self.created_at, tz=pytz.utc
                ).astimezone(tz)
            ).strftime("%a, %I:%M%p")
            return f"{our_time} ({their_time} their time)"
        except:
            return our_time

    async def make_slack_block(self, itgs: Itgs, message_format: str) -> dict:
        """Creates the slack block by formatting the given message with
        the keyword argument "name" (which will be `identifier_for_slack`
        with link True), appending the user details to the message, and
        including the profile image as an accessory to the block if available

        ex:

        ```py
        await slack.send_oseh_bot_blocks(
            [await user.make_slack_block(itgs, "{name} just rated us 5 stars!")],
            preview=f"{user.name_equivalent} just rated us 5 stars!"
        )
        ```

        If in a time sensitive context, it can be helpful to fetch the
        described user in a job runner. The message above could be sent
        via

        ```py
        await jobs.enqueue(
            "runners.send_described_user_slack_message",
            message="{name} just rated us 5 stars!"
            sub=user.sub,
            channel="oseh_bot"
        )
        ```

        or the typed equivalent exported from this module

        ```py
        await enqueue_send_described_user_slack_message(
            itgs,
            message="{name} just rated us 5 stars!",
            sub=user.sub,
            channel="oseh_bot",
        )
        ```

        where the preview is handled for you in that case. More
        complicated messages can be implemented with custom job runners.
        """
        message = message_format.format(name=self.identifier_for_slack)
        profile_image_url = await create_slack_profile_image_url(itgs, self)
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message + self.details_for_slack,
            },
            **(
                {
                    "accessory": {
                        "type": "image",
                        "image_url": profile_image_url,
                        "alt_text": "profile image",
                    }
                }
                if profile_image_url is not None
                else {}
            ),
        }


async def describe_user(itgs: Itgs, sub: str) -> Optional[DescribedUser]:
    """Fetches information about the user with the given sub, if they exist,
    otherwise returns None.
    """
    cached = await _get_or_lock_described_user_from_cache(itgs, sub)
    if cached is not None:
        return cached

    real = await _describe_user_from_source(itgs, sub)
    redis = await itgs.redis()
    if real is None:
        await redis.delete(f"described_users:{sub}".encode("utf-8"))
        return None

    await redis.set(
        f"described_users:{sub}".encode("utf-8"),
        real.model_dump_json().encode("utf-8"),
        ex=60 * 15,
    )
    return real


async def _get_or_lock_described_user_from_cache(
    itgs: Itgs, sub: str
) -> Optional[DescribedUser]:
    cache_key = f"described_users:{sub}".encode("utf-8")
    redis = await itgs.redis()

    while True:
        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.set(cache_key, b"1", ex=10, nx=True)
            await pipe.get(cache_key)
            result = await pipe.execute()

        if result[0] is True:
            return None
        if result[1] == b"1":
            await asyncio.sleep(1)
            continue
        return DescribedUser.model_validate_json(result[1])


async def _describe_user_from_source(
    itgs: Itgs, sub: str, *, consistency: Literal["strong", "weak", "none"] = "none"
) -> Optional[DescribedUser]:
    conn = await itgs.conn()
    cursor = conn.cursor(consistency)

    response = await cursor.execute(
        """
        SELECT 
            users.given_name, 
            users.family_name, 
            users.timezone, 
            users.created_at,
            profile_image_files.uid,
            attributed_utms.canonical_query_param,
            attributed_journeys.uid,
            attributed_journeys.title,
            attributed_journey_links.code
        FROM users 
        LEFT OUTER JOIN image_files AS profile_image_files ON (
            EXISTS (
                SELECT 1 FROM user_profile_pictures
                WHERE
                    user_profile_pictures.user_id = users.id
                    AND user_profile_pictures.latest = 1
                    AND user_profile_pictures.image_file_id = profile_image_files.id
            )
        )
        LEFT OUTER JOIN utms AS attributed_utms ON (
            EXISTS (
                SELECT 1 FROM visitor_utms AS last_visitor_utms
                WHERE
                    last_visitor_utms.utm_id = attributed_utms.id
                    AND last_visitor_utms.clicked_at <= users.created_at
                    AND EXISTS (
                        SELECT 1 FROM visitor_users
                        WHERE 
                            visitor_users.visitor_id = last_visitor_utms.visitor_id
                            AND visitor_users.user_id = users.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM visitor_utms AS other_visitor_utms
                        WHERE
                            other_visitor_utms.clicked_at <= users.created_at
                            AND EXISTS (
                                SELECT 1 FROM visitor_users
                                WHERE visitor_users.visitor_id = other_visitor_utms.visitor_id
                                AND visitor_users.user_id = users.id
                            )
                            AND (
                                other_visitor_utms.clicked_at > last_visitor_utms.clicked_at
                                OR (
                                    other_visitor_utms.clicked_at = last_visitor_utms.clicked_at
                                    AND other_visitor_utms.uid > last_visitor_utms.uid
                                )
                            )
                    )
            )
        )
        LEFT OUTER JOIN journey_public_links AS attributed_journey_links ON (
            EXISTS (
                SELECT 1 FROM journey_public_link_views, visitor_users
                WHERE
                    journey_public_link_views.journey_public_link_id = attributed_journey_links.id
                    AND journey_public_link_views.visitor_id = visitor_users.visitor_id
                    AND visitor_users.user_id = users.id
                    AND journey_public_link_views.created_at <= users.created_at
                    AND NOT EXISTS (
                        SELECT 1 FROM 
                            journey_public_links AS other_journey_public_links,
                            journey_public_link_views AS other_journey_public_link_views, 
                            visitor_users AS other_visitor_users
                        WHERE
                            other_journey_public_links.id != attributed_journey_links.id
                            AND other_journey_public_link_views.journey_public_link_id = other_journey_public_links.id
                            AND other_journey_public_link_views.visitor_id = other_visitor_users.visitor_id
                            AND other_visitor_users.user_id = users.id
                            AND other_journey_public_link_views.created_at <= users.created_at
                            AND (
                                other_journey_public_link_views.created_at > journey_public_link_views.created_at
                                OR (
                                    other_journey_public_link_views.created_at = journey_public_link_views.created_at
                                    AND other_journey_public_links.uid > attributed_journey_links.uid
                                )
                            )
                    )
            )
        )
        LEFT OUTER JOIN journeys AS attributed_journeys ON attributed_journeys.id = attributed_journey_links.journey_id
        WHERE users.sub=?
        """,
        (sub,),
    )
    if not response.results:
        if consistency == "none":
            return await _describe_user_from_source(itgs, sub, consistency="weak")
        return None

    given_name = typing_cast(Optional[str], response.results[0][0])
    family_name = typing_cast(Optional[str], response.results[0][1])
    timezone = typing_cast(Optional[str], response.results[0][2])
    created_at = typing_cast(float, response.results[0][3])
    profile_image_uid = typing_cast(Optional[str], response.results[0][4])
    attributed_utm_canonical_query_param = typing_cast(
        Optional[str], response.results[0][5]
    )
    attributed_journey_uid = typing_cast(Optional[str], response.results[0][6])
    attributed_journey_title = typing_cast(Optional[str], response.results[0][7])
    attributed_journey_code = typing_cast(Optional[str], response.results[0][8])

    response = await cursor.execute(
        """
        SELECT 
            email,
            verified,
            EXISTS (
                SELECT 1 FROM suppressed_emails
                WHERE suppressed_emails.email_address = user_email_addresses.email COLLATE NOCASE
            ) AS b1
        FROM user_email_addresses 
        WHERE 
            EXISTS (
                SELECT 1 FROM users 
                WHERE 
                    users.id = user_email_addresses.user_id
                    AND users.sub = ?
            )
        """,
        (sub,),
    )

    emails: List[Email] = [
        Email(address=address, verified=bool(verified), suppressed=bool(suppressed))
        for address, verified, suppressed in (response.results or [])
    ]

    response = await cursor.execute(
        """
        SELECT
            phone_number,
            verified,
            EXISTS (
                SELECT 1 FROM suppressed_phone_numbers
                WHERE suppressed_phone_numbers.phone_number = user_phone_numbers.phone_number
            ) AS b1
        FROM user_phone_numbers
        WHERE
            EXISTS (
                SELECT 1 FROM users
                WHERE
                    users.id = user_phone_numbers.user_id
                    AND users.sub = ?
            )
        """,
        (sub,),
    )

    phones: List[Phone] = [
        Phone(number=number, verified=bool(verified), suppressed=bool(suppressed))
        for number, verified, suppressed in (response.results or [])
    ]

    response = await cursor.execute(
        """
        SELECT DISTINCT 
            provider
        FROM user_identities
        WHERE
            EXISTS (
                SELECT 1 FROM users
                WHERE
                    users.id = user_identities.user_id
                    AND users.sub = ?
            )
        """,
        (sub,),
    )

    identity_providers: List[
        Literal["Google", "SignInWithApple", "Direct", "Silent", "Passkey", "Dev"]
    ] = [provider for provider, in (response.results or [])]

    return DescribedUser(
        sub=sub,
        given_name=given_name,
        family_name=family_name,
        emails=emails,
        phones=phones,
        profile_image=Image(uid=profile_image_uid) if profile_image_uid else None,
        attribution=(
            Attribution(
                utm=attributed_utm_canonical_query_param,
                journey=(
                    Journey(
                        uid=attributed_journey_uid,
                        title=attributed_journey_title,
                        code=attributed_journey_code,
                    )
                    if (
                        attributed_journey_code is not None
                        and attributed_journey_title is not None
                        and attributed_journey_uid is not None
                    )
                    else None
                ),
            )
            if attributed_utm_canonical_query_param or attributed_journey_code
            else None
        ),
        identity_providers=identity_providers,
        timezone=timezone,
        created_at=created_at,
    )


def get_name_equivalent(user: DescribedUser) -> str:
    """Gets the nearest available equivalent to the users given name, for
    identifying the user to a human
    """
    if user.given_name is not None and user.family_name is not None:
        return clean_for_non_code_slack(f"{user.given_name} {user.family_name}".strip())

    if user.given_name is not None:
        return clean_for_non_code_slack(user.given_name)

    if user.family_name is not None:
        return clean_for_non_code_slack(user.family_name)

    if user.emails:
        return clean_for_non_code_slack(user.emails[0].address)

    if user.phones:
        return clean_for_non_code_slack(user.phones[0].number)

    return clean_for_non_code_slack(user.sub)


def wrap_in_link(user: DescribedUser, s: str) -> str:
    """Wraps the given string in a link to the users admin page"""
    root_frontend_url = os.environ["ROOT_FRONTEND_URL"]
    return f"<{root_frontend_url}/admin/user?sub={urllib.parse.quote(user.sub)}|{s}>"


async def create_slack_profile_image_url(
    itgs: Itgs, user: DescribedUser
) -> Optional[str]:
    """Finds the image exports available for the given users profile picture,
    selects the best one for slack, encodes a JWT, and returns the corresponding
    link.

    If the user doesn't have a profile image or its since been deleted this
    returns no url.

    This value is cached for 15 minutes.

    Args:
        itgs (Itgs): the integrations to use
        user (DescribedUser): the user to create a slack image url for
    """
    found_in_cache, url = await _get_or_lock_slack_image_url_from_cache(itgs, user)
    if found_in_cache:
        return url

    redis = await itgs.redis()
    cache_key = f"described_users:profile_picture:{user.sub}".encode("utf-8")
    if user.profile_image is None:
        await redis.set(cache_key, b"0", ex=60 * 15)
        return

    url = await create_slack_image_url_from_source(itgs, user.profile_image.uid)
    await redis.set(
        cache_key, url.encode("utf-8") if url is not None else b"0", ex=60 * 15
    )
    return url


async def create_slack_image_url_from_source(itgs: Itgs, image_file_uid: str):
    """Finds the image exports available for the image file with the given uid,
    selects the best one for slack, encodes a JWT, and returns the corresponding
    link.

    This can be used for adding image accessories to slack messages. For profile
    pictures specifically, prefer `create_slack_profile_image_url` which has
    caching and stampede protection.

    Args:
        itgs (Itgs): the integrations to use
        image_file_uid (str): the uid of the image file to create a slack image
            url for
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")
    response = await cursor.execute(
        """
        SELECT
            image_file_exports.uid,
            image_file_exports.format
        FROM image_file_exports
        WHERE
            EXISTS (
                SELECT 1 FROM image_files
                WHERE 
                    image_files.uid = ?
                    AND image_files.id = image_file_exports.image_file_id
            )
            AND (
                image_file_exports.width = image_file_exports.height
                OR NOT EXISTS (
                    SELECT 1 FROM image_file_exports AS ife
                    WHERE
                        ife.image_file_id = image_file_exports.image_file_id
                        AND ife.width = ife.height
                )
            )
            AND (
                (image_file_exports.width <= 512 AND image_file_exports.height <= 512)
                OR NOT EXISTS (
                    SELECT 1 FROM image_file_exports AS ife
                    WHERE
                        ife.image_file_id = image_file_exports.image_file_id
                        AND ife.width <= 512 AND ife.height <= 512
                        AND ((ife.width = ife.height) >= (image_file_exports.width = image_file_exports.height))
                )
            )
            AND NOT EXISTS (
                SELECT 1 FROM image_file_exports AS ife
                WHERE
                    ife.image_file_id = image_file_exports.image_file_id
                    AND (ife.width = ife.height) >= (image_file_exports.width = image_file_exports.height)
                    AND (ife.width <= 512 AND ife.height <= 512) >= (image_file_exports.width <= 512 AND image_file_exports.height <= 512)
                    AND abs(ife.width * ife.height - 512 * 512) < abs(image_file_exports.width * image_file_exports.height - 512 * 512)
            )
            AND NOT EXISTS (
                SELECT 1 FROM image_file_exports AS ife
                WHERE
                    ife.image_file_id = image_file_exports.image_file_id
                    AND image_file_exports.width = ife.width
                    AND image_file_exports.height = ife.height
                    AND EXISTS (
                        SELECT 1 FROM s3_files AS cur_s3
                        JOIN s3_files AS other_s3 ON other_s3.id = ife.s3_file_id
                        WHERE 
                            cur_s3.id = image_file_exports.s3_file_id
                            AND (
                                other_s3.file_size < cur_s3.file_size
                                OR (other_s3.file_size = cur_s3.file_size AND other_s3.id < cur_s3.id)
                            )
                    )
            )
        """,
        (image_file_uid,),
    )

    if not response.results:
        return None

    assert (
        len(response.results) == 1
    ), "describe_user slack image url selection is not unique"
    image_file_export_uid, image_file_export_format = response.results[0]
    image_file_jwt = await create_jwt(itgs, image_file_uid, duration=60 * 60 * 24 * 7)
    backend_url = os.environ["ROOT_BACKEND_URL"]
    return f"{backend_url}/api/1/image_files/image/{image_file_export_uid}.{image_file_export_format}?jwt={urllib.parse.quote(image_file_jwt)}"


async def _get_or_lock_slack_image_url_from_cache(
    itgs: Itgs, user: DescribedUser
) -> Tuple[bool, Optional[str]]:
    cache_key = f"described_users:profile_picture:{user.sub}".encode("utf-8")
    redis = await itgs.redis()

    while True:
        async with redis.pipeline() as pipe:
            pipe.multi()
            await pipe.set(cache_key, b"1", ex=10, nx=True)
            await pipe.get(cache_key)
            result = await pipe.execute()

        if result[0] is True:
            return (False, None)
        if result[1] == b"1":
            await asyncio.sleep(1)
            continue
        if result[1] == b"0":
            return (True, None)
        return (True, result[1].decode("utf-8"))


async def enqueue_send_described_user_slack_message(
    itgs: Itgs,
    *,
    message: str,
    sub: str,
    channel: Literal["web_error", "ops", "oseh_bot", "oseh_classes"],
) -> None:
    """Enqueues a job to fetch a described user as if via
    `describe_user(itgs, sub)` and then send a message to slack
    formatted as if via `user.make_slack_block(itgs, message)`
    where the preview is formed by the message with name replaced
    with `get_name_equivalent(user)` without wrapping it in a link.

    If the user is not found, the message will still be sent but the
    name will be replaced with something like

    ```md
    USER NOT FOUND: sub=`oseh_u_asdf`
    ```

    Args:
        itgs (Itgs): the integrations to use
        message (str): the message to send to slack
        sub (str): the sub of the user to send the message about
        channel (str): the channel to send the message to
    """
    jobs = await itgs.jobs()
    await jobs.enqueue(
        "runners.send_described_user_slack_message",
        message=message,
        sub=sub,
        channel=channel,
    )
