"""Updates the state of the given user in Klaviyo, matching it with the data in
the database and any data provided.
"""
import asyncio
import secrets
import time
from typing import Dict, List, Literal, Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import os

from jobs import JobCategory
from klaviyo import DuplicateProfileError

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    user_sub: str,
    timezone: Optional[str] = None,
    timezone_technique: Optional[Literal["browser"]] = None,
):
    """Updates the state of the given user in Klaviyo, matching it with the data in
    the database and any data provided.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): The sub of the user to check
        timezone (str, None): If the users timezone is known, must be specified in
            IANA format (e.g., "America/New_York") and the technique must be
            specified as well.
        timezone_technique (str, None): If the users timezone is known, must be
            the technique used to determine it (e.g., "browser") and the timezone
            must be specified as well.
    """
    if (timezone is None) != (timezone_technique is None):
        raise ValueError(
            "If either timezone or timezone_technique is provided, both must be"
        )

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            users.email,
            users.given_name,
            users.family_name,
            users.phone_number,
            user_notification_settings.timezone,
            user_notification_settings.preferred_notification_time,
            user_notification_settings.channel,
            phone_verifications.phone_number,
            user_klaviyo_profiles.uid,
            user_klaviyo_profiles.email,
            user_klaviyo_profiles.phone_number,
            user_klaviyo_profiles.first_name,
            user_klaviyo_profiles.last_name,
            user_klaviyo_profiles.timezone,
            user_klaviyo_profiles.environment,
            user_klaviyo_profiles.klaviyo_id
        FROM users
        LEFT OUTER JOIN user_notification_settings ON (
            user_notification_settings.user_id = users.id
            AND NOT EXISTS (
                SELECT 1 FROM user_notification_settings AS uns
                WHERE 
                    uns.user_id = users.id
                    AND (
                        uns.created_at > user_notification_settings.created_at
                        OR (
                            uns.created_at = user_notification_settings.created_at
                            AND uns.id > user_notification_settings.id
                        )
                    )
                    AND (
                        uns.channel = 'sms'
                        OR user_notification_settings.channel != 'sms'
                    )
            )
        )
        LEFT OUTER JOIN phone_verifications ON (
            phone_verifications.user_id = users.id
            AND phone_verifications.status = 'approved'
            AND NOT EXISTS (
                SELECT 1 FROM phone_verifications AS pvs
                WHERE pvs.user_id = users.id
                  AND pvs.status = 'approved'
                  AND (
                    pvs.verified_at > phone_verifications.verified_at
                    OR (
                        pvs.verified_at = phone_verifications.verified_at
                        AND pvs.id > phone_verifications.id
                    )
                )
            )
        )
        LEFT OUTER JOIN user_klaviyo_profiles ON user_klaviyo_profiles.user_id = users.id
        WHERE users.sub = ?
        """,
        (user_sub,),
    )

    if not response.results:
        logging.warning(f"User with sub {user_sub=} not found")
        return

    email: str = response.results[0][0]
    given_name: str = response.results[0][1]
    family_name: str = response.results[0][2]
    user_phone_number: Optional[str] = response.results[0][3]
    uns_timezone: Optional[str] = response.results[0][4]
    uns_preferred_notification_time: Optional[str] = response.results[0][5]
    uns_channel: Optional[str] = response.results[0][6]
    pv_phone_number: Optional[str] = response.results[0][7]
    k_uid: Optional[str] = response.results[0][8]
    k_email: Optional[str] = response.results[0][9]
    k_phone_number: Optional[str] = response.results[0][10]
    k_first_name: Optional[str] = response.results[0][11]
    k_last_name: Optional[str] = response.results[0][12]
    k_timezone: Optional[str] = response.results[0][13]
    k_environment: Optional[str] = response.results[0][14]
    k_klaviyo_id: Optional[str] = response.results[0][15]

    k_list_ids: list[str] = []
    if k_uid is not None:
        response = await cursor.execute(
            """
            SELECT list_id FROM user_klaviyo_profile_lists
            WHERE
                EXISTS (
                    SELECT 1 FROM user_klaviyo_profiles
                    WHERE user_klaviyo_profiles.uid = ?
                      AND user_klaviyo_profiles.id = user_klaviyo_profile_lists.user_klaviyo_profile_id
                )
            """,
            (k_uid,),
        )

        if response.results:
            k_list_ids = [r[0] for r in response.results]

    environment = os.environ["ENVIRONMENT"]
    best_phone_number = pv_phone_number or user_phone_number
    best_timezone = timezone or uns_timezone or k_timezone or "America/Los_Angeles"
    sms_notification_time = (
        None if uns_channel != "sms" else uns_preferred_notification_time
    )

    klaviyo = await itgs.klaviyo()
    correct_list_internal_identifiers = [
        "users",
        *(["sms-morning"] if sms_notification_time in ("any", "morning") else []),
        *(["sms-afternoon"] if sms_notification_time == "afternoon" else []),
        *(["sms-evening"] if sms_notification_time == "evening" else []),
    ]
    correct_list_ids: List[str] = await asyncio.gather(
        *[
            klaviyo.list_id(internal_identifier)
            for internal_identifier in correct_list_internal_identifiers
        ]
    )
    list_id_to_internal_identifier: Dict[str, str] = dict(
        (v, k) for k, v in zip(correct_list_internal_identifiers, correct_list_ids)
    )

    if k_uid is None:
        new_profile_id = await klaviyo.create_profile(
            email=email,
            phone_number=best_phone_number,
            external_id=user_sub,
            first_name=given_name,
            last_name=family_name,
            timezone=best_timezone,
            environment=environment,
        )
        if new_profile_id is None:
            new_profile_id = await klaviyo.get_profile_id(email=email)
            if new_profile_id is None:
                # the conflict was another account with that phone; we can handle
                # that in a moment
                new_profile_id = await klaviyo.create_profile(
                    email=email,
                    phone_number=None,
                    external_id=user_sub,
                    first_name=given_name,
                    last_name=family_name,
                    timezone=best_timezone,
                    environment=environment,
                )

            try:
                await klaviyo.update_profile(
                    profile_id=new_profile_id,
                    phone_number=best_phone_number,
                    first_name=given_name,
                    last_name=family_name,
                    timezone=best_timezone,
                    environment=environment,
                )
            except DuplicateProfileError as e:
                await klaviyo.update_profile(
                    profile_id=e.duplicate_profile_id,
                    phone_number=None,
                )
                await klaviyo.update_profile(
                    profile_id=new_profile_id, phone_number=None
                )
                await klaviyo.update_profile(
                    profile_id=new_profile_id,
                    phone_number=best_phone_number,
                    first_name=given_name,
                    last_name=family_name,
                    timezone=best_timezone,
                    environment=environment,
                )

        now = time.time()
        new_ukp_uid = f"oseh_ukp_{secrets.token_urlsafe(16)}"
        await cursor.execute(
            """
            INSERT INTO user_klaviyo_profiles (
                uid, klaviyo_id, user_id, email, phone_number, first_name, last_name, timezone,
                environment, created_at, updated_at
            )
            SELECT
                ?, ?, users.id, ?, ?, ?, ?, ?, ?, ?, ?
            FROM users
            WHERE users.sub = ?
            """,
            (
                new_ukp_uid,
                new_profile_id,
                email,
                best_phone_number,
                given_name,
                family_name,
                best_timezone,
                environment,
                now,
                now,
                user_sub,
            ),
        )
        await klaviyo.unsuppress_email(email)

        for list_id in correct_list_ids:
            internal_id = list_id_to_internal_identifier[list_id]
            is_sms_list = internal_id.startswith("sms-")
            if is_sms_list and best_phone_number is None:
                continue
            await klaviyo.subscribe_profile_to_list(
                profile_id=new_profile_id,
                list_id=list_id,
                email=email if not is_sms_list else None,
                phone_number=best_phone_number if is_sms_list else None,
            )
            new_ukpl_uid = f"oseh_ukpl_{secrets.token_urlsafe(16)}"
            await cursor.execute(
                """
                INSERT INTO user_klaviyo_profile_lists (
                    uid, user_klaviyo_profile_id, list_id, created_at
                )
                SELECT
                    ?, user_klaviyo_profiles.id, ?, ?
                FROM user_klaviyo_profiles
                WHERE user_klaviyo_profiles.uid = ?
                """,
                (new_ukpl_uid, list_id, now, new_ukp_uid),
            )

        return

    if (
        k_email != email
        or k_phone_number != best_phone_number
        or k_first_name != given_name
        or k_last_name != family_name
        or k_timezone != best_timezone
        or k_environment != environment
    ):
        await klaviyo.update_profile(
            profile_id=k_klaviyo_id,
            email=email,
            external_id=user_sub,
            phone_number=best_phone_number,
            first_name=given_name,
            last_name=family_name,
            timezone=best_timezone,
            environment=environment,
        )

    if k_email != email:
        await klaviyo.unsuppress_email(email)
        await klaviyo.suppress_email(k_email)

    current_list_ids_set = set(k_list_ids)
    correct_list_ids_set = set(correct_list_ids)

    for list_id_to_remove in current_list_ids_set - correct_list_ids_set:
        await klaviyo.unsubscribe_from_list(
            list_id=list_id_to_remove,
            emails=[email, k_email],
            phone_numbers=[best_phone_number, k_phone_number],
        )
        await cursor.execute(
            """
            DELETE FROM user_klaviyo_profile_lists
            WHERE
                EXISTS (
                    SELECT 1 FROM user_klaviyo_profiles
                    WHERE user_klaviyo_profiles.uid = ?
                      AND user_klaviyo_profiles.id = user_klaviyo_profile_lists.user_klaviyo_profile_id
                )
              AND list_id = ?
            """,
            (k_uid, list_id_to_remove),
        )

    for list_id_to_add in correct_list_ids_set - current_list_ids_set:
        internal_id = list_id_to_internal_identifier[list_id_to_add]
        is_sms_list = internal_id.startswith("sms-")
        if is_sms_list and best_phone_number is None:
            continue
        await klaviyo.subscribe_profile_to_list(
            profile_id=k_klaviyo_id,
            list_id=list_id_to_add,
            email=email if not is_sms_list else None,
            phone_number=best_phone_number if is_sms_list else None,
        )
        new_ukpl_uid = f"oseh_ukpl_{secrets.token_urlsafe(16)}"
        await cursor.execute(
            """
            INSERT INTO user_klaviyo_profile_lists (
                uid, user_klaviyo_profile_id, list_id, created_at
            )
            SELECT
                ?, user_klaviyo_profiles.id, ?, ?
            FROM user_klaviyo_profiles
            WHERE user_klaviyo_profiles.uid = ?
            """,
            (new_ukpl_uid, list_id_to_add, time.time(), k_uid),
        )
