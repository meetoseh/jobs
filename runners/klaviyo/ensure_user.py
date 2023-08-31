"""Updates the state of the given user in Klaviyo, matching it with the data in
the database and any data provided.
"""
import asyncio
import json
import secrets
import time
from pydantic import BaseModel, Field
from typing import Dict, List, Literal, Optional
from error_middleware import handle_contextless_error
from redis.exceptions import NoScriptError
from itgs import Itgs
from graceful_death import GracefulDeath
import hashlib
import logging
import socket
from lib.timezones import TimezoneTechniqueSlug
import unix_dates
import pytz
import os
import lib.users.stats

from jobs import JobCategory
from klaviyo import DuplicateProfileError

category = JobCategory.HIGH_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    user_sub: str,
    timezone: Optional[str] = None,
    timezone_technique: Optional[TimezoneTechniqueSlug] = None,
    is_outside_flow: bool = False,
    is_bounce: bool = False,
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
        is_outside_flow (bool): If true, this is not being called on a scheduled
            basis rather than during the user flow, so if we apply default values
            it shouldn't result in double-opt-out notifications. This can also be
            set if we're at the end of the standard flow and thus aren't expecting
            updates soon.
        is_bounce (bool): If true, this is not an actual request to do anything,
            but rather an implementation detail of how ensure_user works; specifically,
            this is a job to check the queue for the user.
    """
    now = time.time()
    requested_lock = KlaviyoEnsureUserLock(
        typ="lock",
        acquired_at=now,
        host=socket.gethostname(),
        pid=os.getpid(),
        uid=secrets.token_hex(8),
    )
    if is_bounce:
        action_to_execute = await maybe_lock(
            itgs,
            user_sub=user_sub,
            lock=requested_lock,
        )
        if action_to_execute is None:
            logging.info(
                "This was a bounce request for ensure_user, but we either failed "
                "to acquire the lock or there was no action to execute. This "
                "happens under normal circumstances after a chain of queued "
                "actions."
            )
            return
    else:
        queued_action = KlaviyoEnsureUserQueuedAction(
            typ="action",
            queued_at=now,
            timezone=timezone,
            timezone_technique=timezone_technique,
            is_outside_flow=is_outside_flow,
        )
        action_to_execute = await queue_action_and_maybe_lock(
            itgs,
            user_sub=user_sub,
            lock=requested_lock,
            action=queued_action,
        )
        if action_to_execute is None:
            logging.info(
                f"Failed to acquire lock for {user_sub=}, queued instead: {queued_action.json()}"
            )
            return

    if action_to_execute is None:
        return

    logging.debug(
        f"Acquired lock={requested_lock.json()} for {user_sub=} to execute action={action_to_execute.json()}..."
    )
    try:
        await _execute_directly(
            itgs,
            user_sub=user_sub,
            timezone=action_to_execute.timezone,
            timezone_technique=action_to_execute.timezone_technique,
            is_outside_flow=action_to_execute.is_outside_flow,
        )
    finally:
        logging.debug(f"Releasing lock={requested_lock.json()} for {user_sub=}")
        await release_lock(itgs, user_sub=user_sub, lock=requested_lock)

        logging.debug(f"Queueing bounce for {user_sub=}")
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.klaviyo.ensure_user", user_sub=user_sub, is_bounce=True
        )


class KlaviyoEnsureUserLock(BaseModel):
    typ: Literal["lock"] = Field()
    """Indicates this is a lock for deserialization"""
    acquired_at: float = Field()
    """The time at which the lock was acquired"""
    host: str = Field()
    """The host that acquired the lock, as if from socket.gethostname()"""
    pid: str = Field()
    """The PID of the process that acquired the lock, as if from os.getpid()"""
    uid: str = Field()
    """A unique identifier created when acquiring the lock and used in some log messages
    to facilitate debugging
    """


class KlaviyoEnsureUserQueuedAction(BaseModel):
    typ: Literal["action"] = Field()
    """Indicates this is an action for deserialization"""
    queued_at: float = Field()
    """The time at which the action was queued"""
    timezone: Optional[str] = Field(None)
    """See execute timezone kwarg"""
    timezone_technique: Optional[TimezoneTechniqueSlug] = Field(None)
    """See execute timezone_technique kwarg"""
    is_outside_flow: bool = Field(False)
    """See execute is_outside_flow kwarg"""


REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK = """
local lock_key = KEYS[1]
local queue_key = KEYS[2]
local lock_val = ARGV[1]
local action_val = ARGV[2]

local acquired_lock = redis.call("SET", lock_key, lock_val, "NX", "EX", 300)
redis.call("RPUSH", queue_key, action_val)

if acquired_lock ~= nil and acquired_lock ~= false then
    local first_action = redis.call("LPOP", queue_key)
    return {"1", first_action}
end

return {"2", nil}
"""

REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK_SHA = hashlib.sha1(
    REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK.encode("utf-8")
).hexdigest()


async def queue_action_and_maybe_lock(
    itgs: Itgs,
    *,
    user_sub: str,
    lock: KlaviyoEnsureUserLock,
    action: KlaviyoEnsureUserQueuedAction,
) -> Optional[KlaviyoEnsureUserQueuedAction]:
    """Simultaneously attempts to acquire a lock and queue an action. If the lock
    is acquired, then the first queued action is also popped and returned, otherwise
    the action is queued and None is returned.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str): The sub of the user to check
        lock (KlaviyoEnsureUserLock): The lock to acquire
        action (KlaviyoEnsureUserQueuedAction): The action to queue if the lock
            cannot be acquired

    Returns:
        (KlaviyoEnsureUserQueuedAction or None): The first queued action if the lock
            was acquired, otherwise None
    """
    redis = await itgs.redis()
    lock_key = f"users:klaviyo:ensure_user:{user_sub}:lock".encode("utf-8")
    queue_key = f"users:klaviyo:ensure_user:{user_sub}:queue".encode("utf-8")
    ser_lock = lock.json().encode("utf-8")
    ser_action = action.json().encode("utf-8")

    try:
        res = await redis.evalsha(
            REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK_SHA,
            2,
            lock_key,
            queue_key,
            ser_lock,
            ser_action,
        )
    except NoScriptError:
        correct_sha = await redis.script_load(REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK)
        if correct_sha != REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK_SHA:
            raise RuntimeError(
                f"Redis script SHA mismatch: {correct_sha=} != {REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK_SHA=}"
            )

        res = await redis.evalsha(
            REDIS_SCRIPT_QUEUE_ACTION_AND_MAYBE_LOCK_SHA,
            2,
            lock_key,
            queue_key,
            ser_lock,
            ser_action,
        )

    if isinstance(res, bytes):
        assert int(res) == 2, f"{res=} should be 2 for bytes response"
        return None

    assert isinstance(res, (list, tuple)), f"{res=} should be a list or tuple"
    assert len(res) in (1, 2), f"{res=} should have length 1 or 2"

    result_enum = int(res[0])
    if result_enum == 2:
        return None

    assert len(res) == 2, f"{res=} should have length 2"
    assert int(result_enum) == 1, f"{result_enum=} should be 1"
    assert isinstance(res[1], bytes), f"{res[1]=} should be bytes"
    return KlaviyoEnsureUserQueuedAction.parse_raw(
        res[1], content_type="application/json"
    )


REDIS_SCRIPT_MAYBE_LOCK = """
local lock_key = KEYS[1]
local queue_key = KEYS[2]
local lock_val = ARGV[1]

local current_len = redis.call("LLEN", queue_key)
if tonumber(current_len) <= 0 then
    return {"0", nil}
end

local acquired_lock = redis.call("SET", lock_key, lock_val, "NX", "EX", 300)

if acquired_lock ~= nil and acquired_lock ~= false then
    local first_action = redis.call("LPOP", queue_key)
    return {"1", first_action}
end

return {"2", nil}
"""

REDIS_SCRIPT_MAYBE_LOCK_SHA = hashlib.sha1(
    REDIS_SCRIPT_MAYBE_LOCK.encode("utf-8")
).hexdigest()


async def maybe_lock(
    itgs: Itgs,
    *,
    user_sub: str,
    lock: KlaviyoEnsureUserLock,
) -> Optional[KlaviyoEnsureUserQueuedAction]:
    """Attempts to acquire a lock. If the lock is acquired, then the first queued action
    is also popped and returned, otherwise None is returned.

    If there are no actions queued for the given user, this does not attempt to
    acquire the lock and instead just returns None.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str): The sub of the user to check
        lock (KlaviyoEnsureUserLock): The lock to acquire

    Returns:
        (KlaviyoEnsureUserQueuedAction or None): The first queued action if the lock
            was acquired, otherwise None
    """
    redis = await itgs.redis()
    lock_key = f"users:klaviyo:ensure_user:{user_sub}:lock".encode("utf-8")
    queue_key = f"users:klaviyo:ensure_user:{user_sub}:queue".encode("utf-8")
    ser_lock = lock.json().encode("utf-8")

    try:
        res = await redis.evalsha(
            REDIS_SCRIPT_MAYBE_LOCK_SHA, 2, lock_key, queue_key, ser_lock
        )
    except NoScriptError:
        correct_sha = await redis.script_load(REDIS_SCRIPT_MAYBE_LOCK)
        if correct_sha != REDIS_SCRIPT_MAYBE_LOCK_SHA:
            raise RuntimeError(
                f"Redis script SHA mismatch: {correct_sha=} != {REDIS_SCRIPT_MAYBE_LOCK_SHA=}"
            )

        res = await redis.evalsha(
            REDIS_SCRIPT_MAYBE_LOCK_SHA, 2, lock_key, queue_key, ser_lock
        )

    if isinstance(res, bytes):
        assert int(res) in (0, 1), f"{res=} should be 0 or 2 for bytes response"
        return None

    assert isinstance(res, (list, tuple)), f"{res=} should be a list or tuple"
    assert len(res) in (1, 2), f"{res=} should have length 1 or 2"

    result_enum = int(res[0])
    if result_enum in (0, 2):
        return None

    assert len(res) == 2, f"{res=} should have length 2"
    assert result_enum == 1, f"{result_enum=} should be 1"
    assert isinstance(res[1], bytes), f"{res[1]=} should be bytes"
    return KlaviyoEnsureUserQueuedAction.parse_raw(
        res[1], content_type="application/json"
    )


async def release_lock(
    itgs: Itgs, *, user_sub: str, lock: KlaviyoEnsureUserLock
) -> None:
    """Releases the lock for the given user. The lock is released even if it mismatches,
    but an error is sent to slack in that case.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str): The sub of the user to check
        lock (KlaviyoEnsureUserLock): The lock to release
    """
    lock_key = f"users:klaviyo:ensure_user:{user_sub}:lock".encode("utf-8")
    redis = await itgs.redis()
    old_value = await redis.getdel(lock_key)
    if old_value is None:
        await handle_contextless_error(
            extra_info=(
                f"for {user_sub=} tried to release lock={lock.json()}, but no lock was held"
            )
        )
        return

    old_lock = KlaviyoEnsureUserLock.parse_raw(
        old_value, content_type="application/json"
    )
    if old_lock.uid != lock.uid:
        await handle_contextless_error(
            extra_info=(
                f"for {user_sub=} tried to release lock={lock.json()} but instead released lock={old_lock.json()}"
            )
        )


async def _execute_directly(
    itgs: Itgs,
    *,
    user_sub: str,
    timezone: Optional[str],
    timezone_technique: Optional[TimezoneTechniqueSlug],
    is_outside_flow: bool,
):
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
            user_klaviyo_profiles.klaviyo_id,
            user_klaviyo_profiles.course_links_by_slug
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
    k_course_links_by_slug_raw: Optional[str] = response.results[0][16]

    if email == "anonymous@example.com":
        logging.warning(
            f"User with sub {user_sub=} is anonymous (should only happen in dev)"
        )
        return

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

    course_links_by_slug: Dict[str, str] = dict()
    response = await cursor.execute(
        """
        SELECT
            courses.slug,
            course_download_links.code
        FROM course_download_links, courses, users
        WHERE
            course_download_links.course_id = courses.id
            AND users.id = course_download_links.user_id
            AND users.sub = ?
        """,
        (user_sub,),
    )
    for row in response.results or []:
        course_links_by_slug[row[0]] = (
            os.environ["ROOT_FRONTEND_URL"] + "/courses/download?code=" + row[1]
        )

    environment = os.environ["ENVIRONMENT"]
    best_phone_number = pv_phone_number or user_phone_number
    best_timezone = timezone or uns_timezone or k_timezone or "America/Los_Angeles"
    sms_notification_time = (
        None if uns_channel != "sms" else uns_preferred_notification_time
    )

    is_debug_phone_number = False
    if best_phone_number == "+15555555555":
        is_debug_phone_number = True
        best_phone_number = None

    changed_course_links_by_slug: Optional[Dict[str, str]] = dict()
    if k_course_links_by_slug_raw is not None:
        k_course_links_by_slug: Dict[str, str] = json.loads(k_course_links_by_slug_raw)
        for slug, link in k_course_links_by_slug.items():
            if slug not in course_links_by_slug:
                changed_course_links_by_slug[slug] = ""
            elif link != course_links_by_slug[slug]:
                changed_course_links_by_slug[slug] = course_links_by_slug[slug]

        for slug, link in course_links_by_slug.items():
            if slug not in k_course_links_by_slug:
                changed_course_links_by_slug[slug] = link
    else:
        changed_course_links_by_slug = dict(course_links_by_slug)

    if not changed_course_links_by_slug:
        changed_course_links_by_slug = None

    klaviyo = await itgs.klaviyo()
    correct_list_internal_identifiers = [
        "users",
        *(
            ["sms-morning"]
            if (
                sms_notification_time == "morning"
                or (is_outside_flow and sms_notification_time == "any")
            )
            else []
        ),
        *(["sms-afternoon"] if sms_notification_time == "afternoon" else []),
        *(["sms-evening"] if sms_notification_time == "evening" else []),
    ]
    dont_remove_internal_ids = set(
        [
            *(
                ["sms-morning"]
                if sms_notification_time == "any" and is_outside_flow
                else []
            ),
        ]
    )
    dont_remove_list_ids = set(
        [await klaviyo.list_id(i) for i in dont_remove_internal_ids]
    )

    correct_list_ids: List[str] = [
        (await klaviyo.list_id(i)) for i in correct_list_internal_identifiers
    ]
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
            course_links_by_slug=changed_course_links_by_slug,
        )
        await asyncio.sleep(1)
        if new_profile_id is None:
            new_profile_id = await klaviyo.get_profile_id(email=email)
            await asyncio.sleep(1)
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
                    course_links_by_slug=changed_course_links_by_slug,
                )
                await asyncio.sleep(1)

            try:
                await klaviyo.update_profile(
                    profile_id=new_profile_id,
                    phone_number=best_phone_number,
                    first_name=given_name,
                    last_name=family_name,
                    timezone=best_timezone,
                    environment=environment,
                    course_links_by_slug=changed_course_links_by_slug,
                )
                await asyncio.sleep(1)
            except DuplicateProfileError as e:
                await asyncio.sleep(1)
                await klaviyo.update_profile(
                    profile_id=e.duplicate_profile_id,
                    phone_number=None,
                )
                await cursor.execute(
                    "UPDATE user_klaviyo_profiles SET phone_number = NULL WHERE klaviyo_id = ?",
                    (e.duplicate_profile_id,),
                )
                await asyncio.sleep(1)
                await klaviyo.update_profile(
                    profile_id=new_profile_id, email=email, phone_number=None
                )
                await asyncio.sleep(1)
                await klaviyo.update_profile(
                    profile_id=new_profile_id,
                    phone_number=best_phone_number,
                    first_name=given_name,
                    last_name=family_name,
                    timezone=best_timezone,
                    environment=environment,
                    course_links_by_slug=changed_course_links_by_slug,
                )
                await asyncio.sleep(1)

        now = time.time()
        new_ukp_uid = f"oseh_ukp_{secrets.token_urlsafe(16)}"
        response = await cursor.execute(
            """
            INSERT INTO user_klaviyo_profiles (
                uid, klaviyo_id, user_id, email, phone_number, first_name, last_name, timezone,
                environment, course_links_by_slug, created_at, updated_at
            )
            SELECT
                ?, ?, users.id, ?, ?, ?, ?, ?, ?, ?, ?, ?
            FROM users
            WHERE users.sub = ?
            ON CONFLICT (user_id) DO NOTHING
            ON CONFLICT (klaviyo_id) DO NOTHING
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
                json.dumps(course_links_by_slug),
                now,
                now,
                user_sub,
            ),
        )
        if response.rows_affected is None or response.rows_affected <= 0:
            # the most common reason this happens is the user has two accounts via different identities
            # and they both have the same email. Let's see if this is happening and steal the profile
            # from the other account
            response = await cursor.execute(
                """
                SELECT 
                    users.sub 
                FROM user_klaviyo_profiles
                JOIN users ON users.id = user_klaviyo_profiles.user_id
                WHERE
                    user_klaviyo_profiles.klaviyo_id = ?
                    AND users.sub != ?
                """,
                (new_profile_id, user_sub),
            )
            if not response.results:
                await handle_contextless_error(
                    extra_info="raced creating klaviyo profile for user: "
                    + json.dumps(
                        {
                            "user_sub": user_sub,
                            "email": email,
                            "phone_number": best_phone_number,
                            "first_name": given_name,
                            "last_name": family_name,
                            "timezone": best_timezone,
                            "environment": environment,
                            "klaviyo_id": new_profile_id,
                        }
                    )
                )
                return

            old_user_sub = response.results[0][0]
            response = await cursor.execute(
                """
                UPDATE user_klaviyo_profiles SET user_id = users.id
                FROM users
                WHERE
                    users.sub = ?
                    AND user_klaviyo_profiles.klaviyo_id = ?
                    AND EXISTS (
                        SELECT 1 FROM users as old_users
                        WHERE 
                            old_users.id = user_klaviyo_profiles.user_id
                            AND old_users.sub = ?
                    )
                """,
                (user_sub, new_profile_id, old_user_sub),
            )
            if response.rows_affected is None or response.rows_affected <= 0:
                await handle_contextless_error(
                    extra_info="failed to steal klaviyo profile (no rows affected): "
                    + json.dumps(
                        {
                            "old_user_sub": old_user_sub,
                            "user_sub": user_sub,
                            "email": email,
                            "phone_number": best_phone_number,
                            "first_name": given_name,
                            "last_name": family_name,
                            "timezone": best_timezone,
                            "environment": environment,
                            "klaviyo_id": new_profile_id,
                        }
                    )
                )
                return
            await handle_contextless_error(
                extra_info="stole klaviyo profile from another user with the same email: "
                + json.dumps(
                    {
                        "old_user_sub": old_user_sub,
                        "user_sub": user_sub,
                        "email": email,
                        "phone_number": best_phone_number,
                        "first_name": given_name,
                        "last_name": family_name,
                        "timezone": best_timezone,
                        "environment": environment,
                        "klaviyo_id": new_profile_id,
                    }
                )
            )
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.klaviyo.ensure_user",
                user_sub=user_sub,
                timezone=timezone,
                timezone_technique=timezone_technique,
                is_outside_flow=is_outside_flow,
                is_bounce=False,
            )
            return

        if uns_preferred_notification_time is not None and pv_phone_number is not None:
            logging.info(
                f"Since {user_sub=} had a notification time ({uns_preferred_notification_time=}) "
                "with daily event enabled, and had a phone number verified, we are handling a "
                f'notification time update from "unset" to "text-{uns_preferred_notification_time}"'
            )
            await lib.users.stats.on_notification_time_updated(
                itgs,
                user_sub=user_sub,
                old_preference="unset",
                new_preference=f"text-{uns_preferred_notification_time}",
                changed_at=now,
            )
        else:
            logging.info(
                f"Despite creating a profile for {user_sub=}, not necessary to update "
                f"notification time stats, since either {uns_preferred_notification_time=}, "
                f", or {pv_phone_number=} would prevent them from receiving notifications"
            )

        await klaviyo.unsuppress_email(email)
        await asyncio.sleep(1)

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
            await asyncio.sleep(1)
            await klaviyo.add_profile_to_list(
                profile_id=new_profile_id,
                list_id=list_id,
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
                ON CONFLICT (user_klaviyo_profile_id, list_id) DO NOTHING
                """,
                (new_ukpl_uid, list_id, now, new_ukp_uid),
            )
            await asyncio.sleep(1)

        return

    if (
        k_email != email
        or k_phone_number != best_phone_number
        or k_first_name != given_name
        or k_last_name != family_name
        or k_timezone != best_timezone
        or k_environment != environment
        or changed_course_links_by_slug
    ):
        try:
            await klaviyo.update_profile(
                profile_id=k_klaviyo_id,
                email=email,
                external_id=user_sub,
                phone_number=best_phone_number,
                first_name=given_name,
                last_name=family_name,
                timezone=best_timezone,
                environment=environment,
                course_links_by_slug=changed_course_links_by_slug,
            )
            await asyncio.sleep(1)
        except DuplicateProfileError as e:
            await asyncio.sleep(1)
            await klaviyo.update_profile(
                profile_id=e.duplicate_profile_id,
                phone_number=None,
            )
            await cursor.execute(
                "UPDATE user_klaviyo_profiles SET phone_number = NULL WHERE klaviyo_id = ?",
                (e.duplicate_profile_id,),
            )
            await asyncio.sleep(1)
            await klaviyo.update_profile(
                profile_id=k_klaviyo_id,
                email=email,
                external_id=user_sub,
                phone_number=best_phone_number,
                first_name=given_name,
                last_name=family_name,
                timezone=best_timezone,
                environment=environment,
                course_links_by_slug=changed_course_links_by_slug,
            )
            await asyncio.sleep(1)

        await cursor.execute(
            """
            UPDATE user_klaviyo_profiles
            SET
                email = ?,
                phone_number = ?,
                first_name = ?,
                last_name = ?,
                timezone = ?,
                environment = ?,
                updated_at = ?,
                course_links_by_slug = ?
            WHERE user_klaviyo_profiles.uid = ?
            """,
            (
                email,
                best_phone_number,
                given_name,
                family_name,
                best_timezone,
                environment,
                time.time(),
                json.dumps(course_links_by_slug),
                k_uid,
            ),
        )

    if k_email != email:
        await klaviyo.unsuppress_email(email)
        await asyncio.sleep(1)
        await klaviyo.suppress_email(k_email)
        await asyncio.sleep(1)

    current_list_ids_set = set(k_list_ids)
    correct_list_ids_set = set(correct_list_ids)

    for list_id_to_remove in current_list_ids_set - correct_list_ids_set:
        if list_id_to_remove in dont_remove_list_ids:
            continue

        await klaviyo.remove_from_list(
            list_id=list_id_to_remove,
            profile_id=k_klaviyo_id,
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
        await asyncio.sleep(1)

    for list_id_to_add in correct_list_ids_set - current_list_ids_set:
        internal_id = list_id_to_internal_identifier[list_id_to_add]
        is_sms_list = internal_id.startswith("sms-")
        if is_sms_list and best_phone_number is None:
            if not is_debug_phone_number:
                slack = await itgs.slack()
                await slack.send_web_error_message(
                    f"ensure_user has {list_id_to_add=} for {email=} but {is_sms_list=} and {best_phone_number=}."
                )
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
            ON CONFLICT (user_klaviyo_profile_id, list_id) DO NOTHING
            """,
            (new_ukpl_uid, list_id_to_add, time.time(), k_uid),
        )
        await asyncio.sleep(1)


if __name__ == "__main__":

    async def main():
        user_sub = input("User sub: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.klaviyo.ensure_user", user_sub=user_sub, is_outside_flow=True
            )

    asyncio.run(main())
