from typing import Any, Literal, Optional, List, Union, cast
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT = """
local queue_key = KEYS[1]
local now = tonumber(ARGV[1])
local log_id = ARGV[2]
local hostname = ARGV[3]

local journal_chat_uid = redis.call("LPOP", queue_key)
if journal_chat_uid == false then
    return {-1}
end

redis.call("ZADD", "journals:journal_chat_jobs:purgatory", now, journal_chat_uid)

local hash_key = "journals:journal_chat_jobs:" .. journal_chat_uid
redis.call("HINCRBY", hash_key, "starts", 1)
redis.call("HSET", hash_key, "start_time", now, "started_by", hostname, "log_id", log_id)

local events_list_key = "journal_chats:" .. journal_chat_uid .. ":events"
local num_events = redis.call("LLEN", events_list_key)

local data = redis.call(
    "HMGET",
    hash_key,
    "starts",
    "queued_at",
    "user_sub",
    "journal_entry_uid",
    "journal_master_key_uid",
    "encrypted_task"
)


return {
    1,
    journal_chat_uid,
    data[1],
    now,
    hostname,
    log_id,
    data[2],
    data[3],
    data[4],
    data[5],
    data[6],
    num_events
}
"""

JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT_HASH = hashlib.sha1(
    JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_journal_chat_job_try_lock_ensured_at: Optional[float] = None


async def ensure_journal_chat_job_try_lock_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the journal_chat_job_try_lock lua script is loaded into redis."""
    global _last_journal_chat_job_try_lock_ensured_at

    now = time.time()
    if (
        not force
        and _last_journal_chat_job_try_lock_ensured_at is not None
        and (now - _last_journal_chat_job_try_lock_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT)
        assert (
            correct_hash == JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT_HASH=}"

    if (
        _last_journal_chat_job_try_lock_ensured_at is None
        or _last_journal_chat_job_try_lock_ensured_at < now
    ):
        _last_journal_chat_job_try_lock_ensured_at = now


@dataclass
class JournalChatJobTryLockResultSuccess:
    type: Literal["success"]
    """We were successfully able to lock a journal chat job"""
    journal_chat_uid: str
    """The uid of the journal chat job that was locked"""
    starts: int
    """The number of times this job has been started"""
    start_time: int
    """When this job was started by this instance"""
    started_by: str
    """The hostname of our instance that is working on this job"""
    log_id: str
    """The log identifier to help grepping logs related to this job"""
    queued_at: int
    """When this job was queued in seconds from the unix epoch"""
    user_sub: str
    """The sub of the user this job is for"""
    journal_entry_uid: str
    """The uid of the journal entry this job is intended to edit"""
    journal_master_key_uid: str
    """The uid of the journal master key used to encrypt encrypted_task"""
    encrypted_task: bytes
    """
    a JournalChatObject, converted to json, Fernet encrypted with the journal master key with
    the uid `journal_master_key_uid`, then base64url encoded.
    """
    num_events: int
    """The number of events in the events list for this journal chat job"""


@dataclass
class JournalChatJobTryLockResultEmptyQueue:
    type: Literal["empty_queue"]
    """The queue was empty, no jobs to lock"""


JournalChatJobTryLockResult = Union[
    JournalChatJobTryLockResultSuccess, JournalChatJobTryLockResultEmptyQueue
]


async def journal_chat_job_try_lock(
    redis: redis.asyncio.client.Redis,
    /,
    *,
    queue_key: bytes,
    now: int,
    log_id: bytes,
    hostname: bytes,
) -> Optional[JournalChatJobTryLockResult]:
    """Attempts to pull off one journal chat job from the queue, moving it into
    purgatory. You should always queue an event to the events list for the job
    after locking it, but in order to encrypt that event you'll need to select
    a master key based on the user the event is for, hence why that cannot be done
    within this call.

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        queue_key (bytes): the queue to pop from
        now (int): the current time in seconds from the unix epoch
        log_id (bytes): a unique identifier for this job
        hostname (bytes): the hostname of the instance that is attempting to lock
            this job

    Returns:
        JournalChatJobTryLockResult, None: The result, if available. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        JOURNAL_CHAT_JOB_TRY_LOCK_LUA_SCRIPT_HASH,
        1,
        queue_key,  # type: ignore
        str(now).encode("ascii"),  # type: ignore
        log_id,  # type: ignore
        hostname,  # type: ignore
    )
    if res is redis:
        return None
    return parse_journal_chat_job_try_lock_response(res)


async def safe_journal_chat_job_try_lock(
    itgs: Itgs,
    /,
    *,
    queue_key: bytes,
    now: int,
    log_id: bytes,
    hostname: bytes,
) -> JournalChatJobTryLockResult:
    """Same as journal_chat_job_try_lock, but always runs in the standard redis
    instance of the given itgs and thus doesn't need an optional return value
    """
    redis = await itgs.redis()

    async def prepare(force: bool):
        await ensure_journal_chat_job_try_lock_script_exists(redis, force=force)

    async def execute():
        return await journal_chat_job_try_lock(
            redis,
            queue_key=queue_key,
            now=now,
            log_id=log_id,
            hostname=hostname,
        )

    res = await run_with_prep(prepare, execute)
    assert res is not None
    return res


def parse_journal_chat_job_try_lock_response(res: Any) -> JournalChatJobTryLockResult:
    """Parses the result of the jorunal chat job try lock script into
    a more interpretable format
    """
    assert isinstance(res, (list, tuple)), res
    assert len(res) == 1 or len(res) == 12, res
    if len(res) == 1:
        assert int(res[0]) == -1, res
        return JournalChatJobTryLockResultEmptyQueue(type="empty_queue")

    assert int(res[0]) == 1, res
    typed_res = cast(List[bytes], res)
    return JournalChatJobTryLockResultSuccess(
        type="success",
        journal_chat_uid=typed_res[1].decode("utf-8"),
        starts=int(typed_res[2]),
        start_time=int(typed_res[3]),
        started_by=typed_res[4].decode("utf-8"),
        log_id=typed_res[5].decode("utf-8"),
        queued_at=int(typed_res[6]),
        user_sub=typed_res[7].decode("utf-8"),
        journal_entry_uid=typed_res[8].decode("utf-8"),
        journal_master_key_uid=typed_res[9].decode("utf-8"),
        encrypted_task=typed_res[10],
        num_events=int(typed_res[11]),
    )
