from typing import Any, Literal, Optional, List, Union
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from itgs import Itgs
from redis_helpers.run_with_prep import run_with_prep

JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT = """
local journal_chat_uid = ARGV[1]
local log_id = ARGV[2]
local last_event = ARGV[3]
local now = tonumber(ARGV[4])

local hash_key = "journals:journal_chat_jobs:" .. journal_chat_uid
local data = redis.call(
    "HMGET",
    hash_key,
    "user_sub",
    "log_id",
    "journal_entry_uid"
)

if data[2] ~= log_id then
  return {-1, data[2]}
end

local user_sub = data[1]
local journal_entry_uid = data[3]

redis.call("DEL", "journals:journal_entry_to_chat_job:" .. journal_entry_uid)
redis.call("ZREM", "journals:journal_chat_jobs:purgatory", journal_chat_uid)
redis.call("DEL", hash_key)
local event_list_key = "journal_chats:" .. journal_chat_uid .. ":events"
redis.call("RPUSH", event_list_key, last_event)
redis.call("EXPIREAT", event_list_key, now + 60 * 60)
redis.call("PUBLISH", "ps:" .. event_list_key, last_event)

local cnt_by_user_key = "journals:count_queued_journal_chat_jobs_by_user:" .. user_sub
local new_cnt = redis.call("DECR", cnt_by_user_key)
if new_cnt <= 0 then
    redis.call("DEL", cnt_by_user_key)
end

return {1, new_cnt}
"""

JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT_HASH = hashlib.sha1(
    JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_journal_chat_job_finish_ensured_at: Optional[float] = None


async def ensure_journal_chat_job_finish_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the journal_chat_job_finish lua script is loaded into redis."""
    global _last_journal_chat_job_finish_ensured_at

    now = time.time()
    if (
        not force
        and _last_journal_chat_job_finish_ensured_at is not None
        and (now - _last_journal_chat_job_finish_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT)
        assert (
            correct_hash == JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT_HASH=}"

    if (
        _last_journal_chat_job_finish_ensured_at is None
        or _last_journal_chat_job_finish_ensured_at < now
    ):
        _last_journal_chat_job_finish_ensured_at = now


@dataclass
class JournalChatJobFinishResultLogIdMismatch:
    type: Literal["log_id_mismatch"]
    actual: Optional[bytes]


@dataclass
class JournalChatJobFinishResultSuccess:
    type: Literal["success"]
    remaining_num_jobs_for_user_in_queue: int


JournalChatJobFinishResult = Union[
    JournalChatJobFinishResultLogIdMismatch, JournalChatJobFinishResultSuccess
]


async def journal_chat_job_finish(
    redis: redis.asyncio.client.Redis,
    /,
    *,
    journal_chat_uid: bytes,
    log_id: bytes,
    last_event: bytes,
    now: int,
) -> Optional[JournalChatJobFinishResult]:
    """Removes the journal chat job with the given uid from the purgatory set and
    associated hash, and adds the last event to the journal chat's event list.

    Before removing the job, it checks that the log_id in the hash matches the
    provided log_id. If it does not match, it returns a
    JournalChatJobFinishResultLogIdMismatch.

    Args:
        redis (redis.asyncio.client.Redis): The redis client

    Returns:
        JournalChatJobFinishResult, None: The result, if available. None if executed
            within a transaction, since the result is not known until the
            transaction is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        JOURNAL_CHAT_JOB_FINISH_LUA_SCRIPT_HASH,
        0,
        journal_chat_uid,  # type: ignore
        log_id,  # type: ignore
        last_event,  # type: ignore
        str(now).encode("ascii"),  # type: ignore
    )
    if res is redis:
        return None
    return parse_journal_chat_job_finish_result(res)


async def safe_journal_chat_job_finish(
    itgs: Itgs,
    /,
    *,
    journal_chat_uid: bytes,
    log_id: bytes,
    last_event: bytes,
    now: int,
) -> JournalChatJobFinishResult:
    """Same as journal_chat_job_finish, but always runs in the standard redis
    instance of the given itgs and thus doesn't need an optional return value
    """
    redis = await itgs.redis()

    async def prepare(force: bool):
        await ensure_journal_chat_job_finish_script_exists(redis, force=force)

    async def execute():
        return await journal_chat_job_finish(
            redis,
            journal_chat_uid=journal_chat_uid,
            log_id=log_id,
            last_event=last_event,
            now=now,
        )

    res = await run_with_prep(prepare, execute)
    assert res is not None
    return res


def parse_journal_chat_job_finish_result(raw: Any) -> JournalChatJobFinishResult:
    assert isinstance(raw, (list, tuple)), raw
    assert len(raw) == 2

    type_ = int(raw[0])
    if type_ == -1:
        assert isinstance(raw[1], bytes), raw
        return JournalChatJobFinishResultLogIdMismatch(
            type="log_id_mismatch", actual=raw[1]
        )

    assert type_ == 1, raw
    return JournalChatJobFinishResultSuccess(
        type="success",
        remaining_num_jobs_for_user_in_queue=int(raw[1]),
    )
