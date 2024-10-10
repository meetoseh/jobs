import hashlib
from typing import Any, Literal, Union, Optional, TypedDict, TYPE_CHECKING
from itgs import Itgs
import lib.redis_scripts
import redis.asyncio.client
from dataclasses import dataclass

from redis_helpers.run_with_prep import run_with_prep

if TYPE_CHECKING:
    from typing import Unpack


_SCRIPT = """
local voice_note_uid = ARGV[1]
local time_vs_intensity = ARGV[2]
local now_str = ARGV[3]
local finalize_job = ARGV[4]
local analyze_job_progress_uid = ARGV[5]
local finalize_job_progress_uid = ARGV[6]
local analyze_progress_finalize_job_spawn_event = ARGV[7]
local finalize_job_queued_event = ARGV[8]

local voice_note_key = "voice_notes:processing:" .. voice_note_uid
local old_time_vs_intensity = redis.call("HGET", voice_note_key, "encrypted_time_vs_intensity")
if old_time_vs_intensity ~= "not_yet" then
    if old_time_vs_intensity == false then return -1 end
    return -2
end

redis.call(
    "HSET",
    voice_note_key,
    "encrypted_time_vs_intensity", time_vs_intensity,
    "analyze_job_finished_at", now_str
)

local transcode_job_finished_at = redis.call("HGET", voice_note_key, "transcode_job_finished_at")
if transcode_job_finished_at == "not_yet" then
    return 1
end

local transcribe_job_finished_at = redis.call("HGET", voice_note_key, "transcribe_job_finished_at")
if transcribe_job_finished_at == "not_yet" then
    return 1
end

local function push_job_progress(uid, evt)
    local events_key = "jobs:progress:events:" .. uid
    local count_key = "jobs:progress:count:" .. uid

    local new_length = redis.call("RPUSH", events_key, evt)
    while new_length > 50 do
        redis.call("LPOP", events_key)
        new_length = new_length - 1
    end

    redis.call("INCR", count_key)
    redis.call("PUBLISH", "ps:jobs:progress:" .. uid, '[' .. evt .. ']')

    redis.call("EXPIRE", events_key, 1800)
    redis.call("EXPIRE", count_key, 1800)
end

redis.call("RPUSH", "jobs:hot", finalize_job)
redis.call("HSET", voice_note_key, "finalize_job_queued_at", now_str)
push_job_progress(analyze_job_progress_uid, analyze_progress_finalize_job_spawn_event)
push_job_progress(finalize_job_progress_uid, finalize_job_queued_event)
return 2
"""
_SCRIPT_HASH = hashlib.sha1(_SCRIPT.encode("utf-8")).hexdigest()

ensure_voice_notes_analyze_finished_script_exists = (
    lib.redis_scripts.make_partial_ensure_script_exists(
        script_name="voice_notes_analyze_finished",
        script=_SCRIPT,
        script_hash=_SCRIPT_HASH,
    )
)


@dataclass
class VoiceNotesAnalyzeFinishedResultSuccessPendingOther:
    type: Literal["success_pending_other"]
    """
    - `success`: the analysis was stored successfully, but other
        jobs haven't finished yet so the finalize job wasn't queued
    """


@dataclass
class VoiceNotesAnalyzeFinishedResultSuccessPendingFinalize:
    type: Literal["success_pending_finalize"]
    """
    - `success_pending_finalize`: the analysis was stored successfully
        and the finalize job was queued
    """


@dataclass
class VoiceNotesAnalyzeFinishedResultNotFound:
    type: Literal["not_found"]
    """
    - `not_found`: the indicated voice note was not in the processing
        pseudo-set, so we did nothing
    """


@dataclass
class VoiceNotesAnalyzeFinishedResultConflict:
    type: Literal["conflict"]
    """
    - `conflict`: the indicated voice note already had an analysis so
        we did nothing
    """


VoiceNotesAnalyzeFinishedResult = Union[
    VoiceNotesAnalyzeFinishedResultSuccessPendingOther,
    VoiceNotesAnalyzeFinishedResultSuccessPendingFinalize,
    VoiceNotesAnalyzeFinishedResultNotFound,
    VoiceNotesAnalyzeFinishedResultConflict,
]


class VoiceNotesAnalyzeFinishedParams(TypedDict):
    voice_note_uid: bytes
    """the uid of the voice note whose analyze job finished"""
    encrypted_time_vs_intensity: bytes
    """the computed json-lines file (the raw bytes for the file) corresponding to the
    time vs intensity graph. the format of this file is documented in the `voice_notes`
    table documentation. encrypted with the journal master key.
    """
    now_str: bytes
    """the current time in string form as if by `str(time.time())`"""
    finalize_job: bytes
    """the finalize job to queue in the form `{"name": name, "kwargs": kwargs, "queued_at": time.time()}`"""
    analyze_job_progress_uid: bytes
    """the job progress uid being used for the analyze job"""
    finalize_job_progress_uid: bytes
    """the job progress uid that will be used for the finalize job if it's queued"""
    analyze_progress_finalize_job_spawn_event: bytes
    """the json object representing the event to push to the analyze job progress uid when spawning the finalize job"""
    finalize_job_queued_event: bytes
    """the json object representing the event to push to the finalize job progress uid when it's queued"""


async def voice_notes_analyze_finished(
    redis: redis.asyncio.client.Redis,
    /,
    **params: "Unpack[VoiceNotesAnalyzeFinishedParams]",
) -> Optional[VoiceNotesAnalyzeFinishedResult]:
    """Does X

    Args:
        redis (redis.asyncio.client.Redis): The redis client or pipeline to invoke the
            script within.
        **params (VoiceNotesAnalyzeFinishedParams): The parameters for the script

    Returns:
        (VoiceNotesAnalyzeFinishedResult, None): The result, if not run within a pipeline, otherwise None
            as the result is not known until the pipeline is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    result = await redis.evalsha(
        _SCRIPT_HASH,
        0,
        params["voice_note_uid"],  # type: ignore
        params["encrypted_time_vs_intensity"],  # type: ignore
        params["now_str"],  # type: ignore
        params["finalize_job"],  # type: ignore
        params["analyze_job_progress_uid"],  # type: ignore
        params["finalize_job_progress_uid"],  # type: ignore
        params["analyze_progress_finalize_job_spawn_event"],  # type: ignore
        params["finalize_job_queued_event"],  # type: ignore
    )
    if result is redis:
        return None
    return parse_voice_notes_analyze_finished_response(result)


async def safe_voice_notes_analyze_finished(
    itgs: Itgs, /, **params: "Unpack[VoiceNotesAnalyzeFinishedParams]"
) -> VoiceNotesAnalyzeFinishedResult:
    """Much like voice_notes_analyze_finished, but runs in the primary redis instance from the given
    integrations. Since this is definitely not within a pipeline, it can verify
    the script exists and ensure it provides a result.

    Args:
        itgs (Itgs): the integrations to (re)use
        **params (VoiceNotesAnalyzeFinishedParams): The parameters for the script

    Returns:
        VoiceNotesAnalyzeFinishedResult: what the script returned, already parsed
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_voice_notes_analyze_finished_script_exists(redis, force=force)

    async def _execute():
        return await voice_notes_analyze_finished(redis, **params)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_voice_notes_analyze_finished_response(
    response: Any,
) -> VoiceNotesAnalyzeFinishedResult:
    """Parses the result of the voice_notes_analyze_finished script into the typed representation"""
    assert isinstance(response, int), f"{response=}"
    if response == 1:
        return VoiceNotesAnalyzeFinishedResultSuccessPendingOther(
            type="success_pending_other"
        )
    if response == 2:
        return VoiceNotesAnalyzeFinishedResultSuccessPendingFinalize(
            type="success_pending_finalize"
        )
    if response == -1:
        return VoiceNotesAnalyzeFinishedResultNotFound(type="not_found")
    if response == -2:
        return VoiceNotesAnalyzeFinishedResultConflict(type="conflict")
    raise ValueError(f"Unknown response: {response}")
