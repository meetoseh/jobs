import hashlib
from typing import Any, Literal, TypedDict, Union, Optional, TYPE_CHECKING
from itgs import Itgs
import lib.redis_scripts
import redis.asyncio.client
from dataclasses import dataclass

from redis_helpers.run_with_prep import run_with_prep

if TYPE_CHECKING:
    from typing import Unpack


_SCRIPT = """
local voice_note_uid = ARGV[1]
local stitched_s3_key = ARGV[2]
local transcribe_job = ARGV[3]
local transcribe_job_progress_uid = ARGV[4]
local transcribe_job_event = ARGV[5]
local transcode_job = ARGV[6]
local transcode_job_progress_uid = ARGV[7]
local transcode_job_event = ARGV[8]
local primary_job_spawn_transcode_event = ARGV[9]
local primary_job_spawn_transcribe_event = ARGV[10]
local now_str = ARGV[11]

local voice_note_key = "voice_notes:processing:" .. voice_note_uid
local old_stitched_s3_key = redis.call("HGET", voice_note_key, "stitched_s3_key")
if old_stitched_s3_key ~= 'not_yet' then
    if old_stitched_s3_key == false then return -1 end
    return -2
end

local primary_job_progress_uid = redis.call("HGET", voice_note_key, "job_progress_uid")
if primary_job_progress_uid == false then
    return -1
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

redis.call(
    "HSET",
    voice_note_key, 
    "stitched_s3_key", stitched_s3_key,
    "transcribe_job_queued_at", now_str,
    "transcode_job_queued_at", now_str
)
redis.call("RPUSH", "jobs:hot", transcribe_job, transcode_job)
push_job_progress(transcribe_job_progress_uid, transcribe_job_event)
push_job_progress(transcode_job_progress_uid, transcode_job_event)
push_job_progress(primary_job_progress_uid, primary_job_spawn_transcribe_event)
push_job_progress(primary_job_progress_uid, primary_job_spawn_transcode_event)
return 1
"""
_SCRIPT_HASH = hashlib.sha1(_SCRIPT.encode("utf-8")).hexdigest()

ensure_voice_notes_upload_success_stitched_script_exists = (
    lib.redis_scripts.make_partial_ensure_script_exists(
        script_name="voice_notes_upload_success_stitched",
        script=_SCRIPT,
        script_hash=_SCRIPT_HASH,
    )
)


@dataclass
class VoiceNotesUploadSuccessStitchedResultSuccess:
    type: Literal["success"]
    """
    - `success`: the operation completed successfully
    """


@dataclass
class VoiceNotesUploadSuccessStitchedResultNotFound:
    type: Literal["not_found"]
    """
    - `not_found`: there was no voice note with that uid in the processing pseudo-set
    """


@dataclass
class VoiceNotesUploadSuccessStitchedResultBadState:
    type: Literal["bad_state"]
    """
    - `bad_state`: there is a voice not with that uid in the processing pseudo-set, but
      it already has a stitched s3 key set
    """


VoiceNotesUploadSuccessStitchedResult = Union[
    VoiceNotesUploadSuccessStitchedResultSuccess,
    VoiceNotesUploadSuccessStitchedResultNotFound,
    VoiceNotesUploadSuccessStitchedResultBadState,
]


class VoiceNotesUploadSuccessStitchedParams(TypedDict):
    voice_note_uid: bytes
    """The voice note uid"""
    stitched_s3_key: bytes
    """The s3 key which contains the uploaded audio file with the parts all stitched together"""
    transcribe_job: bytes
    """A utf-8 encoded json object of the form `{"name": name, "kwargs": kwargs, "queued_at": time.time()}`"""
    transcribe_job_progress_uid: bytes
    """The job progress uid assigned to the transcribe job"""
    transcribe_job_event: bytes
    """The event to push to the job progress uid assigned to the transcribe job"""
    transcode_job: bytes
    """A utf-8 encoded json object of the form `{"name": name, "kwargs": kwargs, "queued_at": time.time()}`"""
    transcode_job_progress_uid: bytes
    """The job progress uid assigned to the transcode job"""
    transcode_job_event: bytes
    """The event to push to the job progress uid assigned to the transcode job"""
    primary_job_spawn_transcode_event: bytes
    """The event to push to the primary job progress uid to indicate we are spawning the transcode job"""
    primary_job_spawn_transcribe_event: bytes
    """The event to push to the primary job progress uid to indicate we are spawning the transcribe job"""
    now_str: bytes
    """The current time as a string, e.g., `str(time.time()).encode('ascii')`"""


async def voice_notes_upload_success_stitched(
    redis: redis.asyncio.client.Redis,
    /,
    **params: "Unpack[VoiceNotesUploadSuccessStitchedParams]",
) -> Optional[VoiceNotesUploadSuccessStitchedResult]:
    """If the voice note is in the processing pseudo-set within redis, i.e., there exists
    a `voice_notes:processing:{voice_note_uid}` key, then this script will set the stitched
    s3 key to the given value, the transcribe job queued at and the transcode job queued at
    to the given time, and queue the transcribe and transcode jobs.


    Args:
        redis (redis.asyncio.client.Redis): The redis client or pipeline to invoke the
            script within.
        **params (VoiceNotesUploadSuccessStitchedParams): The parameters for the script

    Returns:
        (VoiceNotesUploadSuccessStitchedResult, None): The result, if not run
            within a pipeline, otherwise None as the result is not known until
            the pipeline is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    result = await redis.evalsha(
        _SCRIPT_HASH,
        0,
        params["voice_note_uid"],  # type: ignore
        params["stitched_s3_key"],  # type: ignore
        params["transcribe_job"],  # type: ignore
        params["transcribe_job_progress_uid"],  # type: ignore
        params["transcribe_job_event"],  # type: ignore
        params["transcode_job"],  # type: ignore
        params["transcode_job_progress_uid"],  # type: ignore
        params["transcode_job_event"],  # type: ignore
        params["primary_job_spawn_transcode_event"],  # type: ignore
        params["primary_job_spawn_transcribe_event"],  # type: ignore
        params["now_str"],  # type: ignore
    )
    if result is redis:
        return None
    return parse_voice_notes_upload_success_stitched_response(result)


async def safe_voice_notes_upload_success_stitched(
    itgs: Itgs, /, **params: "Unpack[VoiceNotesUploadSuccessStitchedParams]"
) -> VoiceNotesUploadSuccessStitchedResult:
    """Much like example, but runs in the primary redis instance from the given
    integrations. Since this is definitely not within a pipeline, it can verify
    the script exists and ensure it provides a result.

    Args:
        itgs (Itgs): the integrations to (re)use
        **params (ExampleParams): The parameters for the script

    Returns:
        ExampleResult: what the script returned, already parsed
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_voice_notes_upload_success_stitched_script_exists(
            redis, force=force
        )

    async def _execute():
        return await voice_notes_upload_success_stitched(redis, **params)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_voice_notes_upload_success_stitched_response(
    response: Any,
) -> VoiceNotesUploadSuccessStitchedResult:
    """Parses the result of the voice_notes_upload_success_stitched script into the typed representation"""
    assert isinstance(response, int), f"{response=}"
    if response == 1:
        return VoiceNotesUploadSuccessStitchedResultSuccess(type="success")
    if response == -1:
        return VoiceNotesUploadSuccessStitchedResultNotFound(type="not_found")
    if response == -2:
        return VoiceNotesUploadSuccessStitchedResultBadState(type="bad_state")
    raise ValueError(f"Unknown response: {response}")
