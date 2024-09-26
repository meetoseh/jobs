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
local encrypted_transcription_vtt = ARGV[2]
local transcription_vtt_journal_master_key_uid = ARGV[3]
local transcription_source = ARGV[4]
local now_str = ARGV[5]
local finalize_job = ARGV[6]
local transcribe_job_progress_uid = ARGV[7]
local finalize_job_progress_uid = ARGV[8]
local transcribe_progress_finalize_job_spawn_event = ARGV[9]
local finalize_job_queued_event = ARGV[10]

local voice_note_key = "voice_notes:processing:" .. voice_note_uid
local old_encrypted_transcription_vtt = redis.call("HGET", voice_note_key, "encrypted_transcription_vtt")
if old_encrypted_transcription_vtt ~= "not_yet" then
    if old_encrypted_transcription_vtt == false then return -1 end
    return -2
end

redis.call(
    "HSET",
    voice_note_key,
    "encrypted_transcription_vtt", encrypted_transcription_vtt,
    "transcription_vtt_journal_master_key_uid", transcription_vtt_journal_master_key_uid,
    "transcription_source", transcription_source,
    "transcribe_job_finished_at", now_str
)

local transcode_job_finished_at = redis.call("HGET", voice_note_key, "transcode_job_finished_at")
if transcode_job_finished_at == "not_yet" then
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
push_job_progress(transcribe_job_progress_uid, transcribe_progress_finalize_job_spawn_event)
push_job_progress(finalize_job_progress_uid, finalize_job_queued_event)
return 2
"""
_SCRIPT_HASH = hashlib.sha1(_SCRIPT.encode("utf-8")).hexdigest()

ensure_voice_notes_transcription_finished_script_exists = (
    lib.redis_scripts.make_partial_ensure_script_exists(
        script_name="voice_notes_transcription_finished",
        script=_SCRIPT,
        script_hash=_SCRIPT_HASH,
    )
)


@dataclass
class VoiceNotesTranscriptionFinishedResultSuccessPendingTranscode:
    type: Literal["success_pending_transcode"]
    """
    - `success_pending_transcode`: the transcription was stored. the transcode
        job has not yet finished so the finalize job was not queued
    """


@dataclass
class VoiceNotesTranscriptionFinishedResultSuccessPendingFinalize:
    type: Literal["success_pending_finalize"]
    """
    - `success_finalized`: the transcription was stored. the transcode job has
        already finished, so the finalize job was queued
    """


@dataclass
class VoiceNotesTranscriptionFinishedResultNotFound:
    type: Literal["not_found"]
    """
    - `not_found`: the transcription was not stored because the voice note was
        not in the voice note processing pseudo-set
    """


@dataclass
class VoiceNotesTranscriptionFinishedResultConflict:
    type: Literal["conflict"]
    """
    - `conflict`: the transcription was not stored because the voice note already
        had a transcription
    """


VoiceNotesTranscriptionFinishedResult = Union[
    VoiceNotesTranscriptionFinishedResultSuccessPendingTranscode,
    VoiceNotesTranscriptionFinishedResultSuccessPendingFinalize,
    VoiceNotesTranscriptionFinishedResultNotFound,
    VoiceNotesTranscriptionFinishedResultConflict,
]


class VoiceNotesTranscriptionFinishedParams(TypedDict):
    voice_note_uid: bytes
    """The UID of the voice note whose transcription job finished"""
    encrypted_transcription_vtt: bytes
    """The VTT transcription of the voice note, encrypted with the journal master key"""
    transcription_vtt_journal_master_key_uid: bytes
    """The UID of the journal master key used to encrypt the transcription VTT"""
    transcription_source: bytes
    """The source of the transcription as a json object"""
    now_str: bytes
    """The current time as a string"""
    finalize_job: bytes
    """The job to finalize the voice note of the form `{"name": name, "kwargs": kwargs, "queued_at": time.time()}`"""
    transcribe_job_progress_uid: bytes
    """the job progress uid being used for the transcribe job"""
    finalize_job_progress_uid: bytes
    """the job progress uid that will be used for the finalize job if it's queued"""
    transcribe_progress_finalize_job_spawn_event: bytes
    """the json object representing the event to push to the transcribe job progress uid when spawning the finalize job"""
    finalize_job_queued_event: bytes
    """the json object representing the event to push to the finalize job progress uid when it's queued"""


async def voice_notes_transcription_finished(
    redis: redis.asyncio.client.Redis,
    /,
    **params: "Unpack[VoiceNotesTranscriptionFinishedParams]",
) -> Optional[VoiceNotesTranscriptionFinishedResult]:
    """If the voice note with the given uid is in the voice note processing pseudo-set,
    stores the transcription. If the voice note is now ready for finalization, queues
    the finalize job.

    Args:
        redis (redis.asyncio.client.Redis): The redis client or pipeline to invoke the
            script within.
        **params (VoiceNotesTranscriptionFinishedParams): The parameters for the script

    Returns:
        (VoiceNotesTranscriptionFinishedResult, None): The result, if not run
            within a pipeline, otherwise None as the result is not known until
            the pipeline is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    result = await redis.evalsha(
        _SCRIPT_HASH,
        0,
        params["voice_note_uid"],  # type: ignore
        params["encrypted_transcription_vtt"],  # type: ignore
        params["transcription_vtt_journal_master_key_uid"],  # type: ignore
        params["transcription_source"],  # type: ignore
        params["now_str"],  # type: ignore
        params["finalize_job"],  # type: ignore
        params["transcribe_job_progress_uid"],  # type: ignore
        params["finalize_job_progress_uid"],  # type: ignore
        params["transcribe_progress_finalize_job_spawn_event"],  # type: ignore
        params["finalize_job_queued_event"],  # type: ignore
    )
    if result is redis:
        return None
    return parse_voice_notes_transcription_finished_response(result)


async def safe_voice_notes_transcription_finished(
    itgs: Itgs, /, **params: "Unpack[VoiceNotesTranscriptionFinishedParams]"
) -> VoiceNotesTranscriptionFinishedResult:
    """Much like voice_notes_transcription_finished, but runs in the primary redis
    instance from the given integrations. Since this is definitely not within a
    pipeline, it can verify the script exists and ensure it provides a result.

    Args:
        itgs (Itgs): the integrations to (re)use
        **params (VoiceNotesTranscriptionFinishedParams): The parameters for the script

    Returns:
        VoiceNotesTranscriptionFinishedResult: what the script returned, already parsed
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_voice_notes_transcription_finished_script_exists(
            redis, force=force
        )

    async def _execute():
        return await voice_notes_transcription_finished(redis, **params)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_voice_notes_transcription_finished_response(
    response: Any,
) -> VoiceNotesTranscriptionFinishedResult:
    """Parses the result of the voice_notes_transcription_finished script into the typed representation"""
    assert isinstance(response, int), f"{response=}"
    if response == 1:
        return VoiceNotesTranscriptionFinishedResultSuccessPendingTranscode(
            type="success_pending_transcode"
        )
    if response == 2:
        return VoiceNotesTranscriptionFinishedResultSuccessPendingFinalize(
            type="success_pending_finalize"
        )
    if response == -1:
        return VoiceNotesTranscriptionFinishedResultNotFound(type="not_found")
    if response == -2:
        return VoiceNotesTranscriptionFinishedResultConflict(type="conflict")
    raise ValueError(f"Unknown response: {response}")
