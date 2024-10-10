import asyncio
from dataclasses import dataclass
import io
import time
from typing import Dict, List, Literal, Optional, Set, Tuple, Union, cast
from error_middleware import handle_warning
from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import (
    InstructorMemoryCachedData,
    JournalChatJobContext,
    JourneyMemoryCachedData,
    RefMemoryCachedData,
    VoiceNoteMemoryCachedData,
)
from lib.image_files.image_file_ref import ImageFileRef
from lib.journals.journal_entry_item_data import (
    JournalEntryItemData,
    JournalEntryItemDataClient,
    JournalEntryItemDataData,
    JournalEntryItemDataDataClient,
    JournalEntryItemDataDataSummary,
    JournalEntryItemDataDataTextual,
    JournalEntryItemDataDataTextualClient,
    JournalEntryItemDataDataUI,
    JournalEntryItemTextualPart,
    JournalEntryItemTextualPartClient,
    JournalEntryItemTextualPartJourney,
    JournalEntryItemTextualPartJourneyClient,
    JournalEntryItemTextualPartJourneyClientDetails,
    JournalEntryItemTextualPartParagraph,
    JournalEntryItemTextualPartVoiceNote,
    JournalEntryItemTextualPartVoiceNoteClient,
    MinimalJourneyInstructor,
)
from lib.journals.master_keys import (
    get_journal_master_key_for_decryption,
    get_journal_master_key_from_s3,
)
from lib.journeys.external_journey import ExternalJourney
from lib.journeys.read_one_external import read_one_external
import lib.image_files.auth
from lib.transcripts.model import parse_vtt_transcript
import lib.users.entitlements
import lib.voice_notes.auth
import cryptography.fernet
import itertools


@dataclass
class DataToClientInspectResult:
    """The result of inspecting what information would be required to convert
    the data to the client format"""

    pro: bool
    """True if we would need to check if the user has the pro entitlement to
    convert this data to the client format, False if we do not need to check
    """
    journeys: Set[str]
    """The journey uids which would need to be inspected to convert this data"""
    voice_notes: Set[str]
    """The voice note uids which would need to be inspected to convert this data"""


async def data_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, item: JournalEntryItemData
) -> JournalEntryItemDataClient:
    """Converts the given journal entry item data into the format expected by the
    client. This conversion may change over time; for example, journeys that are
    linked within the database may themselves change, causing the conversion from
    uid to metadata to change. Furthermore, there may be JWTs inside the client
    representation with expiration times.

    May require database or cache access.
    """
    return JournalEntryItemDataClient(
        data=await _data_data_to_client(itgs, ctx=ctx, data=item.data),
        display_author=item.display_author,
        type=item.type,
    )


def inspect_data_to_client(
    item: JournalEntryItemData,
    /,
    *,
    out: DataToClientInspectResult,
) -> None:
    """Determines what information would need to be known to convert the given journal
    entry item data into the format expected by the client. Never requires database or
    cache access.
    """
    return _inspect_data_data_to_client(item.data, out=out)


async def bulk_prepare_data_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, inspect: DataToClientInspectResult
) -> None:
    """Ensures all the data indiciated in the inspect result is available in the
    given ctx, loading anything that is missing. This will tend to be much more
    efficient than calling `data_to_client` for each item individually when there
    are many entries, as it will avoid N+1 database queries.

    Args:
        itgs (Itgs): the integrations to (re)use
        ctx (JournalChatJobContext): the context to load the data into
        inspect (DataToClientInspectResult): the result of inspecting the data
    """
    pro_task = asyncio.create_task(_bulk_prepare_pro(itgs, ctx=ctx, inspect=inspect))
    journey_task = asyncio.create_task(
        _bulk_load_journeys(itgs, ctx=ctx, inspect=inspect)
    )
    voice_note_task = asyncio.create_task(
        _bulk_load_voice_notes(itgs, ctx=ctx, inspect=inspect)
    )
    await asyncio.wait(
        [pro_task, journey_task, voice_note_task], return_when=asyncio.ALL_COMPLETED
    )
    # raise exceptions
    await pro_task
    await journey_task
    await voice_note_task


async def _bulk_prepare_pro(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, inspect: DataToClientInspectResult
) -> None:
    """Prepares the pro status for the user in the context, if required
    by the given inspect and not already in the given context
    """
    if ctx.has_pro is not None or not inspect.pro:
        return

    entitlement = await lib.users.entitlements.get_entitlement(
        itgs, user_sub=ctx.user_sub, identifier="pro"
    )
    ctx.has_pro = False if entitlement is None else entitlement.is_active


async def _bulk_load_journeys(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, inspect: DataToClientInspectResult
) -> None:
    """Loads the journeys indicated in the inspect result into the context.
    In general, we always need to get the information relating the journey to
    the user (i.e., if the user has liked that journey, the last time they took
    it, etc), and we have a 2-layer cache for metadata about the journey itself
    (e.g., the title, description, etc) with active eviction (allowing for long TTLs)

    This will handle loading all that user-specific information within one request.
    For the metadata about the journey itself, it will use the existing helpers
    that access that 2-layer cache (journeys.lib.read_one_external), so a _very_ cold start
    may require N queries anyway - but only for the first user. After that, even restarting
    the instances would only require N redis queries to refill the local cache rather than
    N database queries.
    """

    uids_for_user = [
        uid for uid in inspect.journeys if uid not in ctx.memory_cached_journeys
    ]
    if not uids_for_user:
        return

    candidate_uids_for_user: List[str] = []
    metadata_uids_for_user: List[ExternalJourney] = []

    for uid in uids_for_user:
        raw = await read_one_external(itgs, journey_uid=uid)
        if raw is None:
            ctx.memory_cached_journeys[uid] = None
            continue

        candidate_uids_for_user.append(uid)
        metadata_uids_for_user.append(raw)

    del uid  # type: ignore
    del raw  # type: ignore

    if not candidate_uids_for_user:
        return

    batch_cte = io.StringIO()

    batch_cte.write("WITH batch(uid) AS (VALUES (?)")
    for _ in range(1, len(candidate_uids_for_user)):
        batch_cte.write(", (?)")
    batch_cte.write(")")
    batch_cte_sql = batch_cte.getvalue()

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.executeunified3(
        (
            (  # get which ones just don't actually exist anymore
                f"""
{batch_cte_sql}
SELECT uid FROM batch 
WHERE 
    NOT EXISTS (
        SELECT 1 FROM journeys 
        WHERE 
            journeys.uid = batch.uid 
            AND journeys.deleted_at IS NULL
    )
                """,
                candidate_uids_for_user,
            ),
            (  # get instructor profile image file uids
                f"""
{batch_cte_sql}
SELECT 
    batch.uid AS a,
    image_files.uid AS b
FROM batch, journeys, instructors, image_files
WHERE
    journeys.uid = batch.uid
    AND journeys.deleted_at IS NULL
    AND journeys.instructor_id = instructors.id
    AND instructors.picture_image_file_id = image_files.id
                """,
                candidate_uids_for_user,
            ),
            (  # last taken at
                f"""
{batch_cte_sql}
SELECT
    batch.uid AS a,
    MAX(user_journeys.created_at) AS b
FROM batch, journeys, users, user_journeys
WHERE
    journeys.uid = batch.uid
    AND journeys.deleted_at IS NULL
    AND users.sub = ?
    AND user_journeys.user_id = users.id
    AND user_journeys.journey_id = journeys.id
GROUP BY batch.uid
                """,
                (*candidate_uids_for_user, ctx.user_sub),
            ),
            (  # liked at
                f"""
{batch_cte_sql}
SELECT
    batch.uid AS a,
    user_likes.created_at AS b
FROM batch, journeys, users, user_likes
WHERE
    journeys.uid = batch.uid
    AND journeys.deleted_at IS NULL
    AND users.sub = ?
    AND user_likes.user_id = users.id
    AND user_likes.journey_id = journeys.id
                """,
                (*candidate_uids_for_user, ctx.user_sub),
            ),
            (  # requires pro
                f"""
{batch_cte_sql}
SELECT
    batch.uid
FROM batch, journeys, course_journeys, courses
WHERE
    journeys.uid = batch.uid
    AND course_journeys.journey_id = journeys.id
    AND course_journeys.course_id = courses.id
    AND (courses.flags & 256) = 0
                """,
                candidate_uids_for_user,
            ),
        )
    )
    non_existing_uids_response = response[0]
    instructor_profile_image_uids_response = response[1]
    last_taken_at_response = response[2]
    liked_at_response = response[3]
    requires_pro_response = response[4]

    non_existing = set(
        cast(str, x) for (x,) in (non_existing_uids_response.results or [])
    )
    instructor_profile_image_uids = dict(
        (cast(str, a), cast(str, b))
        for a, b in (instructor_profile_image_uids_response.results or [])
    )
    last_taken_ats = dict(
        (cast(str, a), cast(float, b))
        for a, b in (last_taken_at_response.results or [])
    )
    liked_ats = dict(
        (cast(str, a), cast(float, b)) for a, b in (liked_at_response.results or [])
    )
    requires_pro = set(cast(str, x) for (x,) in (requires_pro_response.results or []))

    for row_uid, row_raw in zip(candidate_uids_for_user, metadata_uids_for_user):
        if row_uid in non_existing:
            ctx.memory_cached_journeys[row_uid] = None
            continue

        row_instructor_profile_image_uid = instructor_profile_image_uids.get(row_uid)
        row_last_taken_at = last_taken_ats.get(row_uid)
        row_liked_at = liked_ats.get(row_uid)
        row_requires_pro = row_uid in requires_pro

        result = JourneyMemoryCachedData(
            uid=row_raw.uid,
            title=row_raw.title,
            description=row_raw.description.text,
            darkened_background=RefMemoryCachedData(
                uid=row_raw.darkened_background_image.uid,
                jwt=await lib.image_files.auth.create_jwt(
                    itgs, row_raw.darkened_background_image.uid
                ),
            ),
            duration_seconds=row_raw.duration_seconds,
            instructor=InstructorMemoryCachedData(
                name=row_raw.instructor.name,
                image=(
                    None
                    if row_instructor_profile_image_uid is None
                    else RefMemoryCachedData(
                        uid=row_instructor_profile_image_uid,
                        jwt=await lib.image_files.auth.create_jwt(
                            itgs, row_instructor_profile_image_uid
                        ),
                    )
                ),
            ),
            last_taken_at=row_last_taken_at,
            liked_at=row_liked_at,
            requires_pro=row_requires_pro,
        )
        ctx.memory_cached_journeys[row_uid] = result


async def _bulk_load_voice_notes(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, inspect: DataToClientInspectResult
) -> None:
    """Loads the voice notes indicated in the inspect result into the context.
    In general, we always want to present the transcription we used to the user
    for clarity, and we keep that transcription encrypted in transit.
    """
    uids_for_user = [
        uid for uid in inspect.voice_notes if uid not in ctx.memory_cached_voice_notes
    ]
    if not uids_for_user:
        return

    none_consistency_rows = await _batch_load_voice_notes_from_db(
        itgs, ctx=ctx, voice_note_uids=uids_for_user, read_consistency="none"
    )

    remaining_uids = set(uids_for_user)
    for row in none_consistency_rows:
        remaining_uids.remove(row.uid)

    low_latency_rows: List[Union[_VoiceNoteFromDBRow, _VoiceNoteFromRedis]] = []
    for remaining_uid in list(remaining_uids):
        low_latency_row = await _low_latency_load_potentially_processing_voice_note(
            itgs, ctx=ctx, voice_note_uid=remaining_uid
        )
        if low_latency_row is not None:
            remaining_uids.remove(remaining_uid)
            low_latency_rows.append(low_latency_row)

    journal_master_keys_by_uid: Dict[str, Optional[cryptography.fernet.Fernet]] = dict()
    journal_master_keys_by_uid[ctx.journal_master_key.journal_master_key_uid] = (
        ctx.journal_master_key.journal_master_key
    )

    for row in itertools.chain(none_consistency_rows, low_latency_rows):
        if row.journal_master_key_uid in journal_master_keys_by_uid:
            continue
        if row.master_key_s3_file_key is not None:
            master_key_result = await get_journal_master_key_from_s3(
                itgs,
                user_journal_master_key_uid=row.journal_master_key_uid,
                user_sub=ctx.user_sub,
                s3_key=row.master_key_s3_file_key,
            )
        else:
            master_key_result = await get_journal_master_key_for_decryption(
                itgs,
                user_sub=ctx.user_sub,
                journal_master_key_uid=row.journal_master_key_uid,
            )
        if master_key_result.type != "success":
            await handle_warning(
                f"{__name__}:master_key:{master_key_result.type}",
                f"Failed to get master key for voice note `{row.uid}`",
            )
            journal_master_keys_by_uid[row.journal_master_key_uid] = None
            continue
        journal_master_keys_by_uid[row.journal_master_key_uid] = (
            master_key_result.journal_master_key
        )

    for row in itertools.chain(none_consistency_rows, low_latency_rows):
        journal_master_key = journal_master_keys_by_uid[row.journal_master_key_uid]
        if journal_master_key is None:
            ctx.memory_cached_voice_notes[row.uid] = None
            continue

        if row.src == "db":
            files = await itgs.files()
            encrypted_transcript_out = io.BytesIO()
            try:
                await files.download(
                    encrypted_transcript_out,
                    key=row.transcript_s3_file_key,
                    bucket=files.default_bucket,
                    sync=True,
                )
            except Exception as e:
                await handle_warning(
                    f"{__name__}:transcript_download",
                    f"Failed to download transcript for voice note `{row.uid}` from `{row.transcript_s3_file_key}`",
                    exc=e,
                )
                ctx.memory_cached_voice_notes[row.uid] = None
                continue
            encrypted_transcript_vtt = encrypted_transcript_out.getvalue()
        else:
            encrypted_transcript_vtt = row.encrypted_vtt_transcript

        try:
            decrypted_transcript_vtt = journal_master_key.decrypt(
                encrypted_transcript_vtt
            )
            parsed_transcript = parse_vtt_transcript(
                decrypted_transcript_vtt.decode("utf-8")
            )
        except Exception as e:
            await handle_warning(
                f"{__name__}:transcript_decrypt",
                f"Failed to decrypt or parse transcript for voice note `{row.uid}`",
                exc=e,
            )
            ctx.memory_cached_voice_notes[row.uid] = None
            continue

        ctx.memory_cached_voice_notes[row.uid] = VoiceNoteMemoryCachedData(
            uid=row.uid, transcript=parsed_transcript
        )


@dataclass
class _VoiceNoteFromRedis:
    src: Literal["redis"]
    uid: str
    journal_master_key_uid: str
    master_key_s3_file_key: Literal[None]
    encrypted_vtt_transcript: str


@dataclass
class _VoiceNoteFromDBRow:
    src: Literal["db"]
    uid: str
    journal_master_key_uid: str
    master_key_s3_file_key: str
    transcript_s3_file_key: str


async def _low_latency_load_potentially_processing_voice_note(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, voice_note_uid: str
) -> Optional[Union[_VoiceNoteFromDBRow, _VoiceNoteFromRedis]]:
    """A latency-optimized load of a single voice note which may still be processing.
    This is able to complete before the voice note completes processing
    """
    voice_note_uid_bytes = voice_note_uid.encode("utf-8")
    max_stall_time = 30

    started_at = time.time()
    redis = await itgs.redis()
    pubsub = redis.pubsub()
    try:
        await pubsub.subscribe(b"ps:voice_notes:transcripts:" + voice_note_uid_bytes)

        message_task = asyncio.create_task(
            pubsub.get_message(ignore_subscribe_messages=True, timeout=5)
        )

        while True:
            (
                user_sub,
                encrypted_transcription_vtt,
                transcription_vtt_journal_master_key_uid,
            ) = cast(
                Tuple[Optional[bytes], Optional[bytes], Optional[bytes]],
                await redis.hmget(
                    b"voice_notes:processing:" + voice_note_uid_bytes,  # type: ignore
                    b"user_sub",  # type: ignore
                    b"encrypted_transcription_vtt",  # type: ignore
                    b"journal_master_key_uid",  # type: ignore
                ),
            )

            if (
                user_sub is None
                or encrypted_transcription_vtt is None
                or transcription_vtt_journal_master_key_uid is None
            ):
                # not in redis; either it's in the db at weak consistency or it doesn't exist anywhere
                # (NOTE: the order we checked is important as we know it can go redis -> db but not db -> redis)
                await _safe_cancel(message_task)
                db_load = await _batch_load_voice_notes_from_db(
                    itgs,
                    ctx=ctx,
                    voice_note_uids=[voice_note_uid],
                    read_consistency="weak",
                )
                return db_load[0] if db_load else None

            if user_sub != ctx.user_sub.encode("utf-8"):
                # weird this voice note isn't for the right user, treat it like it doesn't exist anywhere
                await _safe_cancel(message_task)
                return None

            if (
                encrypted_transcription_vtt != b"not_yet"
                and transcription_vtt_journal_master_key_uid != b"not_yet"
            ):
                # the voice note already has a transcription ready
                await _safe_cancel(message_task)
                return _VoiceNoteFromRedis(
                    src="redis",
                    uid=voice_note_uid,
                    journal_master_key_uid=transcription_vtt_journal_master_key_uid.decode(
                        "utf-8"
                    ),
                    master_key_s3_file_key=None,
                    encrypted_vtt_transcript=encrypted_transcription_vtt.decode(
                        "utf-8"
                    ),
                )

            message = await message_task
            if message is None:
                if time.time() - started_at > max_stall_time:
                    # we've waited too long for the voice note to finish processing
                    await handle_warning(
                        f"{__name__}:voice_note_stall",
                        f"Voice note `{voice_note_uid}` has been processing for too long to retrieve",
                    )
                    return None
                message_task = asyncio.create_task(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=5)
                )
                continue

            msg_data = cast(bytes, message.get("data"))
            assert isinstance(msg_data, bytes), msg_data
            msg = io.BytesIO(msg_data)

            msg_voice_note_uid_length = int.from_bytes(msg.read(4), "big", signed=False)
            msg_voice_note_uid_bytes = msg.read(msg_voice_note_uid_length)
            msg_journal_master_key_uid_length = int.from_bytes(
                msg.read(4), "big", signed=False
            )
            msg_journal_master_key_uid = msg.read(
                msg_journal_master_key_uid_length
            ).decode("utf-8")
            msg_encrypted_vtt_transcript_length = int.from_bytes(
                msg.read(8), "big", signed=False
            )
            msg_encrypted_vtt_transcript = msg.read(
                msg_encrypted_vtt_transcript_length
            ).decode("utf-8")

            assert msg_voice_note_uid_bytes == voice_note_uid_bytes, (
                msg_voice_note_uid_bytes,
                voice_note_uid_bytes,
            )
            return _VoiceNoteFromRedis(
                src="redis",
                uid=voice_note_uid,
                journal_master_key_uid=msg_journal_master_key_uid,
                master_key_s3_file_key=None,
                encrypted_vtt_transcript=msg_encrypted_vtt_transcript,
            )
    finally:
        await pubsub.aclose()


async def _safe_cancel(task: asyncio.Task) -> None:
    if not task.cancel():
        return

    try:
        await task
    except asyncio.CancelledError:
        current_task = asyncio.current_task()
        if current_task is not None and current_task.cancelled():
            raise


async def _batch_load_voice_notes_from_db(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    voice_note_uids: List[str],
    read_consistency: Literal["none", "weak", "strong"],
) -> List[_VoiceNoteFromDBRow]:
    batch_cte = io.StringIO()

    batch_cte.write("WITH batch(uid) AS (VALUES (?)")
    for _ in range(1, len(voice_note_uids)):
        batch_cte.write(", (?)")

    batch_cte.write(")")

    conn = await itgs.conn()
    cursor = conn.cursor(read_consistency)

    response = await cursor.execute(
        f"""
{batch_cte.getvalue()}
SELECT
    voice_notes.uid,
    user_journal_master_keys.uid,
    master_key_s3_files.key,
    transcript_s3_files.key
FROM
    batch,
    voice_notes,
    user_journal_master_keys,
    s3_files AS master_key_s3_files,
    s3_files AS transcript_s3_files
WHERE
    batch.uid = voice_notes.uid
    AND voice_notes.user_id = (SELECT users.id FROM users WHERE users.sub=?)
    AND user_journal_master_keys.id = voice_notes.user_journal_master_key_id
    AND user_journal_master_keys.user_id = voice_notes.user_id
    AND master_key_s3_files.id = user_journal_master_keys.s3_file_id
    AND transcript_s3_files.id = voice_notes.transcript_s3_file_id
        """,
        [
            *voice_note_uids,
            ctx.user_sub,
        ],
    )

    result: List[_VoiceNoteFromDBRow] = []
    for row in response.results or []:
        result.append(
            _VoiceNoteFromDBRow(
                src="db",
                uid=row[0],
                journal_master_key_uid=row[1],
                master_key_s3_file_key=row[2],
                transcript_s3_file_key=row[3],
            )
        )

    return result


async def _data_data_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, data: JournalEntryItemDataData
) -> JournalEntryItemDataDataClient:
    if data.type == "textual":
        return await _data_data_textual_to_client(itgs, ctx=ctx, data=data)
    if data.type == "ui":
        return await _data_data_ui_to_client(itgs, ctx=ctx, data=data)
    if data.type == "summary":
        return await _data_data_summary_to_client(itgs, ctx=ctx, data=data)
    raise ValueError(f"Unknown data type: {data}")


def _inspect_data_data_to_client(
    data: JournalEntryItemDataData, /, *, out: DataToClientInspectResult
) -> None:
    if data.type == "textual":
        return _inspect_data_data_textual_to_client(data, out=out)
    if data.type == "ui":
        return _inspect_data_data_ui_to_client(data, out=out)
    if data.type == "summary":
        return _inspect_data_data_summary_to_client(data, out=out)
    raise ValueError(f"Unknown data type: {data}")


async def _data_data_textual_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, data: JournalEntryItemDataDataTextual
) -> JournalEntryItemDataDataTextualClient:
    parts: List[JournalEntryItemTextualPartClient] = []
    for part in data.parts:
        parts.append(await _textual_part_to_client(itgs, ctx=ctx, part=part))
    return JournalEntryItemDataDataTextualClient(parts=parts, type=data.type)


def _inspect_data_data_textual_to_client(
    data: JournalEntryItemDataDataTextual, /, *, out: DataToClientInspectResult
) -> None:
    for part in data.parts:
        _inspect_textual_part_to_client(part, out=out)


async def _textual_part_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, part: JournalEntryItemTextualPart
) -> JournalEntryItemTextualPartClient:
    if part.type == "journey":
        return await _textual_part_journey_to_client(itgs, ctx=ctx, part=part)
    if part.type == "paragraph":
        return await _textual_part_paragraph_to_client(itgs, ctx=ctx, part=part)
    if part.type == "voice_note":
        return await _textual_part_voice_note_to_client(itgs, ctx=ctx, part=part)
    raise ValueError(f"Unknown textual part type: {part}")


def _inspect_textual_part_to_client(
    part: JournalEntryItemTextualPart, /, *, out: DataToClientInspectResult
) -> None:
    if part.type == "journey":
        return _inspect_textual_part_journey_to_client(part, out=out)
    elif part.type == "paragraph":
        return _inspect_textual_part_paragraph_to_client(part, out=out)
    elif part.type == "voice_note":
        return _inspect_textual_part_voice_note_to_client(part, out=out)
    raise ValueError(f"Unknown textual part type: {part}")


async def get_journal_chat_job_journey_metadata(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, journey_uid: str
) -> Optional[JourneyMemoryCachedData]:
    """Gets metadata on the journey with the given uid if it exists and can
    be seen by the user the job is for, otherwise returns None
    """
    cached = ctx.memory_cached_journeys.get(journey_uid)
    if cached is not None:
        return cached
    if journey_uid in ctx.memory_cached_journeys:
        return None

    await _bulk_load_journeys(
        itgs,
        ctx=ctx,
        inspect=DataToClientInspectResult(
            pro=False, journeys={journey_uid}, voice_notes=set()
        ),
    )
    return ctx.memory_cached_journeys[journey_uid]


async def get_journal_chat_job_voice_note_metadata(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, voice_note_uid: str
) -> Optional[VoiceNoteMemoryCachedData]:
    """Gets metadata on the voice note with the given uid if it exists and can
    be seen by the user the job is for, otherwise returns None
    """
    cached = ctx.memory_cached_voice_notes.get(voice_note_uid)
    if cached is not None:
        return cached
    if voice_note_uid in ctx.memory_cached_voice_notes:
        return None

    await _bulk_load_voice_notes(
        itgs,
        ctx=ctx,
        inspect=DataToClientInspectResult(
            pro=False, journeys=set(), voice_notes={voice_note_uid}
        ),
    )
    return ctx.memory_cached_voice_notes[voice_note_uid]


async def _textual_part_journey_to_client(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    part: JournalEntryItemTextualPartJourney,
) -> Union[
    JournalEntryItemTextualPartJourneyClient, JournalEntryItemTextualPartParagraph
]:
    details = await get_journal_chat_job_journey_metadata(
        itgs, ctx=ctx, journey_uid=part.uid
    )
    if details is None:
        return JournalEntryItemTextualPartParagraph(
            type="paragraph", value="(link to deleted journey)"
        )

    has_pro = ctx.has_pro
    if has_pro is None and details.requires_pro:
        entitlement = await lib.users.entitlements.get_entitlement(
            itgs, user_sub=ctx.user_sub, identifier="pro"
        )
        has_pro = entitlement is not None and entitlement.is_active
        ctx.has_pro = has_pro

    return JournalEntryItemTextualPartJourneyClient(
        details=JournalEntryItemTextualPartJourneyClientDetails(
            uid=details.uid,
            title=details.title,
            description=details.description,
            darkened_background=ImageFileRef(
                uid=details.darkened_background.uid,
                jwt=details.darkened_background.jwt,
            ),
            duration_seconds=details.duration_seconds,
            instructor=MinimalJourneyInstructor(
                name=details.instructor.name,
                image=(
                    None
                    if details.instructor.image is None
                    else ImageFileRef(
                        uid=details.instructor.image.uid,
                        jwt=details.instructor.image.jwt,
                    )
                ),
            ),
            last_taken_at=details.last_taken_at,
            liked_at=details.liked_at,
            access=(
                "free"
                if not details.requires_pro
                else ("paid-requires-upgrade" if not has_pro else "paid-unlocked")
            ),
        ),
        type=part.type,
        uid=part.uid,
    )


def _inspect_textual_part_journey_to_client(
    part: JournalEntryItemTextualPartJourney, /, *, out: DataToClientInspectResult
) -> None:
    out.journeys.add(part.uid)
    out.pro = True
    return None


async def _textual_part_paragraph_to_client(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    part: JournalEntryItemTextualPartParagraph,
) -> JournalEntryItemTextualPartParagraph:
    return part


def _inspect_textual_part_paragraph_to_client(
    part: JournalEntryItemTextualPartParagraph, /, *, out: DataToClientInspectResult
) -> None:
    return None


async def _textual_part_voice_note_to_client(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    part: JournalEntryItemTextualPartVoiceNote,
) -> Union[
    JournalEntryItemTextualPartVoiceNoteClient, JournalEntryItemTextualPartParagraph
]:
    voice_note = await get_journal_chat_job_voice_note_metadata(
        itgs, ctx=ctx, voice_note_uid=part.voice_note_uid
    )
    if voice_note is None:
        return JournalEntryItemTextualPartParagraph(
            type="paragraph", value="(link to deleted voice note)"
        )
    return JournalEntryItemTextualPartVoiceNoteClient(
        transcription=voice_note.transcript.to_external(uid=""),
        type="voice_note",
        voice_note_jwt=await lib.voice_notes.auth.create_jwt(
            itgs, voice_note_uid=part.voice_note_uid
        ),
        voice_note_uid=part.voice_note_uid,
    )


def _inspect_textual_part_voice_note_to_client(
    part: JournalEntryItemTextualPartVoiceNote, /, *, out: DataToClientInspectResult
) -> None:
    out.voice_notes.add(part.voice_note_uid)


async def _data_data_ui_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, data: JournalEntryItemDataDataUI
) -> JournalEntryItemDataDataUI:
    return data


def _inspect_data_data_ui_to_client(
    data: JournalEntryItemDataDataUI, /, *, out: DataToClientInspectResult
) -> None:
    return None


async def _data_data_summary_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, data: JournalEntryItemDataDataSummary
) -> JournalEntryItemDataDataSummary:
    return data


def _inspect_data_data_summary_to_client(
    data: JournalEntryItemDataDataSummary, /, *, out: DataToClientInspectResult
) -> None:
    return None
