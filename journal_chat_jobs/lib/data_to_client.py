import asyncio
from dataclasses import dataclass
import io
from typing import List, Optional, Set, Union, cast
from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import (
    InstructorMemoryCachedData,
    JournalChatJobContext,
    JourneyMemoryCachedData,
    RefMemoryCachedData,
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
    MinimalJourneyInstructor,
)
from lib.journeys.external_journey import ExternalJourney
from lib.journeys.read_one_external import read_one_external
import lib.image_files.auth
import lib.users.entitlements


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
    await asyncio.wait([pro_task, journey_task], return_when=asyncio.ALL_COMPLETED)
    # raise exceptions
    await pro_task
    await journey_task


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

    del uid
    del raw

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
    raise ValueError(f"Unknown textual part type: {part}")


def _inspect_textual_part_to_client(
    part: JournalEntryItemTextualPart, /, *, out: DataToClientInspectResult
) -> None:
    if part.type == "journey":
        return _inspect_textual_part_journey_to_client(part, out=out)
    elif part.type == "paragraph":
        return _inspect_textual_part_paragraph_to_client(part, out=out)
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
        inspect=DataToClientInspectResult(pro=False, journeys={journey_uid}),
    )
    return ctx.memory_cached_journeys[journey_uid]


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
