from typing import List, Optional, Union, cast
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
from lib.journeys.read_one_external import read_one_external
import lib.image_files.auth
import lib.users.entitlements


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


async def _data_data_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, data: JournalEntryItemDataData
) -> JournalEntryItemDataDataClient:
    if data.type == "textual":
        return await _data_data_textual_to_client(itgs, ctx=ctx, data=data)
    if data.type == "ui":
        return await _data_data_ui_to_client(itgs, ctx=ctx, data=data)
    raise ValueError(f"Unknown data type: {data}")


async def _data_data_textual_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, data: JournalEntryItemDataDataTextual
) -> JournalEntryItemDataDataTextualClient:
    parts: List[JournalEntryItemTextualPartClient] = []
    for part in data.parts:
        parts.append(await _textual_part_to_client(itgs, ctx=ctx, part=part))
    return JournalEntryItemDataDataTextualClient(parts=parts, type=data.type)


async def _textual_part_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, part: JournalEntryItemTextualPart
) -> JournalEntryItemTextualPartClient:
    if part.type == "journey":
        return await _textual_part_journey_to_client(itgs, ctx=ctx, part=part)
    if part.type == "paragraph":
        return await _textual_part_paragraph_to_client(itgs, ctx=ctx, part=part)
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

    # Although this cache doesn't save us a database query, it does save a
    # reasonable amount of bandwidth
    raw = await read_one_external(itgs, journey_uid=journey_uid)
    if raw is None:
        ctx.memory_cached_journeys[journey_uid] = None
        return None

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.executeunified3(
        (
            (
                "SELECT 1 FROM journeys WHERE uid=? AND deleted_at IS NULL",
                (journey_uid,),
            ),
            (
                """
SELECT image_files.uid AS a FROM journeys, instructors, image_files
WHERE
    journeys.uid = ?
    AND journeys.deleted_at IS NULL
    AND journeys.instructor_id = instructors.id
    AND instructors.picture_image_file_id = image_files.id
                """,
                (journey_uid,),
            ),
            (
                """
SELECT
    MAX(user_journeys.created_at) AS a
FROM journeys, users, user_journeys
WHERE
    journeys.uid = ?
    AND journeys.deleted_at IS NULL
    AND users.sub = ?
    AND user_journeys.user_id = users.id
    AND user_journeys.journey_id = journeys.id
                """,
                (journey_uid, ctx.user_sub),
            ),
            (
                """
SELECT user_likes.created_at AS a
FROM journeys, users, user_likes
WHERE
    journeys.uid = ?
    AND journeys.deleted_at IS NULL
    AND users.sub = ?
    AND user_likes.user_id = users.id
    AND user_likes.journey_id = journeys.id
                """,
                (journey_uid, ctx.user_sub),
            ),
            (
                """
SELECT
    1
FROM journeys, course_journeys, courses
WHERE
    journeys.uid = ?
    AND course_journeys.journey_id = journeys.id
    AND course_journeys.course_id = courses.id
    AND (courses.flags & 256) = 0
                """,
                (journey_uid,),
            ),
        )
    )

    if not response[0].results:
        ctx.memory_cached_journeys[journey_uid] = None
        return None

    instructor_profile_image_uid = (
        None if not response[1].results else cast(str, response[1].results[0][0])
    )
    last_taken_at = (
        None
        if not response[2].results
        else cast(Optional[float], response[2].results[0][0])
    )
    liked_at = (
        None if not response[3].results else cast(float, response[3].results[0][0])
    )
    requires_pro = not not response[4].results

    result = JourneyMemoryCachedData(
        uid=raw.uid,
        title=raw.title,
        description=raw.description.text,
        darkened_background=RefMemoryCachedData(
            uid=raw.darkened_background_image.uid,
            jwt=await lib.image_files.auth.create_jwt(
                itgs, raw.darkened_background_image.uid
            ),
        ),
        duration_seconds=raw.duration_seconds,
        instructor=InstructorMemoryCachedData(
            name=raw.instructor.name,
            image=(
                None
                if instructor_profile_image_uid is None
                else RefMemoryCachedData(
                    uid=instructor_profile_image_uid,
                    jwt=await lib.image_files.auth.create_jwt(
                        itgs, instructor_profile_image_uid
                    ),
                )
            ),
        ),
        last_taken_at=last_taken_at,
        liked_at=liked_at,
        requires_pro=requires_pro,
    )
    ctx.memory_cached_journeys[journey_uid] = result
    return result


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


async def _textual_part_paragraph_to_client(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    part: JournalEntryItemTextualPartParagraph,
) -> JournalEntryItemTextualPartParagraph:
    return part


async def _data_data_ui_to_client(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, data: JournalEntryItemDataDataUI
) -> JournalEntryItemDataDataUI:
    return data
