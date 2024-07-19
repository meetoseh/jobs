"""
Filters journeys using cosine similarity between the user message and the journey,
then a few head-to-head comparisons with gpt-4o-mini
"""

import asyncio
from dataclasses import dataclass
import json
import random
from typing import (
    AsyncIterable,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)
import numpy as np

from error_middleware import handle_warning
from itgs import Itgs
from journal_chat_jobs.lib.fast_top_1 import fast_top_1
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from journal_chat_jobs.lib.journey_embeddings import get_journey_embeddings
from journal_chat_jobs.lib.openai_ratelimits import reserve_openai_tokens
from lib.image_files.image_file_ref import ImageFileRef
from lib.journals.journal_chat_redis_packet import (
    EventBatchPacketDataItemDataError,
)
from lib.journals.journal_entry_item_data import (
    JournalEntryItemData,
    JournalEntryItemDataClient,
    JournalEntryItemDataDataTextual,
    JournalEntryItemDataDataTextualClient,
    JournalEntryItemTextualPartJourney,
    JournalEntryItemTextualPartJourneyClient,
    JournalEntryItemTextualPartJourneyClientDetails,
    JournalEntryItemTextualPartParagraph,
    MinimalJourneyInstructor,
)
from lib.transcripts.cache import get_transcript

from lib.transcripts.model import Transcript
from lib.journals.journal_stats import JournalStats
import openai
import os
import lib.users.entitlements
import lib.image_files.auth
import journal_chat_jobs.lib.chat_helper as chat_helper


TECHNIQUE_PARAMETERS = cast(
    chat_helper.TechniqueParameters,
    {
        "type": "llm",
        "platform": "openai",
        "model": "gpt-4o",
    },
)
SMALL_MODEL = "gpt-4o-mini"

BIG_RATELIMIT_CATEGORY = "gpt-4o"
SMALL_RATELIMIT_CATEGORY = "gpt-4o-mini"
CONCURRENCY = 10


async def handle_chat(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """Uses cosine similarity of the user message as a first pass at filtering
    journeys, then uses gpt-4o-mini for some head-to-head comparisons

    When `replace_index` is None, this responds to the conversation of the the
    current point, where presumably the last message in the conversation is from
    the user.

    Alternatively, if `replace_index` is set, replaces the message at that index
    with a new response from the system. This will only work well if the item being
    replaced was also generated by this function
    """
    await chat_helper.handle_chat_outer_loop(
        itgs,
        ctx,
        technique_parameters=TECHNIQUE_PARAMETERS,
        response_pipeline=_response_pipeline,
        prompt_identifier="chat_embeddings_rank_and_pluck",
    )


async def _response_pipeline(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    greeting: JournalEntryItemData,
    user_message: JournalEntryItemData,
    stats: JournalStats,
) -> AsyncIterable[
    Union[
        Tuple[bool, JournalEntryItemData, JournalEntryItemDataClient],
        Tuple[Literal[None], EventBatchPacketDataItemDataError],
    ]
]:
    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Running prechecks...")
    text_greeting = chat_helper.extract_as_text(greeting)
    text_user_message = chat_helper.extract_as_text(user_message)

    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    try:
        # using openai.AsyncOpenAI breaks redis somehow... if you try to ping
        # redis after it, no matter what (even in a fresh connection), it will fail
        # with asyncio.CancelledError
        moderation_response = await asyncio.to_thread(
            client.moderations.create, input=text_user_message
        )
    except Exception as e:
        stats.incr_system_chats_failed_net_unknown(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="moderation",
            category="net",
            detail="unknown",
            error_name=type(e).__name__,
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to connect with LLM",
            detail="moderation error",
        )
        return

    if moderation_response.results[0].categories.self_harm_intent:
        yield True, *chat_helper.get_message_from_paragraphs(
            [
                "In a crisis?",
                "Text HELLO to 741741 to connect with a volunteer Crisis Counselor",
                "Free 24/7 support at your fingerprints.",
                "*741741 is a registered trademark of Crisis Text Line, Inc.",
            ]
        )
        return

    if moderation_response.results[0].flagged:
        stats.incr_system_chats_failed_llm(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="moderation",
            category="llm",
            detail="flagged",
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to create response",
            detail="flagged",
        )
        return

    pro_entitlement = await lib.users.entitlements.get_entitlement(
        itgs, user_sub=ctx.user_sub, identifier="pro"
    )
    has_pro = pro_entitlement is not None and pro_entitlement.is_active

    await chat_helper.publish_spinner(
        itgs, ctx=ctx, message="Writing initial response..."
    )

    try:
        await chat_helper.reserve_tokens(
            itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=2048
        )
        chat_response = await asyncio.to_thread(
            client.chat.completions.create,
            messages=[
                {
                    "role": "system",
                    "content": """
Objective: Craft responses that acknowledge the user’s current feelings and
validate their experience.

Write 2-3 sentences acknowledging the user’s current feelings. Use empathetic
language to show understanding and support. Keep your response professional
and forward-thinking; for example, do not apologize for negative emotions, but
instead thank them for sharing.

Do not conclude your messages. End responses on a strong semantically meaningful
sentence. If they did not share a lot of information, use only two sentences.
                        """.strip(),
                },
                {"role": "assistant", "content": text_greeting},
                {"role": "user", "content": text_user_message},
            ],
            model=TECHNIQUE_PARAMETERS["model"],
            max_tokens=2048,
        )
    except Exception as e:
        if os.environ["ENVIRONMENT"] == "dev":
            await handle_warning(
                f"{__name__}:llm", f"Failed to connect with LLM", exc=e
            )
        stats.incr_system_chats_failed_net_unknown(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="chat",
            category="net",
            detail="unknown",
            error_name=type(e).__name__,
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to connect with LLM",
            detail="empathy error",
        )
        return

    empathy_message = chat_response.choices[0].message
    if empathy_message.content is None:
        stats.incr_system_chats_failed_llm(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="chat_initial_empathy",
            category="llm",
            detail="no content",
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to create response",
            detail="no content",
        )
        return

    yield False, *chat_helper.get_message_from_text(empathy_message.content)
    await chat_helper.publish_spinner(
        itgs, ctx=ctx, message="Searching for free journey..."
    )

    allowing_free_progress_messages = asyncio.Event()
    allowing_free_progress_messages.set()

    allowing_pro_progress_messages = asyncio.Event()

    free_class_selection_task = asyncio.create_task(
        select_class(
            itgs,
            ctx=ctx,
            user_message=user_message,
            stats=stats,
            pro=False,
            has_pro=has_pro,
            report_progress=allowing_free_progress_messages,
        )
    )
    pro_class_selection_task = asyncio.create_task(
        select_class(
            itgs,
            ctx=ctx,
            user_message=user_message,
            stats=stats,
            pro=True,
            has_pro=has_pro,
            report_progress=allowing_pro_progress_messages,
        )
    )

    try:
        free_class, free_class_transcript = await free_class_selection_task
    except ValueError as e:
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to select free journey",
            detail=str(e),
        )
        return

    try:
        await chat_helper.reserve_tokens(
            itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=2048
        )
        chat_response = await asyncio.to_thread(
            client.chat.completions.create,
            messages=[
                {
                    "role": "system",
                    "content": "You should always suggest 1-2 sentences to complete their response. Do not attempt to rewrite their existing response, and do not repeat content from their existing response.",
                },
                {
                    "role": "user",
                    "content": (
                        f"""
Finish my response to a user who wrote

{user_message}

I want to recommend them {free_class.title} by {free_class.instructor.name}. Here's
the transcript of that class

```txt
{str(free_class_transcript)}
```

Here's a general description:

{free_class.description}

I need you to add 1-3 sentences to my response so far. Do not include my
response in your message. There has been some time since I wrote my initial
message, so start this one with a proper sentence (starting with an uppercase
letter). Use emojis if appropriate for visual variety.

Include specific information about the contents of the class, taking from either
the description or the transcript. Try to suggest ways to get the most out of the
class. Talk about why I picked this class for them, and try to make sure
its clear how it relates to their original message. Do not use italics or bold.
If necessary, use paragraph breaks (2 newlines).


{empathy_message.content}
                    """
                    ),
                },
            ],
            model=TECHNIQUE_PARAMETERS["model"],
            max_tokens=2048,
        )
    except Exception as e:
        if os.environ["ENVIRONMENT"] == "dev":
            await handle_warning(
                f"{__name__}:llm", f"Failed to connect with LLM", exc=e
            )
        stats.incr_system_chats_failed_net_unknown(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="chat_free_class",
            category="net",
            detail="unknown",
            error_name=type(e).__name__,
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to connect with LLM",
            detail="empathy error",
        )
        return

    free_class_message = chat_response.choices[0].message
    if free_class_message.content is None:
        stats.incr_system_chats_failed_llm(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="chat_free_class",
            category="llm",
            detail="no content",
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to create response",
            detail="no content",
        )
        return

    paragraphs1 = chat_helper.break_paragraphs(empathy_message.content)
    paragraphs2 = chat_helper.break_paragraphs(free_class_message.content)
    paragraphs = paragraphs1 + paragraphs2
    yield False, JournalEntryItemData(
        type="chat",
        data=JournalEntryItemDataDataTextual(
            type="textual",
            parts=[
                *[
                    JournalEntryItemTextualPartParagraph(
                        type="paragraph",
                        value=p,
                    )
                    for p in paragraphs
                ],
                JournalEntryItemTextualPartJourney(
                    type="journey",
                    uid=free_class.uid,
                ),
            ],
        ),
        display_author="other",
    ), JournalEntryItemDataClient(
        type="chat",
        data=JournalEntryItemDataDataTextualClient(
            type="textual",
            parts=[
                *[
                    JournalEntryItemTextualPartParagraph(
                        type="paragraph",
                        value=p,
                    )
                    for p in paragraphs
                ],
                JournalEntryItemTextualPartJourneyClient(
                    details=free_class,
                    type="journey",
                    uid=free_class.uid,
                ),
            ],
        ),
        display_author="other",
    )

    allowing_pro_progress_messages.set()
    try:
        pro_class, pro_class_transcript = await pro_class_selection_task
    except ValueError as e:
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to select pro journey",
            detail=str(e),
        )
        return

    try:
        await chat_helper.reserve_tokens(
            itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=4096
        )
        chat_response = await asyncio.to_thread(
            client.chat.completions.create,
            messages=[
                {
                    "role": "system",
                    "content": "You should always suggest 1-2 sentences to complete their response. Do not attempt to rewrite their existing response, and do not repeat content from their existing response.",
                },
                {
                    "role": "user",
                    "content": (
                        f"""
Finish my response to a user who wrote

{user_message}

I am going to recommend them {pro_class.title} by {pro_class.instructor.name}. Here's
the transcript of that class

```txt
{str(pro_class_transcript)}
```

Here's a general description:

{pro_class.description}

I need you to add 1-3 sentences to my response so far. Do not include my
response in your message. There has been some time since I wrote my initial
message, so start this one with a proper sentence (starting with an uppercase
letter). Use emojis if appropriate for visual variety.

Include specific information about the contents of the class, taking from either
the description or the transcript. Try to suggest ways to get the most out of the
class. Talk about why I picked this class for them, and try to make sure
its clear how it relates to their original message.  Do not use italics or bold.
If necessary, use paragraph breaks (2 newlines).

---

{empathy_message.content}

{free_class_message.content}

[{free_class.title}](journey:{chat_helper.make_id(free_class.title)})
                    """
                    ),
                },
            ],
            model=TECHNIQUE_PARAMETERS["model"],
            max_tokens=4096,
        )
    except Exception as e:
        if os.environ["ENVIRONMENT"] == "dev":
            await handle_warning(
                f"{__name__}:llm", f"Failed to connect with LLM", exc=e
            )
        stats.incr_system_chats_failed_net_unknown(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="chat_pro_class",
            category="net",
            detail="unknown",
            error_name=type(e).__name__,
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to connect with LLM",
            detail="pro class suggestion error",
        )
        return

    pro_class_message = chat_response.choices[0].message
    if pro_class_message.content is None:
        stats.incr_system_chats_failed_llm(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier="chat_pro_class",
            category="llm",
            detail="no content",
        )
        yield None, EventBatchPacketDataItemDataError(
            type="error",
            message="Failed to create response",
            detail="no content",
        )
        return

    paragraphs3 = chat_helper.break_paragraphs(pro_class_message.content)
    paragraphs = paragraphs1 + paragraphs2
    yield True, JournalEntryItemData(
        type="chat",
        data=JournalEntryItemDataDataTextual(
            type="textual",
            parts=[
                *[
                    JournalEntryItemTextualPartParagraph(
                        type="paragraph",
                        value=p,
                    )
                    for p in paragraphs1 + paragraphs2
                ],
                JournalEntryItemTextualPartJourney(
                    type="journey",
                    uid=free_class.uid,
                ),
                *[
                    JournalEntryItemTextualPartParagraph(
                        type="paragraph",
                        value=p,
                    )
                    for p in paragraphs3
                ],
                JournalEntryItemTextualPartJourney(
                    type="journey",
                    uid=pro_class.uid,
                ),
            ],
        ),
        display_author="other",
    ), JournalEntryItemDataClient(
        type="chat",
        data=JournalEntryItemDataDataTextualClient(
            type="textual",
            parts=[
                *[
                    JournalEntryItemTextualPartParagraph(
                        type="paragraph",
                        value=p,
                    )
                    for p in paragraphs1 + paragraphs2
                ],
                JournalEntryItemTextualPartJourneyClient(
                    details=free_class,
                    type="journey",
                    uid=free_class.uid,
                ),
                *[
                    JournalEntryItemTextualPartParagraph(
                        type="paragraph",
                        value=p,
                    )
                    for p in paragraphs3
                ],
                JournalEntryItemTextualPartJourneyClient(
                    details=pro_class,
                    type="journey",
                    uid=pro_class.uid,
                ),
            ],
        ),
        display_author="other",
    )


@dataclass
class PossibleJourney:
    journey_uid: str
    audio_file_uid: str
    journey_title: str
    journey_description: str
    instructor_name: str
    transcript_uid: str


async def select_class(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    user_message: JournalEntryItemData,
    stats: JournalStats,
    pro: bool,
    has_pro: bool,
    report_progress: asyncio.Event,
) -> Tuple[JournalEntryItemTextualPartJourneyClientDetails, Transcript]:
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    responses = await cursor.executeunified3(
        (
            ("SELECT 1 FROM users WHERE sub=?", (ctx.user_sub,)),
            (
                """
WITH 
relevant_journeys(journey_id) AS (
SELECT j.id
FROM journeys AS j
WHERE
    j.deleted_at IS NULL
    AND (? = 0 OR NOT EXISTS (
        SELECT 1 FROM courses, course_journeys
        WHERE
            courses.id = course_journeys.course_id
            AND course_journeys.journey_id = j.id
            AND (courses.flags & 128) = 0
    ))
    AND (? = 0 OR EXISTS (
        SELECT 1 FROM courses, course_journeys
        WHERE
            courses.id = course_journeys.course_id
            AND course_journeys.journey_id = j.id
            AND (courses.flags & 256) <> 0
    ))
    AND j.special_category IS NULL
)
, user_journey_counts_raw(journey_id, cnt) AS (
SELECT
    uj.journey_id,
    COUNT(*)
FROM user_journeys AS uj
WHERE uj.user_id = (SELECT users.id FROM users WHERE users.sub=?)
)
, user_journey_counts(journey_id, cnt) AS (
SELECT
    relevant_journeys.journey_id,
    COALESCE(user_journey_counts_raw.cnt, 0)
FROM relevant_journeys
LEFT OUTER JOIN user_journey_counts_raw ON user_journey_counts_raw.journey_id = relevant_journeys.journey_id
)
SELECT 
    journeys.uid AS d1,
    content_files.uid AS d2,
    journeys.title AS d3,
    journeys.description AS d4,
    instructors.name AS d5,
    transcripts.uid AS d6
FROM 
    user_journey_counts, 
    journeys,
    content_files,
    instructors,
    content_file_transcripts,
    transcripts
WHERE
    user_journey_counts.journey_id = journeys.id
    AND user_journey_counts.cnt = (SELECT MIN(cnt) FROM user_journey_counts)
    AND content_files.id = journeys.audio_content_file_id
    AND content_file_transcripts.id = (
        SELECT cft.id FROM content_file_transcripts AS cft
        WHERE cft.content_file_id = content_files.id
        ORDER BY cft.created_at DESC, cft.uid ASC
    )
    AND content_file_transcripts.transcript_id = transcripts.id
    AND instructors.id = journeys.instructor_id
                """,
                [
                    int(not pro),
                    int(pro),
                    ctx.user_sub,
                ],
            ),
        )
    )

    if not responses[0].results:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier=f"select_class_{'' if pro else 'non_'}pro:user_not_found",
            category="internal",
        )
        raise ValueError(f"User {ctx.user_sub} not found")

    if not responses[1].results:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier=f"select_class_{'' if pro else 'non_'}pro:no_journeys",
            category="internal",
        )
        raise ValueError(f"No journeys found for user {ctx.user_sub}")

    possible_journeys: List[PossibleJourney] = []
    for row in responses[1].results:
        possible_journeys.append(
            PossibleJourney(
                journey_uid=row[0],
                audio_file_uid=row[1],
                journey_title=row[2],
                journey_description=row[3],
                instructor_name=row[4],
                transcript_uid=row[5],
            )
        )

    if report_progress.is_set():
        await chat_helper.publish_pbar(
            itgs,
            ctx=ctx,
            message=f"Rating {len(possible_journeys)} journeys...",
            at=0,
            of=len(possible_journeys),
        )

    user_message_text = chat_helper.extract_as_text(user_message)

    if report_progress.is_set():
        await chat_helper.publish_spinner(
            itgs,
            ctx=ctx,
            message="Rating journeys",
            detail="Generating user message embedding",
        )

    embeddings = await get_journey_embeddings(itgs)
    if embeddings.type != "success":
        raise ValueError(f"Failed to get journey embeddings: {embeddings.type}")

    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    try:
        user_message_embedding = await asyncio.to_thread(
            client.embeddings.create,
            input=user_message_text,
            model=embeddings.model,
            encoding_format="float",
        )
    except Exception as e:
        await handle_warning(
            f"{__name__}:embeddings",
            f"Failed to create user message embedding",
            exc=e,
        )
        raise ValueError("Failed to create user message embedding")

    user_message_embedding_ndarray = np.array(
        user_message_embedding.data[0].embedding, dtype=np.float64
    )

    similarities = np.dot(embeddings.journey_embeddings, user_message_embedding_ndarray)

    possible_journey_by_uid: Dict[str, PossibleJourney] = dict(
        (journey.journey_uid, journey) for journey in possible_journeys
    )
    rated_journeys: List[Tuple[float, PossibleJourney]] = []
    for idx, journey_uid in enumerate(embeddings.journey_uids):
        journey = possible_journey_by_uid.get(journey_uid)
        if journey is None:
            continue
        rated_journeys.append((float(similarities[idx]), journey))

    journey_ratings_by_uid = dict(
        (journey.journey_uid, rating) for rating, journey in rated_journeys
    )

    eligible_journeys = [v[1] for v in sorted(rated_journeys, key=lambda x: -x[0])]

    max_cnt = 2 if not has_pro else 4
    eligible_journeys = eligible_journeys[:max_cnt]

    if not eligible_journeys:
        raise ValueError("No eligible journeys found")

    async def _compare(a: PossibleJourney, b: PossibleJourney) -> int:
        """Compares two possible journeys using the llm"""
        transcript1 = await get_transcript(itgs, a.transcript_uid)
        transcript2 = await get_transcript(itgs, b.transcript_uid)

        if transcript1 is None and transcript2 is None:
            return random.choice([-1, 1])
        if transcript1 is None:
            return 1
        if transcript2 is None:
            return -1

        a_id = chat_helper.make_id(a.journey_title)
        b_id = chat_helper.make_id(b.journey_title)

        if a_id == b_id or a_id == "" or b_id == "":
            a_id = a.journey_uid
            b_id = b.journey_uid

        if a_id == b_id:
            return random.choice([-1, 1])

        reserve_result = await reserve_openai_tokens(
            itgs, category=SMALL_RATELIMIT_CATEGORY, count=2048, max_wait_seconds=0
        )
        if reserve_result.type != "immediate":
            semantic_a = journey_ratings_by_uid.get(a.journey_uid, 0)
            semantic_b = journey_ratings_by_uid.get(b.journey_uid, 0)
            if semantic_a == semantic_b:
                return 0
            return -1 if semantic_a > semantic_b else 1

        comparison_response = await asyncio.to_thread(
            client.chat.completions.create,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "The user provides a message and two journeys. You determine which journey is a better "
                        "fit for the message. A better fit is one that is more relevant to the user's needs."
                    ),
                },
                {
                    "role": "user",
                    "content": f"""
First, write a paragraph on the strongest link between the first journey and the users message.

Then, write a paragraph on the strongest link between the second journey and the users message.
                    
Finally, choose select the journey for which your argument is more convincing. Prefer arguments
that are direct and specific, while avoiding abstract or tenuous relationships.
                    
The message is:

```txt
{user_message_text}
```

The first journey is:

id: {a_id}
title: {a.journey_title}
description: {a.journey_description}
instructor: {a.instructor_name}
transcript:

```txt
{str(transcript1.to_internal())}
```

The second journey is:

id: {b_id}
title: {b.journey_title}
description: {b.journey_description}
instructor: {b.instructor_name}
transcript:

```txt
{str(transcript2.to_internal())}
```
""",
                },
            ],
            model=SMALL_MODEL,
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "select_journey",
                        "description": "Select the given journey as the preferred journey",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "description": "The ID of the journey which is preferred amongst the two",
                                }
                            },
                            "required": ["id"],
                        },
                    },
                }
            ],
            tool_choice={"type": "function", "function": {"name": "select_journey"}},
            max_tokens=2048,
        )

        if not comparison_response.choices:
            return random.choice([-1, 1])

        comparison_message = comparison_response.choices[0].message
        if not comparison_message.tool_calls or len(comparison_message.tool_calls) != 1:
            return random.choice([-1, 1])

        comparison_arguments_json = comparison_message.tool_calls[0].function.arguments
        try:
            comparison_arguments = json.loads(comparison_arguments_json)
        except Exception:
            return random.choice([-1, 1])

        if not isinstance(comparison_arguments, dict):
            return random.choice([-1, 1])

        selected_id = comparison_arguments.get("id")
        if not isinstance(selected_id, str):
            return random.choice([-1, 1])

        if selected_id == a_id:
            return -1
        if selected_id == b_id:
            return 1
        return random.choice([-1, 1])

    num_comparisons = 0
    _spinner_task = cast(Optional[asyncio.Task], None)

    async def _compare_with_progress(a: PossibleJourney, b: PossibleJourney) -> int:
        nonlocal num_comparisons, _spinner_task

        result = await _compare(a, b)
        num_comparisons += 1
        if _spinner_task is not None and _spinner_task.done():
            await _spinner_task
            _spinner_task = None
        if _spinner_task is None and report_progress.is_set():
            _spinner_task = asyncio.create_task(
                chat_helper.publish_spinner(
                    itgs,
                    ctx=ctx,
                    message=f"Ranking top {len(eligible_journeys)} pairwise...",
                    detail=f"Comparisons so far: {num_comparisons}",
                )
            )
        return result

    best_journey = await fast_top_1(
        eligible_journeys,
        compare=_compare_with_progress,
        semaphore=asyncio.Semaphore(CONCURRENCY),
    )
    if _spinner_task is not None:
        await _spinner_task
    del _spinner_task

    if report_progress.is_set():
        await chat_helper.publish_spinner(
            itgs,
            ctx=ctx,
            message=f"Finishing up (total comparisons: {num_comparisons})...",
        )

    response = await cursor.execute(
        """
SELECT
    journeys.uid,
    journeys.title,
    journeys.description,
    darkened_image_files.uid,
    content_files.duration_seconds,
    instructors.name,
    instructor_profile_pictures.uid,
    (SELECT MAX(uj.created_at) FROM user_journeys AS uj WHERE uj.journey_id = journeys.id AND uj.user_id = (SELECT users.id FROM users WHERE users.sub=?)) AS last_taken_at,
    (SELECT ul.created_at FROM user_likes AS ul WHERE ul.journey_id = journeys.id AND ul.user_id = (SELECT users.id FROM users WHERE users.sub=?)) AS liked_at
FROM 
    journeys, 
    image_files AS darkened_image_files, 
    content_files, 
    instructors
LEFT OUTER JOIN image_files AS instructor_profile_pictures ON instructor_profile_pictures.id = instructors.picture_image_file_id
WHERE
    journeys.uid = ?
    AND journeys.deleted_at IS NULL
    AND darkened_image_files.id = journeys.darkened_background_image_file_id
    AND content_files.id = journeys.audio_content_file_id
    AND instructors.id = journeys.instructor_id
        """,
        (ctx.user_sub, ctx.user_sub, best_journey.journey_uid),
    )
    if not response.results:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier=f"select_class_{'' if pro else 'non_'}pro:journey_not_found",
            category="internal",
        )
        raise ValueError(f"Journey {best_journey.journey_uid} not found")

    row = response.results[0]
    access = (
        "free"
        if not pro
        else ("paid-requires-upgrade" if not has_pro else "paid-unlocked")
    )

    transcript = await get_transcript(itgs, best_journey.transcript_uid)
    if transcript is None:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **TECHNIQUE_PARAMETERS,
            prompt_identifier=f"select_class_{'' if pro else 'non_'}pro:transcript_not_found",
            category="internal",
        )
        raise ValueError(
            f"Transcript for journey {best_journey.journey_uid} not found after selection"
        )

    return (
        JournalEntryItemTextualPartJourneyClientDetails(
            uid=row[0],
            title=row[1],
            description=row[2],
            darkened_background=ImageFileRef(
                uid=row[3],
                jwt=await lib.image_files.auth.create_jwt(
                    itgs,
                    image_file_uid=row[3],
                ),
            ),
            duration_seconds=row[4],
            instructor=MinimalJourneyInstructor(
                name=row[5],
                image=(
                    None
                    if row[6] is None
                    else ImageFileRef(
                        uid=row[6],
                        jwt=await lib.image_files.auth.create_jwt(
                            itgs,
                            image_file_uid=row[6],
                        ),
                    )
                ),
            ),
            last_taken_at=row[7],
            liked_at=row[8],
            access=access,
        ),
        transcript.to_internal(),
    )
