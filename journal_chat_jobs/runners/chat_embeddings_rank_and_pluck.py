"""
Filters journeys using cosine similarity between the user message and the journey,
then a few head-to-head comparisons with gpt-4o-mini
"""

import asyncio
from dataclasses import dataclass
import json
import random
from typing import (
    Awaitable,
    Dict,
    List,
    Optional,
    Tuple,
    cast,
)
import numpy as np

from error_middleware import handle_warning
from itgs import Itgs
from lib.journals.conversation_stream import JournalChatJobConversationStream
from journal_chat_jobs.lib.data_to_client import data_to_client
from journal_chat_jobs.lib.fast_top_1 import fast_top_1
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from journal_chat_jobs.lib.journey_embeddings import get_journey_embeddings
from journal_chat_jobs.lib.openai_ratelimits import reserve_openai_tokens
from lib.image_files.image_file_ref import ImageFileRef
from lib.journals.journal_chat import JournalChat
from lib.journals.journal_chat_redis_packet import (
    SegmentDataMutation,
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
import openai
import openai.types
import openai.types.chat
import os
import lib.users.entitlements
import lib.image_files.auth
import journal_chat_jobs.lib.chat_helper as chat_helper


LARGE_MODEL = "gpt-4o"
SMALL_MODEL = "gpt-4o-mini"
EXPECTED_EMBEDDING_MODEL = "text-embedding-3-large"
"""We can speed up creating the embeddings (starting it before retrieving the journey
embeddings) by assuming what model it uses
"""

BIG_RATELIMIT_CATEGORY = "gpt-4o"
SMALL_RATELIMIT_CATEGORY = "gpt-4o-mini"
CONCURRENCY = 10


async def handle_chat(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """Uses cosine similarity of the user message as a first pass at filtering
    journeys, then uses gpt-4o-mini for some head-to-head comparisons
    """
    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Running prechecks...")
    conversation_stream = JournalChatJobConversationStream(
        journal_entry_uid=ctx.journal_entry_uid, user_sub=ctx.user_sub
    )
    await conversation_stream.start()
    greeting_result = await conversation_stream.load_next_item(timeout=5)
    if greeting_result.type != "item":
        await conversation_stream.cancel()
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:failed_to_load_greeting:{greeting_result.type}",
            stat_id=f"greeting:{greeting_result.type}",
            warning_message=f"Failed to retrieve the greeting for `{ctx.user_sub}`",
            client_message="Failed to retrieve greeting",
            client_detail=greeting_result.type,
        )
        return
    user_message_result = await conversation_stream.load_next_item(timeout=5)
    if user_message_result.type != "item":
        await conversation_stream.cancel()
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:failed_to_load_user_message:{user_message_result.type}",
            stat_id=f"user_message:{user_message_result.type}",
            warning_message=f"Failed to retrieve the user message for `{ctx.user_sub}`",
            client_message="Failed to retrieve user message",
            client_detail=user_message_result.type,
        )
        return

    blank_result = await conversation_stream.load_next_item(timeout=5)
    if blank_result.type != "finished":
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:unexpected_state:{blank_result.type}",
            stat_id=f"unexpected_state:{blank_result.type}",
            warning_message=f"Unexpected state for `{ctx.user_sub}`; expected to get finished, got {blank_result.type}",
            client_message="Unexpected state",
            client_detail=blank_result.type,
        )
        await conversation_stream.cancel()
        return

    greeting = greeting_result.item.data
    user_message = user_message_result.item.data

    chat_state = JournalChat(
        uid=ctx.journal_chat_uid,
        integrity="",
        data=(
            [
                await data_to_client(itgs, ctx=ctx, item=greeting),
                await data_to_client(itgs, ctx=ctx, item=user_message),
            ]
            if ctx.task.include_previous_history
            else []
        ),
    )
    if chat_state.data:
        chat_state.integrity = chat_state.compute_integrity()
        await chat_helper.publish_mutations(
            itgs,
            ctx=ctx,
            final=False,
            mutations=[SegmentDataMutation(key=[], value=chat_state)],
        )
        await chat_helper.publish_spinner(itgs, ctx=ctx, message="Running prechecks...")

    text_greeting = chat_helper.extract_as_text(greeting)
    text_user_message = chat_helper.extract_as_text(user_message)

    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    moderation_response_task = asyncio.create_task(
        asyncio.to_thread(client.moderations.create, input=text_user_message)
    )
    pro_entitlement = await lib.users.entitlements.get_entitlement(
        itgs, user_sub=ctx.user_sub, identifier="pro"
    )
    has_pro = pro_entitlement is not None and pro_entitlement.is_active
    chat_response_task = None
    user_message_embedding_task = None
    if has_pro:
        chat_response_task = asyncio.create_task(
            _get_empathy_response(
                itgs,
                ctx=ctx,
                text_greeting=text_greeting,
                text_user_message=text_user_message,
                client=client,
            )
        )
        user_message_embedding_task = asyncio.create_task(
            _get_user_message_embedding(
                itgs,
                ctx=ctx,
                text_greeting=text_greeting,
                text_user_message=text_user_message,
                client=client,
            )
        )

    try:
        moderation_response = await moderation_response_task
    except Exception as e:
        if chat_response_task is not None:
            if chat_response_task.cancel():
                try:
                    await chat_response_task
                except BaseException:
                    ...
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:moderation_error",
            stat_id="moderation_error",
            warning_message=f"Failed to connect with LLM for moderation for `{ctx.user_sub}`",
            client_message="Failed to connect with LLM",
            client_detail="moderation error",
            exc=e,
        )
        return

    if moderation_response.results[0].categories.self_harm_intent:
        if chat_response_task is not None:
            if chat_response_task.cancel():
                try:
                    await chat_response_task
                except BaseException:
                    ...

        data = chat_helper.get_message_from_paragraphs(
            [
                "In a crisis?",
                "Text HELLO to 741741 to connect with a volunteer Crisis Counselor",
                "Free 24/7 support at your fingerprints.",
                "*741741 is a registered trademark of Crisis Text Line, Inc.",
            ]
        )
        if (
            await chat_helper.write_journal_entry_item_closing_out_on_failure(
                itgs,
                ctx=ctx,
                message=data[0],
                replace_journal_entry_item_uid=ctx.task.replace_entry_item_uid,
            )
            is None
        ):
            return

        chat_state.data.append(data[1])
        chat_state.integrity = chat_state.compute_integrity()
        await chat_helper.publish_mutations(
            itgs,
            ctx=ctx,
            final=True,
            mutations=[
                SegmentDataMutation(key=["integrity"], value=chat_state.integrity),
                SegmentDataMutation(
                    key=["data", len(chat_state.data) - 1], value=data[1]
                ),
            ],
        )
        ctx.stats.incr_completed(
            requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz, type=ctx.type
        )
        return

    if moderation_response.results[0].flagged:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:flagged",
            stat_id="flagged",
            warning_message=f"Failed to create response for `{ctx.user_sub}` as their message was flagged",
            client_message="Failed to create response",
            client_detail="flagged",
        )
        return

    if chat_response_task is None:
        await chat_helper.publish_spinner(
            itgs, ctx=ctx, message="Writing initial response..."
        )
        chat_response_task = asyncio.create_task(
            _get_empathy_response(
                itgs,
                ctx=ctx,
                text_greeting=text_greeting,
                text_user_message=text_user_message,
                client=client,
            )
        )

    if user_message_embedding_task is None:
        user_message_embedding_task = asyncio.create_task(
            _get_user_message_embedding(
                itgs,
                ctx=ctx,
                text_greeting=text_greeting,
                text_user_message=text_user_message,
                client=client,
            )
        )

    try:
        chat_response = await chat_response_task
    except Exception as e:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:empathy_paragraph",
            stat_id=f"llm:empathy:{type(e).__name__}",
            warning_message=f"Failed to connect with LLM for empathy paragraph for `{ctx.user_sub}`",
            client_message="Failed to connect with LLM",
            client_detail="empathy error",
            exc=e,
        )
        return

    empathy_message = chat_response.choices[0].message
    if empathy_message.content is None:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:empathy_no_content",
            stat_id="empathy_no_content",
            warning_message=f"Failed to create response for `{ctx.user_sub}`",
            client_message="Failed to create response",
            client_detail="no content",
        )
        return

    data = chat_helper.get_message_from_text(empathy_message.content)
    chat_state.data.append(data[1])
    chat_state.integrity = chat_state.compute_integrity()
    await chat_helper.publish_mutations(
        itgs,
        ctx=ctx,
        final=False,
        mutations=[
            SegmentDataMutation(key=["integrity"], value=chat_state.integrity),
            SegmentDataMutation(key=["data", len(chat_state.data) - 1], value=data[1]),
        ],
    )
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
            pro=False,
            has_pro=has_pro,
            report_progress=allowing_free_progress_messages,
            user_message_embedding_task=user_message_embedding_task,
        )
    )
    pro_class_selection_task = asyncio.create_task(
        select_class(
            itgs,
            ctx=ctx,
            user_message=user_message,
            pro=True,
            has_pro=has_pro,
            report_progress=allowing_pro_progress_messages,
            user_message_embedding_task=user_message_embedding_task,
        )
    )

    try:
        free_class, free_class_transcript = await free_class_selection_task
    except Exception as e:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:free_class_selection",
            stat_id=f"free_class_selection:{type(e).__name__}",
            warning_message=f"Failed to select free journey for `{ctx.user_sub}`",
            client_message="Failed to select free journey",
            client_detail=None,
            exc=e,
        )
        return

    use_emojis = random.choice(
        [
            "Use emojis if appropriate for visual variety",
            "Do not use emojis under any circumstance",
        ]
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
                    "content": "You should always suggest 1-2 sentences to complete their response. Do not attempt to rewrite their existing response, and do not repeat content from their existing response.",
                },
                {
                    "role": "user",
                    "content": (
                        f"""
Finish my response to a user who responded to today's survey with

Q:
```txt
{text_greeting}
```

A: 
```txt
{user_message}
```

I want to recommend them {free_class.title} by {free_class.instructor.name}. Here's
the transcript of that class

```txt
{str(free_class_transcript)}
```

Here's a general description:

{free_class.description}

I need you to add 2 sentences to my response so far. Do not include my
response in your message. There has been some time since I wrote my initial
message, so start this one with a proper sentence (starting with an uppercase
letter), but do make sure my response flows nicely (i.e., don't greet them again).
{use_emojis}

Suggest specific ways to get the most out of the class. Make sure its clear how
it relates to their original message. Do not use italics or bold. Restrict your
response to no more than 2 sentences and no more than 200 characters.

---

{empathy_message.content}
                    """
                    ),
                },
            ],
            model=LARGE_MODEL,
            max_tokens=2048,
        )
    except Exception as e:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:free_class:llm",
            stat_id=f"free_class:llm:{type(e).__name__}",
            warning_message=f"Failed to connect with LLM for writing the free class suggestion for `{ctx.user_sub}`",
            client_message="Failed to connect with LLM",
            client_detail="free class suggestion error",
            exc=e,
        )
        return

    free_class_message = chat_response.choices[0].message
    if free_class_message.content is None:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:free_class_no_content",
            stat_id="free_class_no_content",
            warning_message=f"Failed to create response for `{ctx.user_sub}`",
            client_message="Failed to create response",
            client_detail="no content",
        )
        return

    paragraphs1 = chat_helper.break_paragraphs(empathy_message.content)
    paragraphs2 = chat_helper.break_paragraphs(free_class_message.content)
    paragraphs = paragraphs1 + paragraphs2
    data = JournalEntryItemDataClient(
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
    chat_state.data[-1] = data
    chat_state.integrity = chat_state.compute_integrity()
    await chat_helper.publish_mutations(
        itgs,
        ctx=ctx,
        final=False,
        mutations=[
            SegmentDataMutation(key=["integrity"], value=chat_state.integrity),
            SegmentDataMutation(key=["data", len(chat_state.data) - 1], value=data),
        ],
    )

    await chat_helper.publish_spinner(
        itgs, ctx=ctx, message="Searching for pro journey..."
    )
    allowing_pro_progress_messages.set()
    try:
        pro_class, pro_class_transcript = await pro_class_selection_task
    except Exception as e:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:pro_class_selection",
            stat_id=f"pro_class_selection:{type(e).__name__}",
            warning_message=f"Failed to select pro journey for `{ctx.user_sub}`",
            client_message="Failed to select pro journey",
            client_detail=None,
            exc=e,
        )
        return

    connective_option = random.choice(
        [
            "Alternatively",
            "Another option would be to",
            "You could instead",
            "Another approach is to",
            "You might instead",
            "If you want a longer class, consider",
            "For a more in-depth experience, try",
            [
                (
                    "Since you have Oseh+, you can instead take"
                    if has_pro
                    else "You would need to get Oseh+, but another great class is"
                )
            ],
        ]
    )
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
Finish my response to a user who responded to today's survey with

Q:
```txt
{text_greeting}
```

A: 
```txt
{user_message}
```

I am going to recommend them {pro_class.title} by {pro_class.instructor.name}. Here's
the transcript of that class

```txt
{str(pro_class_transcript)}
```

Here's a general description:

{pro_class.description}

I need you to add 2 sentences to my response so far. Do not include my
response in your message. There has been some time since I wrote my initial
message, so start this one with a proper sentence (starting with an uppercase
letter). Use emojis if appropriate for visual variety.

Focus exclusively on the class above. Make sure the response does not appear
repetitive when compared with what I said about {free_class.title}. Use a
connective word ({connective_option}) to tie your message in. Often,
instructors will ask the user to do something (breath in a certain way,
visualize something, etc), and the response should reiterate that (e.g., "doing
<thing> is worth it") or a heads up about that thing (e.g., "she will ask you to
repeat after her"), or something else along those lines.

Make sure its clear how it relates to their original message. Do not use italics
or bold. Restrict your response to no more than 3 sentences and no more than 350
characters. After your response, I will link to {pro_class.title}, so you do not
need to.

---

{empathy_message.content}

{free_class_message.content}

[{free_class.title}](journey:{chat_helper.make_id(free_class.title)})
                    """
                    ),
                },
            ],
            model=LARGE_MODEL,
            max_tokens=4096,
        )
    except Exception as e:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:pro_class:llm",
            stat_id=f"pro_class:llm:{type(e).__name__}",
            warning_message=f"Failed to connect with LLM for writing the pro class suggestion for `{ctx.user_sub}`",
            client_message="Failed to connect with LLM",
            client_detail="pro class suggestion error",
            exc=e,
        )
        return

    pro_class_message = chat_response.choices[0].message
    if pro_class_message.content is None:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:pro_class_no_content",
            stat_id="pro_class_no_content",
            warning_message=f"Failed to create response for `{ctx.user_sub}`",
            client_message="Failed to create response",
            client_detail="no content",
        )
        return

    paragraphs3 = chat_helper.break_paragraphs(pro_class_message.content)
    paragraphs = paragraphs1 + paragraphs2
    data = (
        JournalEntryItemData(
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
        ),
        JournalEntryItemDataClient(
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
        ),
    )
    if (
        await chat_helper.write_journal_entry_item_closing_out_on_failure(
            itgs,
            ctx=ctx,
            message=data[0],
            replace_journal_entry_item_uid=ctx.task.replace_entry_item_uid,
        )
        is None
    ):
        return

    chat_state.data[-1] = data[1]
    chat_state.integrity = chat_state.compute_integrity()
    await chat_helper.publish_mutations(
        itgs,
        ctx=ctx,
        final=True,
        mutations=[
            SegmentDataMutation(key=["integrity"], value=chat_state.integrity),
            SegmentDataMutation(key=["data", len(chat_state.data) - 1], value=data[1]),
        ],
    )
    ctx.stats.incr_completed(
        requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz, type=ctx.type
    )


async def _get_empathy_response(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    text_greeting: str,
    text_user_message: str,
    client: openai.OpenAI,
):
    await chat_helper.reserve_tokens(
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=2048
    )
    return await asyncio.to_thread(
        client.chat.completions.create,
        messages=[
            {
                "role": "system",
                "content": f"""
Adopt the role of a support group leader which asks one question to prompt a
response from the group, then responds to individual members. The response is
sent via direct message, so each user feels like it was directed personally at
them. Many members will give updates on how they are feeling or what they are
going through. You never apologize or express regret for how someone is feeling,
because you know that it is a normal part of life to go through highs or lows.
Instead, you are grateful that they are willing to be vulnerable with you.

You keep your responses to 2-3 sentences in a single paragraph, as that is all
the space available in the comment section. You never ask follow-up questions.
Since this is social media, do not include a greeting or a sign-off. There is
not much space, so do not repeat yourself.

Today, you asked:

{text_greeting}
                        """.strip(),
            },
            {"role": "user", "content": text_user_message},
        ],
        model=LARGE_MODEL,
        max_tokens=2048,
    )


async def _get_user_message_embedding(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    text_greeting: str,
    text_user_message: str,
    client: openai.OpenAI,
) -> openai.types.CreateEmbeddingResponse:
    """
    Gets the embedding for the users message. Asks gpt-4o to convert the users
    message into a high-value search string, then uses the
    text-embedding-3-large model to convert that into an embedding
    """
    await chat_helper.reserve_tokens(
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=2048
    )
    setup_messages: List[openai.types.chat.ChatCompletionMessageParam] = [
        {
            "role": "system",
            "content": (
                "You are an AI search assistant. Follow the user's requirements carefully and to the letter. "
                "First, think step-by-step and describe your plan for what to look for, written out in great "
                "detail. Then, output the correct search query in the function provided. Minimize any other prose.\n\n"
                "You know that the best search queries are just phrases of related ideas, for example "
                "'anxiety, stress management, calm down' would be a good search query to find classes "
                "that help reduce anxiety. You might also point towards specific approaches, for example, "
                "'poetic, self-expression, story-telling, inspirational'."
            ),
        },
        {
            "role": "user",
            "content": (
                "I am interested in taking a class. I have a library of classes available to me that is too large to "
                "browse through. I have access to a search engine that can find classes based on any text I provide, "
                "by comparing the text to the content of the classes. This works best if the search text is semantically "
                "similar to the content I want to receive, rather than writing questions that are answered by the "
                "content. Can you write search query for me? Here were my responses to todays prompt:\n\n"
                f"Q: {text_greeting}\n\nA: {text_user_message}"
            ),
        },
    ]
    chat_response = await asyncio.to_thread(
        client.chat.completions.create,
        messages=setup_messages,
        model=LARGE_MODEL,
        max_tokens=2048,
    )

    search_query = text_user_message
    if chat_response.choices and chat_response.choices[0].message.content is not None:
        chat_message_thinking_pass = chat_response.choices[0].message.content

        await chat_helper.reserve_tokens(
            itgs, ctx=ctx, category=SMALL_RATELIMIT_CATEGORY, tokens=2048
        )
        chat_response = await asyncio.to_thread(
            client.chat.completions.create,
            messages=[
                *setup_messages,
                {"role": "assistant", "content": chat_message_thinking_pass},
            ],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "search",
                        "strict": True,
                        "description": "Searches for a class using the given search query.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The search query to use",
                                }
                            },
                            "required": ["query"],
                        },
                    },
                }
            ],
            tool_choice={"type": "function", "function": {"name": "search"}},
            model=SMALL_MODEL,
            max_tokens=2048,
        )

        chat_message = chat_response.choices[0].message
        if (
            chat_message.tool_calls
            and chat_message.tool_calls[0].function.name == "search"
        ):
            search_args_json = chat_message.tool_calls[0].function.arguments
            try:
                search_args = json.loads(search_args_json)
                search_query = f"{search_args['query']}"
            except Exception as e:
                await handle_warning(
                    f"{__name__}:embeddings",
                    f"Failed to parse search query from chat response",
                    exc=e,
                )

    return await asyncio.to_thread(
        client.embeddings.create,
        input=search_query,
        model=EXPECTED_EMBEDDING_MODEL,
        encoding_format="float",
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
    pro: bool,
    has_pro: bool,
    report_progress: asyncio.Event,
    user_message_embedding_task: Awaitable[openai.types.CreateEmbeddingResponse],
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
GROUP BY uj.journey_id
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
        raise ValueError(f"User {ctx.user_sub} not found")

    if not responses[1].results:
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

    await ctx.maybe_check_redis(itgs)
    embeddings = await get_journey_embeddings(itgs)
    if embeddings.type != "success":
        raise ValueError(f"Failed to get journey embeddings: {embeddings.type}")
    assert (
        embeddings.model == EXPECTED_EMBEDDING_MODEL
    ), f"{embeddings.model=} != {EXPECTED_EMBEDDING_MODEL=}"

    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    try:
        user_message_embedding = await user_message_embedding_task
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

        await ctx.maybe_check_redis(itgs)
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
Step 1a: Repeat the id for {a.journey_title}
Step 1b: Write a paragraph on the strongest link between the first journey and
the users message.

Step 2a: Repeat the id for {b.journey_title}
Step 2b: Write a paragraph on the strongest link between the second journey and
the users message.
      
Step 3: Select the journey for which your argument is more convincing. Prefer arguments
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
                        "strict": True,
                        "description": "Select the given journey as the preferred journey",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "step_1a_id": {
                                    "type": "string",
                                    "description": "The ID of the first journey",
                                    "enum": [a_id],
                                },
                                "step_1b_argument": {
                                    "type": "string",
                                    "description": "The argument for the first journey",
                                },
                                "step_2a_id": {
                                    "type": "string",
                                    "description": "The ID of the second journey",
                                    "enum": [b_id],
                                },
                                "step_2b_argument": {
                                    "type": "string",
                                    "description": "The argument for the second journey",
                                },
                                "step_3_id": {
                                    "type": "string",
                                    "description": "The ID of the journey which is preferred amongst the two",
                                    "enum": [a_id, b_id],
                                },
                            },
                            "required": [
                                "step_1a_id",
                                "step_1b_argument",
                                "step_2a_id",
                                "step_2b_argument",
                                "step_3_id",
                            ],
                            "additionalProperties": False,
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

        selected_id = comparison_arguments.get("step_3_id")
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
        raise ValueError(f"Journey {best_journey.journey_uid} not found")

    row = response.results[0]
    access = (
        "free"
        if not pro
        else ("paid-requires-upgrade" if not has_pro else "paid-unlocked")
    )

    transcript = await get_transcript(itgs, best_journey.transcript_uid)
    if transcript is None:
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
