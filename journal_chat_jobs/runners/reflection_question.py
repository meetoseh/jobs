import asyncio
import json
import os
from typing import List, Literal, Optional, cast
import openai
from error_middleware import handle_warning
from itgs import Itgs
from lib.journals.conversation_stream import (
    JournalChatJobConversationStream,
    JournalEntryItem,
)
from journal_chat_jobs.lib.data_to_client import (
    DataToClientInspectResult,
    bulk_prepare_data_to_client,
    data_to_client,
    inspect_data_to_client,
)
from journal_chat_jobs.lib.journal_chat_job_context import (
    JournalChatJobContext,
)
import journal_chat_jobs.lib.chat_helper as chat_helper
from lib.journals.journal_chat import JournalChat
from lib.journals.journal_chat_redis_packet import SegmentDataMutation
from lib.journals.journal_entry_item_data import JournalEntryItemDataClient
from lib.transcripts.cache import get_transcript
from lib.transcripts.model import Transcript
from lib.users.time_of_day import get_time_of_day
import unix_dates
import random


LARGE_MODEL = "gpt-4o"
SMALL_MODEL = "gpt-4o-mini"

BIG_RATELIMIT_CATEGORY = "gpt-4o"
SMALL_RATELIMIT_CATEGORY = "gpt-4o-mini"

VARIETY = [
    "what makes people happy",
    "how people connect with each other",
    "how people notice others are happy",
    "how people can be more positive",
    "what people are optimistic about",
    "what people are grateful for",
    "what people are looking forward to",
    "what people are excited about",
    "what things people did recently",
    "what people are proud of",
    "what people are hopeful about",
    "what people are inspired by",
    "what people are passionate about",
    "what people are motivated by",
    "what people see in the clouds",
    "what they dreamt about recently",
    "what they can do to make someone smile",
    "what they did recently",
    "what question they would want to ask their future self",
    "what they would want to tell their past self",
    "what their future self would probably have forgotten about this week",
    "what they thought they would have forgotten about last week that they still remember",
    'why they "should" be happy',
    "why they feel like they do",
    "what they didn't say to themself that they wanted to",
    "what they didn't say to someone else that they wanted to",
    "how they were daring recently",
    "what had them scared recently (if anything), and if they were able to overcome it",
]


async def handle_reflection(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """Produces a reflection question by asking the large model to brainstorm some options
    and select one
    """
    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Decrypting history...")
    conversation_stream = JournalChatJobConversationStream(
        journal_entry_uid=ctx.journal_entry_uid,
        user_sub=ctx.user_sub,
        pending_moderation="resolve",
        ctx=ctx,
    )
    await conversation_stream.start()

    greeting_entry: Optional[JournalEntryItem] = None
    user_message_entry: Optional[JournalEntryItem] = None

    server_items: List[JournalEntryItem] = []
    reflection_question_index: Optional[int] = None

    while True:
        next_item_result = await conversation_stream.load_next_item(timeout=5)
        if next_item_result.type == "finished":
            break

        if next_item_result.type != "item":
            await chat_helper.publish_error_and_close_out(
                itgs,
                ctx=ctx,
                warning_id=f"{__name__}:failed_to_load_ui_entry:{next_item_result.type}",
                stat_id=f"ui_entry:{next_item_result.type}",
                warning_message=f"Failed to retrieve the user journey ui entry for `{ctx.user_sub}`",
                client_message="Failed to retrieve user journey ui entry",
                client_detail=next_item_result.type,
            )
            await conversation_stream.cancel()
            return

        server_items.append(next_item_result.item)

        if (
            ctx.task.replace_entry_item_uid is not None
            and ctx.task.replace_entry_item_uid == next_item_result.item.uid
        ):
            assert reflection_question_index is None
            reflection_question_index = len(server_items) - 1

        if reflection_question_index is None:
            if (
                greeting_entry is None
                and user_message_entry is None
                and next_item_result.item.data.type == "chat"
                and next_item_result.item.data.display_author == "other"
            ):
                greeting_entry = next_item_result.item
                continue

            if (
                user_message_entry is None
                and next_item_result.item.data.type == "chat"
                and next_item_result.item.data.display_author == "self"
            ):
                user_message_entry = next_item_result.item
                continue

    inspect_result = DataToClientInspectResult(
        pro=False, journeys=set(), voice_notes=set()
    )
    for item in server_items:
        inspect_data_to_client(item.data, out=inspect_result)
    await bulk_prepare_data_to_client(itgs, ctx=ctx, inspect=inspect_result)

    client_items: List[JournalEntryItemDataClient] = [
        await data_to_client(itgs, ctx=ctx, item=item.data) for item in server_items
    ]

    chat_state = JournalChat(
        uid=ctx.journal_chat_uid,
        integrity="",
        data=client_items[:reflection_question_index],
    )
    chat_state.integrity = chat_state.compute_integrity()
    await chat_helper.publish_mutations(
        itgs,
        ctx=ctx,
        final=False,
        mutations=[SegmentDataMutation(key=[], value=chat_state)],
    )
    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Brainstorming options...")

    text_greeting = (
        await chat_helper.extract_as_text(itgs, ctx=ctx, item=greeting_entry.data)
        if greeting_entry is not None
        else None
    )
    text_user_message = (
        await chat_helper.extract_as_text(itgs, ctx=ctx, item=user_message_entry.data)
        if user_message_entry is not None
        else None
    )

    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    try:
        if text_greeting is not None and text_user_message is not None:
            option = await _get_option_response_with_context(
                itgs,
                ctx=ctx,
                text_greeting=text_greeting,
                text_user_message=text_user_message,
                client=client,
            )
        else:
            option = await _get_option_response_without_context(
                itgs, ctx=ctx, client=client
            )
        if option is None:
            raise ValueError("No response content")
    except Exception as e:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:select_option",
            stat_id=f"select_option:llm:{type(e).__name__}",
            warning_message=f"Failed to connect with LLM for selecting reflection question for `{ctx.user_sub}`",
            client_message="Failed to connect with LLM",
            client_detail="failed to select reflection question",
            exc=e,
        )
        return

    data = chat_helper.get_message_from_text(
        option, type="reflection-question", processing_block=None
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
    reflection_question_index = len(chat_state.data) - 1

    chat_state.integrity = chat_state.compute_integrity()
    await chat_helper.publish_mutations(
        itgs,
        ctx=ctx,
        final=len(client_items) == len(chat_state.data),
        mutations=[
            SegmentDataMutation(key=["integrity"], value=chat_state.integrity),
            SegmentDataMutation(key=["data", reflection_question_index], value=data[1]),
        ],
    )

    if len(client_items) != len(chat_state.data):
        chat_state.data.extend(client_items[reflection_question_index + 1 :])
        chat_state.integrity = chat_state.compute_integrity()

        await chat_helper.publish_mutations(
            itgs,
            ctx=ctx,
            final=True,
            mutations=[
                SegmentDataMutation(key=["integrity"], value=chat_state.integrity),
                *[
                    SegmentDataMutation(
                        key=["data", i + reflection_question_index + 1], value=item
                    )
                    for i, item in enumerate(
                        client_items[reflection_question_index + 1 :]
                    )
                ],
            ],
        )

    ctx.stats.incr_completed(
        requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz, type=ctx.type
    )
    return


async def _get_journey_transcript(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    journey_uid: str,
    read_consistency: Literal["none", "weak", "strong"] = "none",
) -> Optional[Transcript]:
    conn = await itgs.conn()
    cursor = conn.cursor(read_consistency)
    response = await cursor.execute(
        """
SELECT transcripts.uid 
FROM journeys, content_file_transcripts, transcripts
WHERE
    journeys.uid = ?
    AND journeys.deleted_at IS NULL
    AND journeys.audio_content_file_id = content_file_transcripts.content_file_id
    AND transcripts.id = content_file_transcripts.transcript_id
ORDER BY content_file_transcripts.created_at DESC, content_file_transcripts.uid ASC
        """,
        (journey_uid,),
    )
    if not response.results:
        if read_consistency == "none":
            return await _get_journey_transcript(
                itgs, ctx=ctx, journey_uid=journey_uid, read_consistency="weak"
            )
        return None
    transcript_uid = cast(str, response.results[0][0])
    cached_transcript = await get_transcript(itgs, transcript_uid)
    if cached_transcript is None:
        return None
    return cached_transcript.to_internal()


async def _get_option_response_with_context(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    text_greeting: str,
    text_user_message: str,
    client: openai.OpenAI,
) -> Optional[str]:
    datetime_user = unix_dates.unix_timestamp_to_datetime(ctx.queued_at, tz=ctx.user_tz)
    time_of_day_user = get_time_of_day(ctx.queued_at, tz=ctx.user_tz)
    variety = random.choice(VARIETY)

    await chat_helper.reserve_tokens(
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=4096
    )
    chat_response = await asyncio.to_thread(
        client.chat.completions.create,
        messages=[
            {
                "role": "system",
                "content": f"""Adopt the role of the perpetually positive peer who asks short, abstract, broad, forward-thinking questions.

This {time_of_day_user}, {datetime_user.strftime('%A, %B %d, %Y')}, you are thinking about {variety}.""",
            },
            {"role": "assistant", "content": text_greeting},
            {"role": "user", "content": text_user_message},
        ],
        model=LARGE_MODEL,
        max_tokens=4096,
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "present_question",
                    "description": "Presents the given question to present to the user",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "The question to present",
                            },
                        },
                        "required": ["question"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
            }
        ],
        tool_choice={
            "type": "function",
            "function": {"name": "present_question"},
        },
    )

    chat_message = chat_response.choices[0].message
    if (
        chat_message.tool_calls
        and chat_message.tool_calls[0].function.name == "present_question"
    ):
        args_json = chat_message.tool_calls[0].function.arguments
        try:
            args = json.loads(args_json)
            result = args["question"]
            if isinstance(result, str):
                return result
        except Exception as e:
            await handle_warning(
                f"{__name__}:options",
                f"Failed to parse reflection question from chat response",
                exc=e,
            )
    return None


async def _get_option_response_without_context(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    client: openai.OpenAI,
) -> Optional[str]:
    datetime_user = unix_dates.unix_timestamp_to_datetime(ctx.queued_at, tz=ctx.user_tz)
    time_of_day_user = get_time_of_day(ctx.queued_at, tz=ctx.user_tz)
    variety = random.choice(VARIETY)

    await chat_helper.reserve_tokens(
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=4096
    )
    chat_response = await asyncio.to_thread(
        client.chat.completions.create,
        messages=[
            {
                "role": "system",
                "content": f"""Adopt the role of the perpetually positive peer who asks short, abstract, broad, forward-thinking questions.

This {time_of_day_user}, {datetime_user.strftime('%A, %B %d, %Y')}, you are thinking about {variety}.""",
            },
        ],
        model=LARGE_MODEL,
        max_tokens=4096,
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "present_question",
                    "description": "Presents the given question to present to the user",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "The question to present",
                            },
                        },
                        "required": ["question"],
                        "additionalProperties": False,
                    },
                    "strict": True,
                },
            }
        ],
        tool_choice={
            "type": "function",
            "function": {"name": "present_question"},
        },
    )

    chat_message = chat_response.choices[0].message
    if (
        chat_message.tool_calls
        and chat_message.tool_calls[0].function.name == "present_question"
    ):
        args_json = chat_message.tool_calls[0].function.arguments
        try:
            args = json.loads(args_json)
            result = args["question"]
            if isinstance(result, str):
                return result
        except Exception as e:
            await handle_warning(
                f"{__name__}:options",
                f"Failed to parse reflection question from chat response",
                exc=e,
            )
    return None
