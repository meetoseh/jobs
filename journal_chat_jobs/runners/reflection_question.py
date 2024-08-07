import asyncio
import json
import os
from typing import Literal, Optional, cast
import openai
from error_middleware import handle_warning
from itgs import Itgs
from lib.journals.conversation_stream import JournalChatJobConversationStream
from journal_chat_jobs.lib.data_to_client import (
    data_to_client,
    get_journal_chat_job_journey_metadata,
)
from journal_chat_jobs.lib.journal_chat_job_context import (
    JournalChatJobContext,
    JourneyMemoryCachedData,
)
import journal_chat_jobs.lib.chat_helper as chat_helper
from lib.journals.journal_chat import JournalChat
from lib.journals.journal_chat_redis_packet import SegmentDataMutation
from lib.transcripts.cache import get_transcript
from lib.transcripts.model import Transcript


LARGE_MODEL = "gpt-4o"
SMALL_MODEL = "gpt-4o-mini"

BIG_RATELIMIT_CATEGORY = "gpt-4o"
SMALL_RATELIMIT_CATEGORY = "gpt-4o-mini"


async def handle_reflection(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """Produces a reflection question by asking the large model to brainstorm some options
    and select one
    """
    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Decrypting history...")
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

    system_message_result = await conversation_stream.load_next_item(timeout=5)
    if system_message_result.type != "item":
        await conversation_stream.cancel()
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:failed_to_load_system_message:{system_message_result.type}",
            stat_id=f"system_message:{system_message_result.type}",
            warning_message=f"Failed to retrieve the system message for `{ctx.user_sub}`",
            client_message="Failed to retrieve system message",
            client_detail=system_message_result.type,
        )
        return

    while True:
        ui_entry_result = await conversation_stream.load_next_item(timeout=5)
        if ui_entry_result.type != "item":
            await chat_helper.publish_error_and_close_out(
                itgs,
                ctx=ctx,
                warning_id=f"{__name__}:failed_to_load_ui_entry:{ui_entry_result.type}",
                stat_id=f"ui_entry:{ui_entry_result.type}",
                warning_message=f"Failed to retrieve the user journey ui entry for `{ctx.user_sub}`",
                client_message="Failed to retrieve user journey ui entry",
                client_detail=ui_entry_result.type,
            )
            await conversation_stream.cancel()
            return

        if (
            ui_entry_result.item.data.data.type == "ui"
            and ui_entry_result.item.data.data.conceptually.type == "user_journey"
        ):
            ui_entry = ui_entry_result.item.data
            break

    # they may have taken additional classes since then; we will not use them
    # for generating the reflection question
    await conversation_stream.cancel()

    greeting = greeting_result.item.data
    user_message = user_message_result.item.data
    system_message = system_message_result.item.data

    if system_message.data.type != "textual":
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:invalid_system_message_type",
            stat_id="system_message:invalid_type",
            warning_message=f"Invalid system message type for `{ctx.user_sub}`",
            client_message="Invalid system message type",
            client_detail="system message must be textual",
        )
        return

    chat_state = JournalChat(
        uid=ctx.journal_chat_uid,
        integrity="",
        data=(
            [
                await data_to_client(itgs, ctx=ctx, item=greeting),
                await data_to_client(itgs, ctx=ctx, item=user_message),
                await data_to_client(itgs, ctx=ctx, item=system_message),
                await data_to_client(itgs, ctx=ctx, item=ui_entry),
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
    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Brainstorming options...")

    text_greeting = chat_helper.extract_as_text(greeting)
    text_user_message = chat_helper.extract_as_text(user_message)
    text_system_message = await chat_helper.convert_textual_to_markdown(
        itgs, ctx=ctx, item=system_message.data, convert_links=True
    )
    journey_uid = chat_helper.extract_as_journey_uid(ui_entry)

    journey_metadata, journey_transcript = await asyncio.gather(
        get_journal_chat_job_journey_metadata(itgs, ctx=ctx, journey_uid=journey_uid),
        _get_journey_transcript(
            itgs,
            ctx=ctx,
            journey_uid=journey_uid,
        ),
    )
    if journey_metadata is None or journey_transcript is None:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:failed_to_retrieve_journey_metadata",
            stat_id="journey_metadata:missing",
            warning_message=f"Failed to retrieve journey metadata for `{journey_uid}`",
            client_message="Failed to retrieve journey metadata",
            client_detail="the journey may have been deleted",
        )
        return

    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    try:
        option = await _get_option_response(
            itgs,
            ctx=ctx,
            text_greeting=text_greeting,
            text_user_message=text_user_message,
            text_system_message=text_system_message,
            journey_metadata=journey_metadata,
            journey_transcript=journey_transcript,
            client=client,
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

    data = chat_helper.get_message_from_text(option, type="reflection-question")
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
            SegmentDataMutation(key=["data", len(chat_state.data) - 1], value=data[1]),
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


async def _get_option_response(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    text_greeting: str,
    text_user_message: str,
    text_system_message: str,
    journey_metadata: JourneyMemoryCachedData,
    journey_transcript: Transcript,
    client: openai.OpenAI,
) -> Optional[str]:
    await chat_helper.reserve_tokens(
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=8192
    )
    chat_response = await asyncio.to_thread(
        client.chat.completions.create,
        messages=[
            {"role": "assistant", "content": text_greeting},
            {"role": "user", "content": text_user_message},
            {"role": "assistant", "content": text_system_message},
            {"role": "user", "content": f"Okay, I took {journey_metadata.title}"},
            {
                "role": "system",
                "content": f"""
Here's what you know about {journey_metadata.title}:

TITLE: {journey_metadata.title}
DESCRIPTION: {journey_metadata.description}
INSTRUCTOR: {journey_metadata.instructor.name}
TRANSCRIPT:
```vtt
{str(journey_transcript)}
```

---

Adopt the role of a life coach. You ask thoughtful questions that help people:

1. consider what they just did
2. reflect on their lives
3. imagine how they would act if they felt differently
4. think about what they want to do next
5. understand themselves better
6. work through their emotions

You are very mindful that it is unhelpful to stew on negative emotions; you want
people to move forward in a positive way. For that reason, while you may
acknowledge that they feel anxious, you would never suggest they dwell on it.
Instead, you might ask them to imagine being less anxious, or to write about
when they didn't feel so anxious, or to write about which things are going on
that are reducing their anxiety, etc, etc.

BAD QUESTION: "What are you anxious about?"
BETTER QUESTION: "When was the last time you felt calm and in control?"

Step 1: Talk about what makes a good question in this context.
Step 2: Brainstorm 3 questions that you could offer the user.
Step 3: Discuss the pros and cons of each question in order.
Step 4: Write a new set of 3 questions based on the discussion. You may reuse 
  questions from the brainstorming session if they were good.
Step 5: Compare and contrast the benefits and downsides of the new questions.
Step 6: Determine the question that you would like to ask the user to reflect on.
                        """.strip(),
            },
        ],
        model=LARGE_MODEL,
        max_tokens=8192,
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "store_question_selection",
                    "description": "Stores the reasoning and selection for the reflection question to present to the user",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "step_1": {
                                "type": "string",
                                "description": "What makes a good question in this context.",
                            },
                            "step_2a": {
                                "type": "string",
                                "description": "The first brainstorming question",
                            },
                            "step_2b": {
                                "type": "string",
                                "description": "The second brainstorming question",
                            },
                            "step_2c": {
                                "type": "string",
                                "description": "The third brainstorming question",
                            },
                            "step_3a": {
                                "type": "string",
                                "description": "The pros and cons of the first brainstorming question",
                            },
                            "step_3b": {
                                "type": "string",
                                "description": "The pros and cons of the second brainstorming question",
                            },
                            "step_3c": {
                                "type": "string",
                                "description": "The pros and cons of the third brainstorming question",
                            },
                            "step_4a": {
                                "type": "string",
                                "description": "The first revised question",
                            },
                            "step_4b": {
                                "type": "string",
                                "description": "The second revised question",
                            },
                            "step_4c": {
                                "type": "string",
                                "description": "The third revised question",
                            },
                            "step_5": {
                                "type": "string",
                                "description": "The benefits and downsides of the new questions, with the purpose of deduucing a winner",
                            },
                            "step_6": {
                                "type": "string",
                                "description": "The question that you would like to ask the user to reflect on",
                            },
                        },
                        "required": [
                            "step_1",
                            "step_2a",
                            "step_2b",
                            "step_2c",
                            "step_3a",
                            "step_3b",
                            "step_3c",
                            "step_4a",
                            "step_4b",
                            "step_4c",
                            "step_5",
                            "step_6",
                        ],
                    },
                },
            }
        ],
        tool_choice={
            "type": "function",
            "function": {"name": "store_question_selection"},
        },
    )

    chat_message = chat_response.choices[0].message
    if (
        chat_message.tool_calls
        and chat_message.tool_calls[0].function.name == "store_question_selection"
    ):
        args_json = chat_message.tool_calls[0].function.arguments
        try:
            args = json.loads(args_json)
            result = args["step_6"]
            if isinstance(result, str):
                return result
        except Exception as e:
            await handle_warning(
                f"{__name__}:options",
                f"Failed to parse reflection question from chat response",
                exc=e,
            )
    return None
