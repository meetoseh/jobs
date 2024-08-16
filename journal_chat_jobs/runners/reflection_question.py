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
        journal_entry_uid=ctx.journal_entry_uid,
        user_sub=ctx.user_sub,
        pending_moderation="resolve",
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

    if greeting_result.item.data.processing_block is not None:
        await conversation_stream.cancel()
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:processing_block",
            stat_id="processing_block",
            warning_message=f"Greeting for `{ctx.user_sub}` is blocked from processing",
            client_message="Greeting blocked",
            client_detail="processing block",
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

    if user_message_result.item.data.processing_block is not None:
        await conversation_stream.cancel()
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:processing_block",
            stat_id="processing_block",
            warning_message=f"User message for `{ctx.user_sub}` is blocked from processing",
            client_message="User message blocked",
            client_detail="processing block",
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

    if system_message_result.item.data.processing_block is not None:
        await conversation_stream.cancel()
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:processing_block",
            stat_id="processing_block",
            warning_message=f"System message for `{ctx.user_sub}` is blocked from processing",
            client_message="System message blocked",
            client_detail="processing block",
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
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=4096
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
                "content": f"""Here's what you know about {journey_metadata.title}:

TITLE: {journey_metadata.title}
DESCRIPTION: {journey_metadata.description}
INSTRUCTOR: {journey_metadata.instructor.name}
TRANSCRIPT:
```vtt
{str(journey_transcript)}
```

---

Adopt the role of a life coach. You ask simple questions that help people:

1. reflect on their lives
2. discuss something on their mind
3. think about the future
4. understand themselves better

You are very mindful that it is unhelpful to stew on negative emotions; you want
people to move forward in a positive way. You should also try to relate the question
either to the greeting you gave, the users response, or the class they took.

Suppose they took a class related to reducing anxiety. Then:


BAD QUESTION: "What are you anxious about?"
REASON: 
- Pro: Relates to the class (anxious -> anxiety)
- Con: Asks the respondant to focus on their anxiety, which can make them more anxious. 
- Con: Very concrete

BETTER QUESTION: "What does feeling calm mean to you?"
REASON: 
- Pro: Relates to the class (calm is the opposite of anxious)
- Pro: Asks the respondant to focus on a positive emotion
- Pro: Highly abstract

BAD QUESTION: If you were an elephant, what would make you happy?
- Pro: Focuses on a positive emotion
- Pro: Very creative and open-ended.
- Pro: Highly abstract
- Con: Does not relate to the class (happiness is not the opposite of anxiety)

Your questions should be abstract and open-ended. Your questions should not
time-bound and do not require commitment. They very likely have something they
already want to discuss, and the question should just be a fun way to invite
them to do so.

Suppose they took a class describing their inner child and how it helps them. Then:

BAD QUESTION: "What is one action you can take today to nurture your inner child?"
REASON:
- Pro: Relates to the class (inner child)
- Con: Asks the respondant to commit to a specific action, which is close-ended.
- Con: Asks the respondant only to discuss today, which is close-ended.
- Con: Very concrete

BAD QUESTION: "How do you feel right now, after completing Inner Child?"
REASON:
- Pro: Relates to the class (inner child)
- Con: Asks the respondant to only discuss their current feelings, which is close-ended.
- Con: Very concrete

BAD QUESTION: "How have you nurtured your inner child?"
- Pro: Relates to the class (inner child)
- Pro: They can talk about any time in the past, which is open-ended.
- Con: They cannot reasonably answer this question by discussing an upcoming event, which is close-ended.

BAD QUESTION: "How do you currently feel about your inner child?"
REASON:
- Pro: Relates to the class (inner child)
- Con: Asks the respondant to only discuss their current feelings, which is close-ended.

GOOD QUESTION: "How is your inner child?"
REASON:
- Pro: Relates to the class (inner child)
- Pro: Highly abstract
- Pro: They can talk about an action or a feeling, which is open-ended
- Pro: They can relate it to what they've done or what they plan to do, which is open-ended
- Pro: Although it superficially asks about their current feelings, in colloquial
  English this question could be interpreted as asking about their inner child in general.

BAD QUESTION: "In what ways do you embrace your inner child?"
REASON:
- Pro: Relates to the class (inner child)
- Pro: They can talk about today, yesterday, the future, etc, which is open-ended.
- Pro: There are multiple ways to interpret this question
- Con: They may feel pressured to come up with a specific instance.
  We can make this question more open-ended by switching to "In what ways can you embrace your inner child?"
  Which they can still choose to answer with a specific instance, but does not pressure them to.

Often if the class is highly abstract, such as a sound bath, it can make more
sense to focus on the original exchange that led to the class. For example, a
question like "Now that some time has passed, would you change your original
answer to the question '(repeat the original question)'?"

Step 1: Talk about what makes a good question in this context.
Step 2: Brainstorm 3 questions that you could offer the respondant.
Step 3: Discuss the pros and cons of each question in order.
Step 4: Write a new set of 3 questions based on the discussion. You may reuse 
  questions from the brainstorming session if they were good.
Step 5: Compare and contrast the benefits and downsides of the new questions.
Step 6: Write the first pass at the final question based on the discussion in step 5.
Step 7: Now that we have a good question, remind ourselves what the point of the final revision pass
  is for.
Step 8: Discuss one small change that could be made to the question. Do not actually make the
  change yet. List out the pros and cons of that change.
Step 9: Determine if that change is worth it. 
Step 10: If the change is worth it, incorporate it into the question. If not,
  keep the question the same.""",
            },
        ],
        model=LARGE_MODEL,
        max_tokens=4096,
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
                                "description": "The first pass at the final question based on the discussion in step 5",
                            },
                            # inject tokens closer to the output.
                            "step_7": {
                                "type": "string",
                                "description": "Remind ourselves what the point of the final revision pass is for",
                                "enum": [
                                    "We have now decided, at a strategic level, on a good question. We will now look "
                                    "to see if there is one obvious small change that would make this question more "
                                    "open-ended. Typically, we look to convert a question that only asks about what has "
                                    "happened into one that invites them to respond EITHER with what has happened OR "
                                    "what they want to happen. Sometimes, we can do this with a single word or phrase, "
                                    "or adding a parenthetical. In step 8, we will try to brainstorm such a change and discuss "
                                    "its pros, cons, and if it is redundant. Then, in step 9, we will decide if we actually  "
                                    "want to keep the change (true) or should keep what we have in step 6 (false). Finally, "
                                    "in step 10, we will incorporate the change if we decided to keep it, or copy the question "
                                    "from step 6 if we decided not to keep the change.",
                                ],
                            },
                            "step_8": {
                                "type": "string",
                                "description": "Suggest a change to the question. List the pros and the cons of that change. Discuss if the change is redundant.",
                            },
                            "step_9": {
                                "type": "boolean",
                                "description": "True if you found a change you want to make, false if you did not",
                            },
                            "step_10": {
                                "type": "string",
                                "description": "The final question. If 7b is false, this should be the same as step_6. Otherwise, it should incorporate the change from step_7a",
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
                            "step_7",
                            "step_8",
                            "step_9",
                            "step_10",
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
            result = args["step_10"]
            if isinstance(result, str):
                return result
        except Exception as e:
            await handle_warning(
                f"{__name__}:options",
                f"Failed to parse reflection question from chat response",
                exc=e,
            )
    return None
