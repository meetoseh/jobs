import asyncio
import io
import json
import os
from typing import List, Optional

import openai
from error_middleware import handle_warning
from itgs import Itgs
from journal_chat_jobs.lib.data_to_client import (
    data_to_client,
    get_journal_chat_job_journey_metadata,
)
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext

import journal_chat_jobs.lib.chat_helper as chat_helper
from lib.journals.conversation_stream import JournalChatJobConversationStream
from lib.journals.journal_entry_item_data import (
    JournalEntryItemData,
    JournalEntryItemDataClient,
    JournalEntryItemDataDataSummaryV1,
)


LARGE_MODEL = "gpt-4o"
SMALL_MODEL = "gpt-4o-mini"

BIG_RATELIMIT_CATEGORY = "gpt-4o"
SMALL_RATELIMIT_CATEGORY = "gpt-4o-mini"


async def handle_summarize(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """Adds or regenerates a summary entry item on the journal entry, consisting
    of a 3-4 word title and some tags.
    """
    await chat_helper.publish_spinner(
        itgs, ctx=ctx, message="Fetching existing state..."
    )

    conversation_stream = JournalChatJobConversationStream(
        journal_entry_uid=ctx.journal_entry_uid,
        user_sub=ctx.user_sub,
        pending_moderation="resolve",
    )
    await conversation_stream.start()

    client_items: List[JournalEntryItemDataClient] = []
    replace_index: Optional[int] = None
    while True:
        result = await conversation_stream.load_next_item(timeout=5)
        if result.type == "finished":
            break

        if result.type != "item":
            await conversation_stream.cancel()
            await chat_helper.publish_error_and_close_out(
                itgs,
                ctx=ctx,
                warning_id=f"{__name__}:failed_to_load_history",
                stat_id="failed_to_load_history",
                warning_message=f"failed to load history for summarizing {ctx.journal_entry_uid} - {result.type}",
                client_message="Failed to load entry",
                client_detail=result.type,
                exc=result.error if result.type == "error" else None,
            )
            return

        client_items.append(await data_to_client(itgs, ctx=ctx, item=result.item.data))

        if (
            ctx.task.replace_entry_item_uid is not None
            and result.item.uid == ctx.task.replace_entry_item_uid
        ):
            replace_index = len(client_items) - 1
        elif result.item.data.type == "summary":
            await conversation_stream.cancel()
            await chat_helper.publish_error_and_close_out(
                itgs,
                ctx=ctx,
                warning_id=f"{__name__}:has_summary",
                stat_id="has_summary",
                warning_message=f"This entry cannot have two summaries! `{ctx.journal_entry_uid}`",
                client_message="Already summarized",
                client_detail=None,
            )
            return

    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Summarizing...")

    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    try:
        summary = await _get_summary_from_llm(
            itgs, ctx=ctx, client_items=client_items, client=client
        )
    except Exception as e:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:failed_to_summarize",
            stat_id="failed_to_summarize",
            warning_message=f"Failed to summarize {ctx.journal_entry_uid}",
            client_message="Failed to summarize",
            client_detail=None,
            exc=e,
        )
        return

    if summary is None:
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:failed_to_summarize:empty",
            stat_id="no_content",
            warning_message=f"Failed to summarize {ctx.journal_entry_uid}",
            client_message="Failed to summarize",
            client_detail=None,
        )
        return

    data = (
        JournalEntryItemData(
            data=summary, display_author="other", processing_block=None, type="summary"
        ),
        JournalEntryItemDataClient(
            data=summary, display_author="other", type="summary"
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

    if replace_index is None:
        client_items.append(data[1])
    else:
        client_items[replace_index] = data[1]

    await chat_helper.publish_entire_chat_state(
        itgs, ctx=ctx, final=True, data=client_items
    )
    ctx.stats.incr_completed(
        requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz, type=ctx.type
    )


async def _get_summary_from_llm(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    client_items: List[JournalEntryItemDataClient],
    client: openai.OpenAI,
) -> Optional[JournalEntryItemDataDataSummaryV1]:
    await chat_helper.reserve_tokens(
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=2048
    )
    chat_response = await asyncio.to_thread(
        client.chat.completions.create,
        messages=[
            {
                "role": "system",
                "content": "Adopt the role of a data archiver. Focus on self, not what the other entity said. "
                "Generate a short (1 to 4 word) title and 2 tags describing the provided information. For tags, where applicable, you prefer to use "
                "emotion words (e.g., '😬 Anxiety')",
            },
            {
                "role": "user",
                "content": "\n\n".join(
                    [
                        p
                        for p in [
                            await client_item_to_text(itgs, ctx=ctx, item=i)
                            for i in client_items
                        ]
                        if p
                    ]
                ),
            },
        ],
        model=LARGE_MODEL,
        max_tokens=2048,
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "store_summary",
                    "description": "Stores the summary information",
                    "strict": True,
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "title": {
                                "type": "string",
                                "description": "1 to 4 words summarizing the information",
                            },
                            "tag1": {
                                "type": "object",
                                "required": ["emoji", "text"],
                                "additionalProperties": False,
                                "properties": {
                                    "emoji": {
                                        "type": "string",
                                        "description": "Emoji for the first tag.",
                                    },
                                    "text": {
                                        "type": "string",
                                        "description": "Keyword related to the information.",
                                    },
                                },
                            },
                            "tag2": {
                                "type": "object",
                                "required": ["emoji", "text"],
                                "additionalProperties": False,
                                "properties": {
                                    "emoji": {
                                        "type": "string",
                                        "description": "Emoji for the second tag.",
                                    },
                                    "text": {
                                        "type": "string",
                                        "description": "Keyword related to the information.",
                                    },
                                },
                            },
                        },
                        "required": ["title", "tag1", "tag2"],
                        "additionalProperties": False,
                    },
                },
            }
        ],
        tool_choice={
            "type": "function",
            "function": {"name": "store_summary"},
        },
    )

    chat_message = chat_response.choices[0].message
    if (
        chat_message.tool_calls
        and chat_message.tool_calls[0].function.name == "store_summary"
    ):
        args_json = chat_message.tool_calls[0].function.arguments
        try:
            args = json.loads(args_json)
            title = args["title"]
            tag1_emoji = args["tag1"]["emoji"]
            tag1_text = args["tag1"]["text"]
            tag2_emoji = args["tag2"]["emoji"]
            tag2_text = args["tag2"]["text"]

            tags = [f"{tag1_emoji} {tag1_text}", f"{tag2_emoji} {tag2_text}"]

            assert isinstance(title, str), args
            return JournalEntryItemDataDataSummaryV1(
                tags=tags, title=title, type="summary", version="v1"
            )
        except Exception as e:
            await handle_warning(
                f"{__name__}:options",
                f"Failed to parse summary from chat response",
                exc=e,
            )
    return None


async def client_item_to_text(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, item: JournalEntryItemDataClient
) -> Optional[str]:
    if item.data.type == "textual":
        result = io.StringIO()
        result.write(item.display_author)
        result.write(":\n\n```md\n")
        for part in item.data.parts:
            if part.type == "paragraph":
                result.write(part.value)
                result.write("\n\n")
            elif part.type == "journey":
                result.write(f"[{part.details.title}](journey:{part.details.uid})")
                result.write("\nAuthor: ")
                result.write(part.details.instructor.name)
                result.write("\nDescription: ")
                result.write(part.details.description)
                result.write("\n\n")

        result.write("```")
        return result.getvalue()

    if item.data.type == "ui" and item.data.conceptually.type == "user_journey":
        journey_uid = item.data.conceptually.journey_uid
        journey_info = await get_journal_chat_job_journey_metadata(
            itgs, ctx=ctx, journey_uid=journey_uid
        )
        if journey_info is not None:
            return f"self: Took class '{journey_info.title}' by '{journey_info.instructor.name}'"

    return None
