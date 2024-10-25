import asyncio
import os
from typing import List, Optional, Tuple, cast

import openai
from error_middleware import handle_warning
from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext

import journal_chat_jobs.lib.chat_helper as chat_helper
from journal_chat_jobs.runners.greeting import handle_greeting
from lib.journals.conversation_stream import (
    JournalChatJobConversationStream,
    JournalEntryItem,
)
from lib.users.time_of_day import get_time_of_day
import unix_dates
import logging


CONTEXT_WINDOW_MAX_WORDS = 100_000
CONTEXT_WINDOW_MAX_CHARACTERS = 500_000
CONTEXT_WINDOW_MAX_ENTRIES = 20

# LARGE_MODEL = "o1-preview"
# BIG_RATELIMIT_CATEGORY = "o1-preview"
# EXPECTED_TOKENS = 32768
# SYSTEM_ROLE = "user"

LARGE_MODEL = "gpt-4o"
BIG_RATELIMIT_CATEGORY = "gpt-4o"
EXPECTED_TOKENS = 4096
SYSTEM_ROLE = "system"


async def handle_smart_greeting(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """Chooses a greeting using gpt-4o including all their previous messages. If this
    is their first time on oseh, uses a fixed greeting.
    """

    await chat_helper.publish_spinner(itgs, ctx=ctx, message="Fetching context...")
    context_window = await make_context_window(itgs, ctx)
    if not context_window:
        logging.info(f"{ctx.log_id}: nothing in context window, using simple greeting")
        return await handle_greeting(itgs, ctx)

    context_window.reverse()

    if os.environ["ENVIRONMENT"] == "dev":
        logging.info(f"Context window: {''.join(context_window)}")

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    unix_date_for_user = unix_dates.unix_timestamp_to_unix_date(
        ctx.queued_at, tz=ctx.user_tz
    )
    date_for_user = unix_dates.unix_date_to_date(unix_date_for_user).strftime(
        "%A, %b %d, %Y"
    )
    time_of_day_for_user = get_time_of_day(ctx.queued_at, ctx.user_tz).value

    response = await cursor.execute(
        "SELECT given_name FROM users WHERE sub=?", [ctx.user_sub]
    )
    given_name = (
        None
        if not response.results or not response.results[0][0]
        else cast(str, response.results[0][0])
    )
    if given_name is not None and "anon" in given_name.lower():
        given_name = None

    await chat_helper.publish_spinner(
        itgs, ctx=ctx, message="Brainstorming a greeting..."
    )
    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])
    try:
        response = await _get_smart_greeting(
            itgs,
            ctx=ctx,
            content_window=context_window,
            client=client,
            given_name=given_name,
            date_for_user=date_for_user,
            time_of_day_for_user=time_of_day_for_user,
        )
    except Exception as e:
        await handle_warning(
            f"{__name__}:_get_smart_greeting",
            "Failed to get smart greeting from OpenAI, using simple greeting",
            exc=e,
        )
        return await handle_greeting(itgs, ctx)

    message = response.choices[0].message.content
    if message is None:
        await handle_warning(
            f"{__name__}:empty_message",
            "Got empty message from openai, using simple greeting",
        )
        return await handle_greeting(itgs, ctx)

    data = chat_helper.get_message_from_text(message, processing_block=None)
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

    await chat_helper.publish_entire_chat_state(
        itgs, ctx=ctx, final=True, data=[data[1]]
    )
    ctx.stats.incr_completed(
        requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz, type=ctx.type
    )


async def make_context_window(itgs: Itgs, ctx: JournalChatJobContext) -> List[str]:
    """Creates a context window for the user, including all their previous messages."""
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    result: List[str] = []
    result_length_words = 0
    result_length_characters = 0
    result_entries = 0

    last_entry_canonical_at: Optional[float] = None
    last_entry_uid: Optional[str] = None
    while True:
        response = await cursor.execute(
            """
SELECT
    journal_entries.uid,
    journal_entries.canonical_at,
    journal_entries.canonical_unix_date
FROM journal_entries
WHERE
    journal_entries.user_id = (SELECT users.id FROM users WHERE users.sub=?)
    AND EXISTS (
        SELECT 1 FROM journal_entry_items 
        WHERE 
            journal_entry_items.journal_entry_id = journal_entries.id
            AND journal_entry_items.entry_counter > 1
    )"""
            + (
                ""
                if last_entry_uid is None
                else """
    AND (
        journal_entries.canonical_at < ?
        OR (
            journal_entries.canonical_at = ?
            AND journal_entries.uid > ?
        )
    )
ORDER BY journal_entries.canonical_at DESC, journal_entries.uid ASC
LIMIT 10
            """
            ),
            [
                ctx.user_sub,
                *(
                    []
                    if last_entry_uid is None
                    else [
                        last_entry_canonical_at,
                        last_entry_canonical_at,
                        last_entry_uid,
                    ]
                ),
            ],
        )

        if not response.results:
            return result

        rows_cast: List[Tuple[str, float, int]] = []
        for row in response.results:
            row_uid = cast(str, row[0])
            row_canonical_at = cast(float, row[1])
            row_canonical_unix_date = cast(int, row[2])
            rows_cast.append((row_uid, row_canonical_at, row_canonical_unix_date))

        last_entry_canonical_at = rows_cast[-1][1]
        last_entry_uid = rows_cast[-1][0]

        context_item_tasks: List[asyncio.Task[Optional[str]]] = []

        for row_uid, row_canonical_at, row_canonical_unix_date in rows_cast:
            context_item_tasks.append(
                asyncio.create_task(
                    get_context_window_item(
                        itgs,
                        ctx,
                        uid=row_uid,
                        canonical_at=row_canonical_at,
                        canonical_unix_date=row_canonical_unix_date,
                    )
                )
            )

        for (
            row_uid,
            row_canonical_at,
            row_canonical_unix_date,
        ), context_item_task in zip(rows_cast, context_item_tasks):
            context_item = await context_item_task

            if context_item is None:
                logging.info(
                    f"{ctx.log_id}: skipping journal entry {row_uid} - get_context_window_item result is None"
                )
                continue

            context_item_num_characters = len(context_item)

            effective_num_characters = (
                result_length_characters + context_item_num_characters
            )
            if effective_num_characters > CONTEXT_WINDOW_MAX_CHARACTERS:
                logging.info(
                    f"{ctx.log_id}: skipping journal entry {row_uid} - reached max character length (would put us at {effective_num_characters} characters)"
                )
                return result

            num_new_words = len(context_item.split())
            effective_num_words = result_length_words + num_new_words
            if effective_num_words > CONTEXT_WINDOW_MAX_WORDS:
                logging.info(
                    f"{ctx.log_id}: skipping journal entry {row_uid} - reached max word length (would put us at {effective_num_words} words)"
                )
                return result

            result.append(context_item)
            result_length_words = effective_num_words
            result_length_characters = effective_num_characters
            result_entries = result_entries + 1

        if result_entries >= CONTEXT_WINDOW_MAX_ENTRIES:
            logging.info(
                f"{ctx.log_id}: stopping at {result_entries} entries - reached max entry length"
            )
            return result


async def get_context_window_item(
    itgs: Itgs,
    ctx: JournalChatJobContext,
    /,
    *,
    uid: str,
    canonical_at: float,
    canonical_unix_date: int,
) -> Optional[str]:
    conversation_stream = JournalChatJobConversationStream(
        journal_entry_uid=uid,
        user_sub=ctx.user_sub,
        pending_moderation="resolve",
        ctx=ctx,
    )
    client_items: List[JournalEntryItem] = []
    try:
        await conversation_stream.start()
        while True:
            item = await conversation_stream.load_next_item(timeout=5)
            if item.type == "finished":
                break
            if item.type != "item":
                logging.info(
                    f"{ctx.log_id}: not producing for {uid=} - type is {item.type}"
                )
                return None

            client_items.append(item.item)
    finally:
        await conversation_stream.cancel()

    if not client_items:
        logging.info(f"{ctx.log_id}: not producing for {uid=} - no client items")
        return None

    if not any(item.data.display_author == "self" for item in client_items):
        logging.info(f"{ctx.log_id}: not producing for {uid=} - no self items")
        return None

    relabel_author = {"self": "user", "other": "assistant"}
    parts: List[str] = []
    for item in client_items:
        if item.data.processing_block is not None:
            return None

        if item.data.data.type != "textual" or not item.data.data.parts:
            continue

        try:
            textual = await chat_helper.extract_as_text(itgs, ctx=ctx, item=item.data)
        except Exception as e:
            await handle_warning(
                f"{__name__}:extract_as_text",
                f"Failed to extract text from item {item.uid}",
                e,
            )
            return None
        parts.append(
            f'<message type="{item.data.type}" author="{relabel_author[item.data.display_author]}">{textual}</message>'
        )

    date_for_user = unix_dates.unix_date_to_date(canonical_unix_date).strftime(
        "%A, %b %d, %Y"
    )
    time_of_day_for_user = get_time_of_day(canonical_at, ctx.user_tz).value
    joined_parts = "".join(parts)
    return f'<conversation date="{date_for_user}" time_of_day="{time_of_day_for_user}">{joined_parts}</conversation>'


async def _get_smart_greeting(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    content_window: List[str],
    client: openai.OpenAI,
    given_name: Optional[str],
    date_for_user: str,
    time_of_day_for_user: str,
):
    await chat_helper.reserve_tokens(
        itgs, ctx=ctx, category=BIG_RATELIMIT_CATEGORY, tokens=EXPECTED_TOKENS
    )
    name = f", {given_name}, " if given_name is not None else ""

    return await asyncio.to_thread(
        client.chat.completions.create,
        messages=[
            {
                "role": SYSTEM_ROLE,
                "content": f"Date: {date_for_user}\nTime of Day:{time_of_day_for_user}\n"
                f"You're a friendly colleague coming up with a good way to ask your friend{name} how they feel based on what you remember of your previous conversations.\n"
                "Format your response as a short plain text question, like 'How are you feeling today?'\n"
                "You can and should reference specific details, but make the question general enough "
                "that they can take it where ever they want",
            },
            {"role": "user", "content": "".join(content_window)},
        ],
        model=LARGE_MODEL,
    )
