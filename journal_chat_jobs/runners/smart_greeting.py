import asyncio
import os
from typing import List, Optional, cast

import openai
from error_middleware import handle_warning
from itgs import Itgs
from journal_chat_jobs.lib.history.db import (
    CorruptedJournalEntryItem,
    JournalEntryItem,
    JournalEntryMetadata,
    load_user_journal_history,
)
from journal_chat_jobs.lib.history.generic_context_db import (
    StandardStreamingUserLLMContextProcessor,
    load_user_llm_context,
)
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext

import journal_chat_jobs.lib.chat_helper as chat_helper
from journal_chat_jobs.runners.greeting import handle_greeting
from lib.journals.user_master_keys_memory_cache import UserMasterKeysMemoryCache
from lib.users.time_of_day import get_time_of_day
import unix_dates
import logging


JOURNAL_CONTEXT_WINDOW_MAX_CHARACTERS = 400_000
JOURNAL_CONTEXT_WINDOW_MAX_ENTRIES = 20

GENERIC_CONTEXT_WINDOW_MAX_CHARACTERS = 100_000
GENERIC_CONTEXT_WINDOW_MAX_ENTRIES = 20

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


class _ContextWindowProcessor:
    def __init__(self, itgs: Itgs, ctx: JournalChatJobContext, canceler: asyncio.Event):
        self.itgs = itgs
        self.parts: List[str] = []
        self.entry_parts: Optional[List[str]] = None
        self.entry_is_useful = False
        self.ctx = ctx
        self.canceler = canceler
        self.character_length = 0
        self.last_self_part = 0

    async def start_entry(self, entry: JournalEntryMetadata) -> None:
        date_for_user = unix_dates.unix_date_to_date(
            entry.canonical_unix_date
        ).strftime("%A, %b %d, %Y")
        time_of_day_for_user = get_time_of_day(
            entry.canonical_at, self.ctx.user_tz
        ).value
        self.entry_parts = [
            f'<conversation date="{date_for_user}" time_of_day="{time_of_day_for_user}">'
        ]
        self.entry_is_useful = False
        self.last_self_part = 0

    async def process_item(
        self, entry: JournalEntryMetadata, item: JournalEntryItem
    ) -> None:
        if self.entry_parts is None:
            return

        if item.data.data.type != "textual" or not item.data.data.parts:
            return

        if (
            item.data.display_author == "other"
            and item.data.type == "chat"
            and item.entry_counter > 1
        ):
            return

        if item.data.processing_block is not None:
            self.entry_parts = None
            return

        try:
            textual = await chat_helper.extract_as_text(
                self.itgs, ctx=self.ctx, item=item.data
            )
        except Exception as e:
            await handle_warning(
                f"{__name__}:extract_as_text",
                f"Failed to extract text from item {item.uid}",
                e,
            )
            self.entry_parts = None
            return

        if item.data.display_author == "self":
            self.entry_is_useful = True
            self.last_self_part = len(self.entry_parts)

        author = "user" if item.data.display_author == "self" else "assistant"
        self.entry_parts.append(
            f'<message type="{item.data.type}" author="{author}">{textual}</message>'
        )

    async def process_corrupted_item(
        self, entry: JournalEntryMetadata, item: CorruptedJournalEntryItem
    ) -> None: ...

    async def end_entry(self, entry: JournalEntryMetadata) -> None:
        if self.entry_parts is None:
            return
        if not self.entry_is_useful:
            self.entry_parts = None
            return
        if self.canceler.is_set():
            self.entry_parts = None
            return

        if len(self.entry_parts) > self.last_self_part + 1:
            self.entry_parts = self.entry_parts[: self.last_self_part + 1]

        self.entry_parts.append("</conversation>")
        entry_part = "".join(self.entry_parts)
        entry_part_length = len(entry_part)
        if (
            self.character_length + entry_part_length
            > JOURNAL_CONTEXT_WINDOW_MAX_CHARACTERS
        ):
            self.entry_parts = None
            self.canceler.set()
            return
        self.parts.append("".join(self.entry_parts))
        self.character_length += entry_part_length
        self.entry_parts = None

        if len(self.parts) >= JOURNAL_CONTEXT_WINDOW_MAX_ENTRIES:
            self.canceler.set()


async def make_context_window(itgs: Itgs, ctx: JournalChatJobContext) -> List[str]:
    """Creates a context window for the user, including all their previous messages."""
    load_journal_canceler = asyncio.Event()
    load_generic_canceler = asyncio.Event()
    journal_processor = _ContextWindowProcessor(itgs, ctx, load_journal_canceler)
    generic_processor = StandardStreamingUserLLMContextProcessor(
        max_items=GENERIC_CONTEXT_WINDOW_MAX_ENTRIES,
        max_characters=GENERIC_CONTEXT_WINDOW_MAX_CHARACTERS,
        canceler=load_generic_canceler,
    )
    async with UserMasterKeysMemoryCache(user_sub=ctx.user_sub) as master_keys:
        load_generic_task = asyncio.create_task(
            load_user_llm_context(
                itgs,
                user_sub=ctx.user_sub,
                master_keys=master_keys,
                processor=generic_processor,
                read_consistency="weak",
                per_query_limit=min(100, GENERIC_CONTEXT_WINDOW_MAX_ENTRIES),
            )
        )
        load_journal_task = asyncio.create_task(
            load_user_journal_history(
                itgs,
                user_sub=ctx.user_sub,
                master_keys=master_keys,
                processor=journal_processor,
                read_consistency="weak",
                # some of these can be not helpful so we will leave per_query_limit
                # at the default
            )
        )
        wait_generic_task = asyncio.create_task(load_generic_canceler.wait())
        wait_journal_task = asyncio.create_task(load_journal_canceler.wait())

        generic_done = False
        journal_done = False
        try:
            while True:
                tasks = []
                if not generic_done:
                    tasks.append(wait_generic_task)
                    tasks.append(load_generic_task)

                if not journal_done:
                    tasks.append(wait_journal_task)
                    tasks.append(load_journal_task)

                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                if not generic_done:
                    if wait_generic_task.done():
                        generic_done = True
                        if not load_generic_task.cancel():
                            await load_generic_task
                    elif load_generic_task.done():
                        generic_done = True
                        if not wait_generic_task.cancel():
                            await wait_generic_task

                if not journal_done:
                    if wait_journal_task.done():
                        journal_done = True
                        if not load_journal_task.cancel():
                            await load_journal_task
                    elif load_journal_task.done():
                        journal_done = True
                        if not wait_journal_task.cancel():
                            await wait_journal_task

                if generic_done and journal_done:
                    break
        except:
            wait_journal_task.cancel()
            wait_generic_task.cancel()
            load_journal_task.cancel()
            load_generic_task.cancel()

    journal_processor.parts.reverse()
    generic_processor.items.reverse()
    return generic_processor.items + journal_processor.parts


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
                "that they can take it where ever they want.\n"
                "Pay attention to the dates involved.",
            },
            {"role": "user", "content": "".join(content_window)},
        ],
        model=LARGE_MODEL,
    )
