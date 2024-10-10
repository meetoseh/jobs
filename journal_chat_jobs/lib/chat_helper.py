import asyncio
from dataclasses import dataclass
import gzip
import secrets
import string
import time
from typing import (
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

from error_middleware import handle_warning
from itgs import Itgs
from journal_chat_jobs.lib.data_to_client import (
    get_journal_chat_job_journey_metadata,
    get_journal_chat_job_voice_note_metadata,
)
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from journal_chat_jobs.lib.openai_ratelimits import (
    OpenAICategory,
    reserve_openai_tokens,
)
from lib.journals.journal_chat import JournalChat
from lib.journals.journal_chat_redis_packet import (
    EventBatchPacketDataItemDataError,
    EventBatchPacketDataItemDataThinkingBar,
    EventBatchPacketDataItemDataThinkingSpinner,
    JournalChatRedisPacket,
    JournalChatRedisPacketMutations,
    JournalChatRedisPacketPassthrough,
    SegmentDataMutation,
)
from lib.journals.journal_entry_item_data import (
    JournalEntryItemData,
    JournalEntryItemDataClient,
    JournalEntryItemDataDataTextual,
    JournalEntryItemDataDataTextualClient,
    JournalEntryItemProcessingBlockedReason,
    JournalEntryItemTextualPartParagraph,
)
from lib.journals.publish_journal_chat_event import publish_journal_chat_event
from lib.journals.serialize_journal_chat_event import serialize_journal_chat_event
from redis_helpers.journal_chat_job_finish import safe_journal_chat_job_finish
import unix_dates


def break_paragraphs(text: str) -> List[str]:
    """Breaks the given text into paragraphs, removing empty lines and leading/trailing whitespace"""
    result = [p.strip() for p in text.split("\n")]
    return [p for p in result if p]


def get_message_from_text(
    text: str,
    /,
    *,
    type: Literal["chat", "reflection-question", "reflection-response"] = "chat",
    display_author: Literal["self", "other"] = "other",
    processing_block: Optional[JournalEntryItemProcessingBlockedReason],
) -> Tuple[JournalEntryItemData, JournalEntryItemDataClient]:
    """Produces a journal entry item from the given text. Returns both the
    internal format and the client format.
    """
    return get_message_from_paragraphs(
        break_paragraphs(text),
        type=type,
        display_author=display_author,
        processing_block=processing_block,
    )


def get_message_from_paragraphs(
    paragraphs: List[str],
    /,
    *,
    type: Literal["chat", "reflection-question", "reflection-response"] = "chat",
    display_author: Literal["self", "other"] = "other",
    processing_block: Optional[JournalEntryItemProcessingBlockedReason],
) -> Tuple[JournalEntryItemData, JournalEntryItemDataClient]:
    """Produces a journal entry item from the given paragraphs. Returns both the
    internal format and the client format.
    """
    return JournalEntryItemData(
        type=type,
        data=JournalEntryItemDataDataTextual(
            type="textual",
            parts=[
                JournalEntryItemTextualPartParagraph(
                    type="paragraph",
                    value=p,
                )
                for p in paragraphs
            ],
        ),
        processing_block=processing_block,
        display_author=display_author,
    ), JournalEntryItemDataClient(
        type=type,
        data=JournalEntryItemDataDataTextualClient(
            type="textual",
            parts=[
                JournalEntryItemTextualPartParagraph(
                    type="paragraph",
                    value=p,
                )
                for p in paragraphs
            ],
        ),
        display_author=display_author,
    )


async def extract_as_text(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    item: JournalEntryItemData,
) -> str:
    """Assuming that the given journal item contains only text (paragraphs or voice
    notes), returns the text representation of the item.
    """
    assert item.type == "chat", "unknown item type"
    assert item.data.type == "textual", "unknown item data type"
    assert item.data.parts, "empty item"

    parts: List[str] = []
    for part in item.data.parts:
        if part.type == "paragraph":
            parts.append(part.value)
        elif part.type == "voice_note":
            metadata = await get_journal_chat_job_voice_note_metadata(
                itgs,
                ctx=ctx,
                voice_note_uid=part.voice_note_uid,
            )
            if metadata is not None:
                parts.append(" ".join(text for _, text in metadata.transcript.phrases))
            else:
                parts.append("(deleted voice note)")
        else:
            raise ValueError(f"unknown part type: {part.type}")
    return "\n\n".join(parts)


async def convert_textual_to_markdown(
    itgs: Itgs,
    *,
    ctx: JournalChatJobContext,
    item: JournalEntryItemDataDataTextual,
    convert_links: bool = False,
) -> str:
    """Converts the given textual data to a markdown-adjacent format. Besides
    explicitly convertable options (like paragraphs), you must opt-in to converting
    additional types:

    - `convert_links`: converts `journey` entries into the format
      `[{title}}](journey:{title_to_id})`
    """

    result_parts = []
    for part in item.parts:
        if part.type == "paragraph":
            result_parts.append(part.value)
        elif convert_links and part.type == "journey":
            metadata = await get_journal_chat_job_journey_metadata(
                itgs, ctx=ctx, journey_uid=part.uid
            )
            if metadata is None:
                result_parts.append("[journey](journey:unknown)")
            else:
                result_parts.append(
                    f"[{metadata.title}](journey:{make_id(metadata.title)})"
                )
        else:
            raise ValueError(f"unknown part type: {part.type}")

    return "\n\n".join(result_parts)


def extract_as_journey_uid(
    item: JournalEntryItemData,
) -> str:
    """Assuming that the given journal item contains only a ui entry to a journey,
    returns the journey uid it points to
    """
    assert item.type == "ui", "unknown item type"
    assert item.data.type == "ui", "unknown item data type"
    assert (
        item.data.conceptually.type == "user_journey"
    ), "unknown item conceptually type"
    return item.data.conceptually.journey_uid


async def publish_packet(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    final: bool,
    packet: JournalChatRedisPacket,
) -> None:
    """Serializes the given packet and publishes it to redis. If final is True,
    this also closes out the journal chat job.
    """
    event_at = time.time()
    event = serialize_journal_chat_event(
        journal_master_key=ctx.journal_master_key,
        event=packet,
        now=time.time(),
    )
    await ctx.maybe_check_redis(itgs)

    if final:
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=event,
            now=int(event_at),
        )
    else:
        await publish_journal_chat_event(
            itgs, journal_chat_uid=ctx.journal_chat_uid, event=event
        )


async def publish_spinner(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    message: str,
    detail: Optional[str] = None,
) -> None:
    """Publishes a journal chat passthrough event which has the client display a spinner."""
    await publish_packet(
        itgs,
        ctx=ctx,
        final=False,
        packet=JournalChatRedisPacketPassthrough(
            counter=ctx.reserve_event_counter(),
            type="passthrough",
            event=EventBatchPacketDataItemDataThinkingSpinner(
                type="thinking-spinner", message=message, detail=detail
            ),
        ),
    )


async def publish_pbar(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    message: str,
    detail: Optional[str] = None,
    at: int,
    of: int,
) -> None:
    """Publishes a journal chat passthrough event which has the client display a progress bar."""
    await publish_packet(
        itgs,
        ctx=ctx,
        final=False,
        packet=JournalChatRedisPacketPassthrough(
            counter=ctx.reserve_event_counter(),
            type="passthrough",
            event=EventBatchPacketDataItemDataThinkingBar(
                type="thinking-bar", message=message, detail=detail, at=at, of=of
            ),
        ),
    )


async def publish_error(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, message: str, detail: Optional[str]
) -> None:
    """Publishes an error packet to the journal chat"""
    await publish_packet(
        itgs,
        ctx=ctx,
        final=True,
        packet=JournalChatRedisPacketPassthrough(
            counter=ctx.reserve_event_counter(),
            type="passthrough",
            event=EventBatchPacketDataItemDataError(
                type="error", message=message, detail=detail
            ),
        ),
    )


async def publish_error_and_close_out(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    warning_id: str,
    stat_id: str,
    warning_message: str,
    client_message: str,
    client_detail: Optional[str],
    exc: Optional[BaseException] = None,
) -> None:
    """Publishes a warning, pushes an error packet to the journal chat, closes out the journal chat job,
    and increments the number of failures.
    """
    await handle_warning(warning_id, warning_message, exc=exc)
    await publish_error(itgs, ctx=ctx, message=client_message, detail=client_detail)
    ctx.stats.incr_failed(
        requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz,
        reason=stat_id.encode("utf-8"),
        type=ctx.type,
    )


async def publish_mutations(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    final: bool,
    mutations: List[SegmentDataMutation],
) -> None:
    """Publishes a list of mutations to the journal chat. Closes out the journal chat if final is True."""
    await publish_packet(
        itgs,
        ctx=ctx,
        final=final,
        packet=JournalChatRedisPacketMutations(
            counter=ctx.reserve_event_counter(),
            type="mutations",
            mutations=mutations,
            more=not final,
        ),
    )


async def publish_entire_chat_state(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    final: bool,
    data: List[JournalEntryItemDataClient],
) -> None:
    """Publishes a new chat state consisting of the given items in a single mutation. This
    is inefficient if you can reuse information you have already sent to the client, but is
    fine if you cannot
    """
    chat_state = JournalChat(uid=ctx.journal_chat_uid, integrity="", data=data)
    chat_state.integrity = chat_state.compute_integrity()
    await publish_mutations(
        itgs,
        ctx=ctx,
        final=final,
        mutations=[SegmentDataMutation(key=[], value=chat_state)],
    )


@dataclass
class WriteJournalEntryItemResultJournalEntryNotFound:
    type: Literal["journal_entry_not_found"]


@dataclass
class WriteJournalEntryItemResultEncryptionFailed:
    type: Literal["encryption_failed"]


@dataclass
class WriteJournalEntryItemResultUpsertFailed:
    type: Literal["upsert_failed"]


@dataclass
class WriteJournalEntryItemResultSuccess:
    type: Literal["success"]
    journal_entry_item_uid: str


WriteJournalEntryItemResult = Union[
    WriteJournalEntryItemResultJournalEntryNotFound,
    WriteJournalEntryItemResultEncryptionFailed,
    WriteJournalEntryItemResultUpsertFailed,
    WriteJournalEntryItemResultSuccess,
]


async def write_journal_entry_item(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    message: JournalEntryItemData,
    replace_journal_entry_item_uid: Optional[str] = None,
) -> WriteJournalEntryItemResult:
    """Replaces or inserts a journal entry item. If replace_journal_entry_uid is None, a new
    journal entry item is inserted. Otherwise, the existing journal entry item is replaced.

    If an error occurs this closes out the journal chat (updating the stats appropriately)
    and raises an exception.
    """
    await ctx.maybe_check_redis(itgs)
    if replace_journal_entry_item_uid is None:
        return await _insert_journal_entry_item(
            itgs,
            ctx=ctx,
            message=message,
        )
    return await _replace_journal_entry_item(
        itgs,
        ctx=ctx,
        message=message,
        replace_journal_entry_item_uid=replace_journal_entry_item_uid,
    )


async def write_journal_entry_item_closing_out_on_failure(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    message: JournalEntryItemData,
    replace_journal_entry_item_uid: Optional[str] = None,
) -> Optional[str]:
    """Writes the journal entry item (inserting or replacing as indicated by
    `replace_journal_entry_uid`), returning the uid of the journal entry item
    that was written or replaced.

    If an error occurs, closes out the chat job (updating stats and publishing
    an error, etc) and returns None
    """
    result = await write_journal_entry_item(
        itgs,
        ctx=ctx,
        message=message,
        replace_journal_entry_item_uid=replace_journal_entry_item_uid,
    )
    if result.type == "success":
        return result.journal_entry_item_uid

    await publish_error_and_close_out(
        itgs,
        ctx=ctx,
        warning_id=f"{__name__}:write_failed",
        stat_id=f"write_failed:{result.type}",
        warning_message=f"Failed to store entry item for `{ctx.user_sub}`: {result.type}",
        client_message="Failed to store journal entry item",
        client_detail=result.type,
    )
    return None


async def _insert_journal_entry_item(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    message: JournalEntryItemData,
) -> WriteJournalEntryItemResult:
    conn = await itgs.conn()
    cursor = conn.cursor()

    created_at = time.time()
    created_unix_date_in_user_tz = unix_dates.unix_timestamp_to_unix_date(
        created_at, tz=ctx.user_tz
    )

    master_encrypted_data = ctx.journal_master_key.journal_master_key.encrypt(
        gzip.compress(
            message.__pydantic_serializer__.to_json(message),
            compresslevel=9,
            mtime=0,
        )
    ).decode("ascii")

    new_journal_entry_item_uid = f"oseh_jei_{secrets.token_urlsafe(16)}"
    responses = await cursor.executeunified3(
        (
            (
                "SELECT 1 FROM journal_entries WHERE uid=?",
                (ctx.journal_entry_uid,),
            ),
            (
                "SELECT 1 FROM user_journal_master_keys WHERE uid=?",
                (ctx.journal_master_key.journal_master_key_uid,),
            ),
            (
                """
INSERT INTO journal_entry_items (
    uid, 
    journal_entry_id, 
    entry_counter, 
    user_journal_master_key_id,
    master_encrypted_data, 
    created_at, 
    created_unix_date
)
SELECT 
    ?, 
    journal_entries.id,
    COALESCE(
        (
            SELECT MAX(jei.entry_counter) 
            FROM journal_entry_items AS jei 
            WHERE jei.journal_entry_id = journal_entries.id
        ), 
        0
    ) + 1,
    user_journal_master_keys.id, 
    ?, ?, ?
FROM journal_entries, user_journal_master_keys
WHERE
    journal_entries.uid = ?
    AND user_journal_master_keys.uid = ?
                """,
                (
                    new_journal_entry_item_uid,
                    master_encrypted_data,
                    created_at,
                    created_unix_date_in_user_tz,
                    ctx.journal_entry_uid,
                    ctx.journal_master_key.journal_master_key_uid,
                ),
            ),
        ),
        transaction=True,
    )

    if not responses[0].results:
        assert (
            responses[2].rows_affected is None or responses[2].rows_affected < 1
        ), responses
        return WriteJournalEntryItemResultJournalEntryNotFound(
            type="journal_entry_not_found"
        )
    if not responses[1].results:
        assert (
            responses[2].rows_affected is None or responses[2].rows_affected < 1
        ), responses
        return WriteJournalEntryItemResultEncryptionFailed(type="encryption_failed")
    if responses[2].rows_affected is None or responses[2].rows_affected < 1:
        return WriteJournalEntryItemResultUpsertFailed(type="upsert_failed")

    return WriteJournalEntryItemResultSuccess(
        type="success",
        journal_entry_item_uid=new_journal_entry_item_uid,
    )


async def _replace_journal_entry_item(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    message: JournalEntryItemData,
    replace_journal_entry_item_uid: str,
) -> WriteJournalEntryItemResult:
    conn = await itgs.conn()
    cursor = conn.cursor()

    created_at = time.time()

    master_encrypted_data = ctx.journal_master_key.journal_master_key.encrypt(
        gzip.compress(
            message.__pydantic_serializer__.to_json(message),
            compresslevel=9,
            mtime=0,
        )
    ).decode("ascii")

    responses = await cursor.executeunified3(
        (
            (
                "SELECT 1 FROM journal_entry_items WHERE uid=?",
                (replace_journal_entry_item_uid,),
            ),
            (
                "SELECT 1 FROM user_journal_master_keys WHERE uid=?",
                (ctx.journal_master_key.journal_master_key_uid,),
            ),
            (
                """
UPDATE journal_entry_items
SET
user_journal_master_key_id = user_journal_master_keys.id,
master_encrypted_data = ?
FROM user_journal_master_keys
WHERE
user_journal_master_keys.uid = ?
AND journal_entry_items.uid = ?
                """,
                (
                    master_encrypted_data,
                    ctx.journal_master_key.journal_master_key_uid,
                    replace_journal_entry_item_uid,
                ),
            ),
        )
    )

    if not responses[0].results:
        assert (
            responses[2].rows_affected is None or responses[2].rows_affected < 1
        ), responses
        return WriteJournalEntryItemResultJournalEntryNotFound(
            type="journal_entry_not_found"
        )

    if not responses[1].results:
        assert (
            responses[2].rows_affected is None or responses[2].rows_affected < 1
        ), responses
        return WriteJournalEntryItemResultEncryptionFailed(type="encryption_failed")

    if responses[2].rows_affected is None or responses[2].rows_affected < 1:
        return WriteJournalEntryItemResultUpsertFailed(type="upsert_failed")

    return WriteJournalEntryItemResultSuccess(
        type="success",
        journal_entry_item_uid=replace_journal_entry_item_uid,
    )


class OpenAIRatelimitExcessivelyBehindException(Exception):
    """Raised when we are too far behind in the openai ratelimit queue"""

    def __init__(self):
        super().__init__("ratelimited-excessive")


async def reserve_tokens(
    itgs: Itgs, /, *, ctx: JournalChatJobContext, category: OpenAICategory, tokens: int
) -> None:
    """Wraps `reserve_openai_tokens`, handling the wait response for you. Raises
    an error if reserving would cause us to wait excessively long, and makes sure
    the client doesn't reconnect to the websocket server due to timeout while we
    are waiting
    """

    await ctx.maybe_check_redis(itgs)
    reserve_result = await reserve_openai_tokens(
        itgs, category=category, count=tokens, max_wait_seconds=120
    )
    if reserve_result.type == "wait":
        started_at = time.time()
        done_at = time.time() + reserve_result.wait_seconds
        while True:
            now = time.time()
            sleep_remaining = done_at - now
            if sleep_remaining <= 0:
                break
            sleep_done = now - started_at
            if sleep_done > 10:
                await publish_pbar(
                    itgs,
                    ctx=ctx,
                    message=f"Waiting in {category} queue",
                    detail=f"{int(sleep_done)}/{reserve_result.wait_seconds} seconds",
                    at=int(sleep_done),
                    of=reserve_result.wait_seconds,
                )
            await asyncio.sleep(min(sleep_remaining, 5))
    elif reserve_result.type == "fail":
        raise OpenAIRatelimitExcessivelyBehindException()


def make_id(v: str) -> str:
    """Simple function to take a safe string and convert it into something that
    is sort of an identifier
    """
    return "".join(
        c
        for c in v.replace(" ", "_").lower()
        if c in string.ascii_lowercase or c == "_"
    )
