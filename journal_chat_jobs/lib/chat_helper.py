import gzip
import secrets
import string
import time
from typing import (
    AsyncIterable,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
    Union,
    cast,
)

import pytz

from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from lib.journals.journal_chat import JournalChat
from lib.journals.journal_chat_redis_packet import (
    EventBatchPacketDataItemDataError,
    EventBatchPacketDataItemDataThinkingBar,
    EventBatchPacketDataItemDataThinkingSpinner,
    JournalChatRedisPacketMutations,
    JournalChatRedisPacketPassthrough,
    SegmentDataMutation,
)
from lib.journals.journal_entry_item_data import (
    JournalEntryItemData,
    JournalEntryItemDataClient,
    JournalEntryItemDataDataTextual,
    JournalEntryItemDataDataTextualClient,
    JournalEntryItemTextualPartParagraph,
)
from lib.journals.journal_stats import JournalStats
from lib.journals.publish_journal_chat_event import publish_journal_chat_event
from lib.journals.serialize_journal_chat_event import serialize_journal_chat_event
from lib.redis_stats_preparer import RedisStatsPreparer
from lib.users.timezones import get_user_timezone
from redis_helpers.journal_chat_job_finish import safe_journal_chat_job_finish
import unix_dates


class TechniqueParameters(TypedDict):
    """The general technique that is being used to produce the response"""

    type: Literal["llm"]
    platform: Literal["openai"]
    model: str
    """The model being used, e.g., gpt-4o. If multiple models are being used,
    the most expensive one
    """


class ResponsePipeline(Protocol):
    def __call__(
        self,
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
    ]: ...


async def handle_chat_outer_loop(
    itgs: Itgs,
    ctx: JournalChatJobContext,
    /,
    *,
    technique_parameters: TechniqueParameters,
    response_pipeline: ResponsePipeline,
    prompt_identifier: str,
) -> None:
    """Manages the basic chat loop, given a response pipeline function.

    The response pipeline should either

    ```py
    yield is_final, message, message_client
    ```

    or

    ```py
    yield None, error
    ```

    When `replace_index` is None, this responds to the conversation of the the
    current point, where presumably the last message in the conversation is from
    the user.

    Alternatively, if `replace_index` is set, replaces the message at that index
    with a new response from the system. This will only work well if the item being
    replaced was also generated by this function
    """
    assert ctx.task.replace_index is None or (
        ctx.task.replace_index >= 0
        and ctx.task.replace_index < len(ctx.task.conversation)
    ), "bad replace index"

    stats = JournalStats(RedisStatsPreparer())
    try:
        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        responses = await cursor.executeunified3(
            (
                ("SELECT given_name FROM users WHERE sub=?", [ctx.user_sub]),
                (
                    "SELECT 1 FROM journal_entries, users WHERE journal_entries.uid = ? AND journal_entries.user_id = users.id AND users.sub = ?",
                    [ctx.journal_entry_uid, ctx.user_sub],
                ),
                *(
                    []
                    if ctx.task.replace_index is None
                    else [
                        (
                            "SELECT journal_entry_items.uid FROM journal_entry_items, journal_entries WHERE journal_entries.uid = ? AND journal_entry_items.journal_entry_id = journal_entries.id AND journal_entry_items.entry_counter = ?",
                            [ctx.journal_entry_uid, ctx.task.replace_index + 1],
                        )
                    ]
                ),
            ),
            transaction=True,
        )

        if not responses[0].results:
            stats.incr_system_chats_failed_internal(
                unix_date=ctx.queued_at_unix_date_in_stats_tz,
                **technique_parameters,
                prompt_identifier="preparation",
                category="internal",
            )
            event_at = time.time()
            await safe_journal_chat_job_finish(
                itgs,
                journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
                log_id=ctx.log_id.encode("utf-8"),
                last_event=serialize_journal_chat_event(
                    journal_master_key=ctx.journal_master_key,
                    event=JournalChatRedisPacketPassthrough(
                        counter=ctx.reserve_event_counter(),
                        type="passthrough",
                        event=EventBatchPacketDataItemDataError(
                            type="error",
                            message="Failed to create response",
                            detail="user not found",
                        ),
                    ),
                    now=event_at,
                ),
                now=int(event_at),
            )
            raise ValueError(f"User {ctx.user_sub} not found")

        if not responses[1].results:
            stats.incr_system_chats_failed_internal(
                unix_date=ctx.queued_at_unix_date_in_stats_tz,
                **technique_parameters,
                prompt_identifier="preparation",
                category="internal",
            )
            event_at = time.time()
            await safe_journal_chat_job_finish(
                itgs,
                journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
                log_id=ctx.log_id.encode("utf-8"),
                last_event=serialize_journal_chat_event(
                    journal_master_key=ctx.journal_master_key,
                    event=JournalChatRedisPacketPassthrough(
                        counter=ctx.reserve_event_counter(),
                        type="passthrough",
                        event=EventBatchPacketDataItemDataError(
                            type="error",
                            message="Failed to create response",
                            detail="journal entry not found",
                        ),
                    ),
                    now=event_at,
                ),
                now=int(event_at),
            )
            raise ValueError(
                f"Journal entry {ctx.journal_entry_uid} not found or not for {ctx.user_sub}"
            )

        given_name = cast(Optional[str], responses[0].results[0][0])
        if ctx.task.replace_index is None:
            replace_journal_entry_uid = None
        else:
            if not responses[2].results:
                stats.incr_system_chats_failed_internal(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    **technique_parameters,
                    prompt_identifier="preparation",
                    category="internal",
                )
                event_at = time.time()
                await safe_journal_chat_job_finish(
                    itgs,
                    journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
                    log_id=ctx.log_id.encode("utf-8"),
                    last_event=serialize_journal_chat_event(
                        journal_master_key=ctx.journal_master_key,
                        event=JournalChatRedisPacketPassthrough(
                            counter=ctx.reserve_event_counter(),
                            type="passthrough",
                            event=EventBatchPacketDataItemDataError(
                                type="error",
                                message="Failed to create response",
                                detail="journal entry item not found",
                            ),
                        ),
                        now=event_at,
                    ),
                    now=int(event_at),
                )
                raise ValueError(
                    f"Journal entry {ctx.journal_entry_uid} has no item at index {ctx.task.replace_index} (i.e., with counter {ctx.task.replace_index + 1})"
                )
            replace_journal_entry_uid = responses[2].results[0][0]

        greeting = ctx.task.conversation[-2]
        user_message = ctx.task.conversation[-1]

        user_tz = await get_user_timezone(itgs, user_sub=ctx.user_sub)

        seen_final = False
        async for item in response_pipeline(
            itgs, ctx=ctx, greeting=greeting, user_message=user_message, stats=stats
        ):
            assert not seen_final, "final message must be last"
            if item[0] is None:
                message = item[1]
                event_at = time.time()
                await safe_journal_chat_job_finish(
                    itgs,
                    journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
                    log_id=ctx.log_id.encode("utf-8"),
                    last_event=serialize_journal_chat_event(
                        journal_master_key=ctx.journal_master_key,
                        event=JournalChatRedisPacketPassthrough(
                            counter=ctx.reserve_event_counter(),
                            type="passthrough",
                            event=message,
                        ),
                        now=event_at,
                    ),
                    now=int(event_at),
                )
                return

            is_final, message, message_client = item
            if is_final:
                seen_final = True

            replace_journal_entry_uid = await write_journal_entry_item(
                itgs,
                user_tz=user_tz,
                ctx=ctx,
                message=message,
                stats=stats,
                replace_journal_entry_uid=replace_journal_entry_uid,
                technique_parameters=technique_parameters,
            )
            chat_state = JournalChat(
                uid=ctx.journal_chat_uid, integrity="", data=[message_client]
            )
            chat_state.integrity = chat_state.compute_integrity()

            event_at = time.time()
            event = serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketMutations(
                    counter=ctx.reserve_event_counter(),
                    type="mutations",
                    mutations=[SegmentDataMutation(key=[], value=chat_state)],
                    more=not is_final,
                ),
                now=event_at,
            )

            if is_final:
                continue

            await publish_journal_chat_event(
                itgs, journal_chat_uid=ctx.journal_chat_uid, event=event
            )

        assert seen_final, "final message must be last"

        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=event,
            now=int(event_at),
        )
        stats.incr_system_chats_succeeded(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **technique_parameters,
            prompt_identifier=prompt_identifier,
        )
    finally:
        await stats.stats.store(itgs)


def break_paragraphs(text: str) -> List[str]:
    """Breaks the given text into paragraphs, removing empty lines and leading/trailing whitespace"""
    result = [p.strip() for p in text.split("\n")]
    return [p for p in result if p]


def get_message_from_text(
    text: str, /, *, display_author: Literal["self", "other"] = "other"
) -> Tuple[JournalEntryItemData, JournalEntryItemDataClient]:
    """Produces a journal entry item from the given text. Returns both the
    internal format and the client format.
    """
    return get_message_from_paragraphs(
        break_paragraphs(text), display_author=display_author
    )


def get_message_from_paragraphs(
    paragraphs: List[str], /, *, display_author: Literal["self", "other"] = "other"
) -> Tuple[JournalEntryItemData, JournalEntryItemDataClient]:
    """Produces a journal entry item from the given paragraphs. Returns both the
    internal format and the client format.
    """
    return JournalEntryItemData(
        type="chat",
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
        display_author="other",
    ), JournalEntryItemDataClient(
        type="chat",
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
        display_author="other",
    )


def extract_as_text(
    item: JournalEntryItemData,
) -> str:
    """Assuming that the given journal item contains only paragraphs, returns the
    text representation of the item.
    """
    assert item.type == "chat", "unknown item type"
    assert item.data.type == "textual", "unknown item data type"
    assert item.data.parts, "empty item"

    assert all(p.type == "paragraph" for p in item.data.parts), "unknown item part type"

    return "\n\n".join(p.value for p in item.data.parts if p.type == "paragraph")


async def publish_spinner(
    itgs: Itgs,
    /,
    *,
    ctx: JournalChatJobContext,
    message: str,
    detail: Optional[str] = None,
) -> None:
    """Publishes a journal chat passthrough event which has the client display a spinner.
    This isn't stored in the journal entry.
    """
    event_at = time.time()
    await publish_journal_chat_event(
        itgs,
        journal_chat_uid=ctx.journal_chat_uid,
        event=serialize_journal_chat_event(
            journal_master_key=ctx.journal_master_key,
            event=JournalChatRedisPacketPassthrough(
                counter=ctx.reserve_event_counter(),
                type="passthrough",
                event=EventBatchPacketDataItemDataThinkingSpinner(
                    type="thinking-spinner", message=message, detail=detail
                ),
            ),
            now=event_at,
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
    """Publishes a journal chat passthrough event which has the client display a progress bar.
    This isn't stored in the journal entry.
    """
    event_at = time.time()
    await publish_journal_chat_event(
        itgs,
        journal_chat_uid=ctx.journal_chat_uid,
        event=serialize_journal_chat_event(
            journal_master_key=ctx.journal_master_key,
            event=JournalChatRedisPacketPassthrough(
                counter=ctx.reserve_event_counter(),
                type="passthrough",
                event=EventBatchPacketDataItemDataThinkingBar(
                    type="thinking-bar", message=message, detail=detail, at=at, of=of
                ),
            ),
            now=event_at,
        ),
    )


async def write_journal_entry_item(
    itgs: Itgs,
    /,
    *,
    user_tz: pytz.BaseTzInfo,
    ctx: JournalChatJobContext,
    message: JournalEntryItemData,
    stats: JournalStats,
    technique_parameters: TechniqueParameters,
    replace_journal_entry_uid: Optional[str] = None,
) -> str:
    """Replaces or inserts a journal entry item. If replace_journal_entry_uid is None, a new
    journal entry item is inserted. Otherwise, the existing journal entry item is replaced.

    If an error occurs this closes out the journal chat (updating the stats appropriately)
    and raises an exception.
    """
    if replace_journal_entry_uid is None:
        return await _insert_journal_entry_item(
            itgs,
            user_tz=user_tz,
            ctx=ctx,
            message=message,
            stats=stats,
            technique_parameters=technique_parameters,
        )
    await _replace_journal_entry_item(
        itgs,
        user_tz=user_tz,
        ctx=ctx,
        message=message,
        replace_journal_entry_uid=replace_journal_entry_uid,
        stats=stats,
        technique_parameters=technique_parameters,
    )
    return replace_journal_entry_uid


async def _insert_journal_entry_item(
    itgs: Itgs,
    /,
    *,
    user_tz: pytz.BaseTzInfo,
    ctx: JournalChatJobContext,
    message: JournalEntryItemData,
    stats: JournalStats,
    technique_parameters: TechniqueParameters,
) -> str:
    conn = await itgs.conn()
    cursor = conn.cursor()

    created_at = time.time()
    created_unix_date_in_user_tz = unix_dates.unix_timestamp_to_unix_date(
        created_at, tz=user_tz
    )

    master_encrypted_data = ctx.journal_master_key.journal_master_key.encrypt(
        gzip.compress(
            message.__pydantic_serializer__.to_json(message),
            compresslevel=9,
            mtime=0,
        )
    ).decode("ascii")

    new_journal_entry_item_uid = f"oseh_jei_{secrets.token_urlsafe(16)}"
    new_journal_entry_counter = len(ctx.task.conversation) + 1
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
SELECT ?, journal_entries.id, ?, user_journal_master_keys.id, ?, ?, ?
FROM journal_entries, user_journal_master_keys
WHERE
journal_entries.uid = ?
AND user_journal_master_keys.uid = ?
                """,
                (
                    new_journal_entry_item_uid,
                    new_journal_entry_counter,
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
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **technique_parameters,
            prompt_identifier="insert_failed:journal_entry_not_found",
            category="internal",
        )
        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketPassthrough(
                    counter=ctx.reserve_event_counter(),
                    type="passthrough",
                    event=EventBatchPacketDataItemDataError(
                        type="error",
                        message="Failed to insert journal entry item",
                        detail="journal entry not found",
                    ),
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        raise ValueError(f"Journal entry {ctx.journal_entry_uid} not found")
    if not responses[1].results:
        assert (
            responses[2].rows_affected is None or responses[2].rows_affected < 1
        ), responses
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **technique_parameters,
            prompt_identifier="insert_failed:journal_master_key_not_found",
            category="encryption",
        )
        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketPassthrough(
                    counter=ctx.reserve_event_counter(),
                    type="passthrough",
                    event=EventBatchPacketDataItemDataError(
                        type="error",
                        message="Failed to insert journal entry item",
                        detail="internal encryption error",
                    ),
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        raise ValueError(
            f"Journal master key {ctx.journal_master_key.journal_master_key_uid} not found"
        )
    if responses[2].rows_affected is None or responses[2].rows_affected < 1:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **technique_parameters,
            prompt_identifier="insert_failed:unknown",
            category="internal",
        )
        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketPassthrough(
                    counter=ctx.reserve_event_counter(),
                    type="passthrough",
                    event=EventBatchPacketDataItemDataError(
                        type="error",
                        message="Failed to insert journal entry item",
                        detail="unknown",
                    ),
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        raise ValueError("Failed to insert new journal entry item")

    return new_journal_entry_item_uid


async def _replace_journal_entry_item(
    itgs: Itgs,
    /,
    *,
    user_tz: pytz.BaseTzInfo,
    ctx: JournalChatJobContext,
    message: JournalEntryItemData,
    replace_journal_entry_uid: str,
    stats: JournalStats,
    technique_parameters: TechniqueParameters,
) -> None:
    conn = await itgs.conn()
    cursor = conn.cursor()

    created_at = time.time()
    created_unix_date_in_user_tz = unix_dates.unix_timestamp_to_unix_date(
        created_at, tz=user_tz
    )

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
                (replace_journal_entry_uid,),
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
                    replace_journal_entry_uid,
                ),
            ),
        )
    )

    if not responses[0].results:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **technique_parameters,
            prompt_identifier="replace_failed:journal_entry_item_not_found",
            category="internal",
        )
        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketPassthrough(
                    counter=ctx.reserve_event_counter(),
                    type="passthrough",
                    event=EventBatchPacketDataItemDataError(
                        type="error",
                        message="Failed to replace journal entry item",
                        detail="journal entry item not found",
                    ),
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        raise ValueError(f"Journal entry item {replace_journal_entry_uid} not found")

    if not responses[1].results:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **technique_parameters,
            prompt_identifier="replace_failed:journal_master_key_not_found",
            category="encryption",
        )
        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketPassthrough(
                    counter=ctx.reserve_event_counter(),
                    type="passthrough",
                    event=EventBatchPacketDataItemDataError(
                        type="error",
                        message="Failed to replace journal entry item",
                        detail="internal encryption error",
                    ),
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        raise ValueError(
            f"Journal master key {ctx.journal_master_key.journal_master_key_uid} not found"
        )

    if responses[2].rows_affected is None or responses[2].rows_affected < 1:
        stats.incr_system_chats_failed_internal(
            unix_date=ctx.queued_at_unix_date_in_stats_tz,
            **technique_parameters,
            prompt_identifier="replace_failed:unknown",
            category="internal",
        )
        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketPassthrough(
                    counter=ctx.reserve_event_counter(),
                    type="passthrough",
                    event=EventBatchPacketDataItemDataError(
                        type="error",
                        message="Failed to replace journal entry item",
                        detail="unknown error",
                    ),
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        raise ValueError("Failed to replace journal entry item")


async def fail_internal(
    ctx: JournalChatJobContext,
    stats: JournalStats,
    technique_parameters: TechniqueParameters,
    part: str,
    message: str,
    detail: Optional[str] = None,
):
    """Records that we had some issue related to the LLM and returns the failure
    response to yield before returning.
    """
    stats.incr_system_chats_failed_llm(
        unix_date=ctx.queued_at_unix_date_in_stats_tz,
        **technique_parameters,
        prompt_identifier=part,
        category="llm",
        detail=make_id(detail) if detail is not None else make_id(message),
    )
    return None, EventBatchPacketDataItemDataError(
        type="error",
        message=message,
        detail=detail,
    )


async def fail_openai_exception(
    ctx: JournalChatJobContext,
    stats: JournalStats,
    technique_parameters: TechniqueParameters,
    exc: Exception,
    message: str = "Failed to connect with LLM",
    detail: Optional[str] = None,
):
    """Records that an error was raised on one of the openai calls and returns the
    failure response to yield before returning.
    """
    stats.incr_system_chats_failed_net_unknown(
        unix_date=ctx.queued_at_unix_date_in_stats_tz,
        **technique_parameters,
        prompt_identifier="chat",
        category="net",
        detail="unknown",
        error_name=type(exc).__name__,
    )
    return None, EventBatchPacketDataItemDataError(
        type="error",
        message=message,
        detail=detail,
    )


def make_id(v: str) -> str:
    """Simple function to take a safe string and convert it into something that
    is sort of an identifier
    """
    return "".join(
        c
        for c in v.replace(" ", "_").lower()
        if c in string.ascii_lowercase or c == "_"
    )
