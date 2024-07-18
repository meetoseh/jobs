import secrets
import time
from typing import Optional, cast
from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from lib.journals.journal_chat import JournalChat
from lib.journals.journal_chat_redis_packet import (
    EventBatchPacketDataItemDataError,
    JournalChatRedisPacketMutations,
    JournalChatRedisPacketPassthrough,
    SegmentDataMutation,
)
from lib.journals.journal_entry_item_data import (
    JournalEntryItemDataClient,
    JournalEntryItemDataDataTextualClient,
    JournalEntryItemTextualPartParagraph,
)
import gzip

from lib.journals.serialize_journal_chat_event import serialize_journal_chat_event
from lib.redis_stats_preparer import RedisStatsPreparer
from lib.users.timezones import get_user_timezone
from lib.journals.journal_stats import JournalStats
from redis_helpers.journal_chat_job_finish import safe_journal_chat_job_finish
import unix_dates


TECHNIQUE = "fixed-1"


async def handle_greeting(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """We expect to have this greeting be based on things they've wrote
    previously, but for now this is a very basic fixed message
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
            stats.incr_greetings_failed(
                unix_date=ctx.queued_at_unix_date_in_stats_tz,
                technique=TECHNIQUE,
                reason="user_not_found",
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
                            message="Failed to create greeting",
                            detail="user not found",
                        ),
                    ),
                    now=event_at,
                ),
                now=int(event_at),
            )
            raise ValueError(f"User {ctx.user_sub} not found")

        if not responses[1].results:
            stats.incr_greetings_failed(
                unix_date=ctx.queued_at_unix_date_in_stats_tz,
                technique=TECHNIQUE,
                reason="journal_entry_not_found",
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
                            message="Failed to create greeting",
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
                stats.incr_greetings_failed(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    technique=TECHNIQUE,
                    reason="replace_index_journal_entry_item_not_found",
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
                                message="Failed to create greeting",
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

        user_tz = await get_user_timezone(itgs, user_sub=ctx.user_sub)
        created_at = time.time()
        created_unix_date_in_user_tz = unix_dates.unix_timestamp_to_unix_date(
            created_at, tz=user_tz
        )

        if given_name is None or "anon" in given_name.lower():
            message = JournalEntryItemDataClient(
                type="chat",
                data=JournalEntryItemDataDataTextualClient(
                    type="textual",
                    parts=[
                        JournalEntryItemTextualPartParagraph(
                            type="paragraph", value="Hi! How are you feeling today? 😊"
                        )
                    ],
                ),
                display_author="other",
            )
        else:
            message = JournalEntryItemDataClient(
                type="chat",
                data=JournalEntryItemDataDataTextualClient(
                    type="textual",
                    parts=[
                        JournalEntryItemTextualPartParagraph(
                            type="paragraph",
                            value=f"Hi {given_name}, how are you feeling today? 😊",
                        )
                    ],
                ),
                display_author="other",
            )

        master_encrypted_data = ctx.journal_master_key.journal_master_key.encrypt(
            gzip.compress(
                message.__pydantic_serializer__.to_json(message),
                compresslevel=9,
                mtime=0,
            )
        ).decode("ascii")

        if replace_journal_entry_uid is None:
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
                stats.incr_greetings_failed(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    technique=TECHNIQUE,
                    reason="insert_failed:journal_entry_not_found",
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
                stats.incr_greetings_failed(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    technique=TECHNIQUE,
                    reason="insert_failed:journal_master_key_not_found",
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
                stats.incr_greetings_failed(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    technique=TECHNIQUE,
                    reason="insert_failed:unknown",
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
        else:
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
                stats.incr_greetings_failed(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    technique=TECHNIQUE,
                    reason="replace_failed:journal_entry_item_not_found",
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
                raise ValueError(
                    f"Journal entry item {replace_journal_entry_uid} not found"
                )

            if not responses[1].results:
                stats.incr_greetings_failed(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    technique=TECHNIQUE,
                    reason="replace_failed:journal_master_key_not_found",
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
                stats.incr_greetings_failed(
                    unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    technique=TECHNIQUE,
                    reason="replace_failed:unknown",
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

        chat_state = JournalChat(uid=ctx.journal_chat_uid, integrity="", data=[message])
        chat_state.integrity = chat_state.compute_integrity()

        event_at = time.time()
        await safe_journal_chat_job_finish(
            itgs,
            journal_chat_uid=ctx.journal_chat_uid.encode("utf-8"),
            log_id=ctx.log_id.encode("utf-8"),
            last_event=serialize_journal_chat_event(
                journal_master_key=ctx.journal_master_key,
                event=JournalChatRedisPacketMutations(
                    counter=ctx.reserve_event_counter(),
                    type="mutations",
                    mutations=[SegmentDataMutation(key=[], value=chat_state)],
                    more=False,
                ),
                now=event_at,
            ),
            now=int(event_at),
        )
        stats.incr_greetings_succeeded(
            unix_date=ctx.queued_at_unix_date_in_stats_tz, technique=TECHNIQUE
        )
    finally:
        await stats.stats.store(itgs)
