from itgs import Itgs
from lib.journals.conversation_stream import JournalChatJobConversationStream
from journal_chat_jobs.lib.data_to_client import data_to_client
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext
from lib.journals.journal_chat import JournalChat
from lib.journals.journal_chat_redis_packet import (
    SegmentDataMutation,
)

import journal_chat_jobs.lib.chat_helper as chat_helper


async def handle_sync(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """Very simple pipeline which just streams the items within the journal entry
    back to the client. This allows the client to treat "journal entry is being
    created/updated" the same as "journal entry is already created" in various
    screens (e.g., opening a journal chat), which makes preloading much simpler.
    """
    stream = JournalChatJobConversationStream(
        journal_entry_uid=ctx.journal_entry_uid,
        user_sub=ctx.user_sub,
        pending_moderation="ignore",
    )
    await stream.start()

    chat_state = JournalChat(uid=ctx.journal_chat_uid, integrity="", data=[])
    try:
        while True:
            item = await stream.load_next_item(timeout=5)
            if item.type == "timeout":
                await chat_helper.publish_error_and_close_out(
                    itgs,
                    ctx=ctx,
                    warning_id=f"{__name__}:timeout",
                    stat_id="timeout",
                    warning_message=f"Failed to load item for sync for `{ctx.user_sub}`",
                    client_message="Failed to load chat",
                    client_detail="timeout",
                )
                return
            if item.type == "error":
                await chat_helper.publish_error_and_close_out(
                    itgs,
                    ctx=ctx,
                    warning_id=f"{__name__}:error",
                    stat_id="error",
                    warning_message=f"Failed to load item for sync for `{ctx.user_sub}`",
                    client_message="Failed to load chat",
                    client_detail="error",
                )
                return
            if item.type == "finished":
                await chat_helper.publish_mutations(
                    itgs, ctx=ctx, final=True, mutations=[]
                )
                ctx.stats.incr_completed(
                    requested_at_unix_date=ctx.queued_at_unix_date_in_stats_tz,
                    type=ctx.type,
                )
                return

            assert item.type == "item"
            chat_state.data.append(
                await data_to_client(itgs, ctx=ctx, item=item.item.data)
            )
            chat_state.integrity = chat_state.compute_integrity()
            await chat_helper.publish_mutations(
                itgs,
                ctx=ctx,
                final=False,
                mutations=[
                    SegmentDataMutation(key=["integrity"], value=chat_state.integrity),
                    SegmentDataMutation(
                        key=["data", len(chat_state.data) - 1],
                        value=chat_state.data[-1],
                    ),
                ],
            )
    except Exception as e:
        await stream.cancel()
        await chat_helper.publish_error_and_close_out(
            itgs,
            ctx=ctx,
            warning_id=f"{__name__}:exception",
            stat_id="exception",
            warning_message=f"Failed to load item for sync for `{ctx.user_sub}`",
            client_message="Failed to load chat",
            client_detail="exception",
            exc=e,
        )
