from typing import cast
from itgs import Itgs
from journal_chat_jobs.lib.journal_chat_job_context import JournalChatJobContext

import journal_chat_jobs.lib.chat_helper as chat_helper
from lib.users.time_of_day import get_time_of_day
import unix_dates


async def handle_greeting(itgs: Itgs, ctx: JournalChatJobContext) -> None:
    """We expect to have this greeting be based on things they've wrote
    previously, but for now this is a very basic fixed message
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    unix_date_for_user = unix_dates.unix_timestamp_to_unix_date(
        ctx.queued_at, tz=ctx.user_tz
    )
    day_of_week_for_user = unix_dates.unix_date_to_date(unix_date_for_user).strftime(
        "%A"
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

    if given_name is None:
        message = "Hi! How are you feeling today? 😊"
    elif unix_date_for_user == 19945:
        message = f"Happy {day_of_week_for_user} and good {time_of_day_for_user} {given_name}—what's on your mind?"
    elif unix_date_for_user == 19946:
        message = f"So happy to see you this {time_of_day_for_user}, {given_name}! How are you?\n\nAs an optional exercise, try to include a rhyme in your response. 😊"
    elif unix_date_for_user == 19947:
        message = f"Hi {given_name}! Is there something you can let go of today? 🎈"
    else:
        message = f"Hi {given_name}, how are you feeling today? 😊"

    data = chat_helper.get_message_from_text(message)
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
