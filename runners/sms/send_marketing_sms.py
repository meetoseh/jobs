"""Sends an SMS channel touch to all users with a phone number which receives
daily reminders
"""

import os
from typing import Optional, cast

import pytz
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory
from lib.daily_reminders.stats import DailyReminderStatsPreparer
from lib.shared.job_callback import JobCallback
from lib.touch.links import abandon_link, create_buffered_link
from lib.touch.send import (
    encode_touch,
    initialize_touch,
    prepare_send_touch,
    send_touch_in_pipe,
)
from redis_helpers.run_with_prep import run_with_prep
import unix_dates

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, touch_point_event_slug: str):
    """Sends a marketing SMS to users which have a phone number receiving daily
    reminders using the given touch point with a `url` parameter.

    This can also be used as the basis for more complicated SMS which, for
    example, are dynamic based on user properties.

    This requires that a touch point has been initialized. See the backend
    `touch_points.md` database documentation file, or e.g. migration 143 for how
    to initialize these touch points.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        touch_point_event_slug (str): the slug of the touch point to send
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    redis = await itgs.redis()

    root_frontend_url = os.environ["ROOT_FRONTEND_URL"]
    if root_frontend_url.startswith("https://"):
        root_frontend_url = root_frontend_url[len("https://") :]

    total_sms = 0
    last_sub: Optional[str] = None
    while True:
        response = await cursor.execute(
            "SELECT "
            " users.sub "
            "FROM users "
            "WHERE"
            " EXISTS ("
            "  SELECT 1 FROM user_phone_numbers"
            "  WHERE"
            "   user_phone_numbers.verified"
            "   AND user_phone_numbers.receives_notifications"
            "   AND NOT EXISTS ("
            "    SELECT 1 FROM suppressed_phone_numbers"
            "    WHERE"
            "      suppressed_phone_numbers.phone_number = user_phone_numbers.phone_number"
            "   )"
            "   AND users.id = user_phone_numbers.user_id"
            " )"
            " AND EXISTS ("
            "  SELECT 1 FROM user_daily_reminders"
            "  WHERE"
            "   user_daily_reminders.user_id = users.id"
            "   AND user_daily_reminders.channel = 'sms'"
            "   AND user_daily_reminders.day_of_week_mask <> 0"
            " )"
            " AND (? IS NULL OR users.sub > ?) "
            "ORDER BY users.sub "
            "LIMIT 100",
            (last_sub, last_sub),
        )

        if not response.results:
            logging.info(f"Finished sending {total_sms} SMS messages")
            break

        logging.info(f"Sending SMS touch to a batch of {len(response.results)} users")

        redis_stats = DailyReminderStatsPreparer()
        try:
            for row in response.results:
                user_sub = cast(str, row[0])

                logging.info(f"Sending SMS touch to {user_sub}")
                success_callback_codes = []
                failure_callback_codes = []
                touch = initialize_touch(
                    user_sub=user_sub,
                    touch_point_event_slug=touch_point_event_slug,
                    channel="sms",
                    event_parameters={},
                    success_callback=JobCallback(
                        name="runners.touch.persist_links",
                        kwargs={"codes": success_callback_codes},
                    ),
                    failure_callback=JobCallback(
                        name="runners.touch.abandon_links",
                        kwargs={"codes": failure_callback_codes},
                    ),
                )

                link = await create_buffered_link(
                    itgs,
                    touch_uid=touch.uid,
                    page_identifier="home",
                    page_extra={},
                    preview_identifier="default",
                    preview_extra={},
                    now=touch.queued_at,
                    code_style="short",
                )
                success_callback_codes.append(link.code)
                failure_callback_codes.append(link.code)
                touch.event_parameters["url"] = f"{root_frontend_url}/a/{link.code}"

                enc_touch = encode_touch(touch)

                async def prep(force: bool):
                    await prepare_send_touch(redis, force=force)

                async def func():
                    return await send_touch_in_pipe(redis, touch, enc_touch)

                result = await run_with_prep(prep, func)
                if not result:
                    await handle_warning(
                        f"{__name__}:backpressure",
                        f"canceling remaining send of {touch_point_event_slug} due to backpressure",
                    )
                    await abandon_link(itgs, code=link.code)
                    return

                unix_date = unix_dates.unix_timestamp_to_unix_date(
                    touch.queued_at, tz=pytz.timezone("America/Los_Angeles")
                )
                redis_stats.incr_sends_attempted(unix_date)
                redis_stats.incr_links(unix_date)
                redis_stats.incr_sent(unix_date, channel="sms")
                total_sms += 1
        finally:
            await redis_stats.store(itgs)

        last_sub = cast(str, response.results[-1][0])


if __name__ == "__main__":
    import asyncio

    async def main():
        touch_point_event_slug = input("Touch point event slug: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.sms.send_marketing_sms",
                touch_point_event_slug=touch_point_event_slug,
            )

    asyncio.run(main())
