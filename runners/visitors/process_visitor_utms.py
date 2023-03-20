from typing import Optional
from error_middleware import handle_contextless_error, handle_warning
from itgs import Itgs
from pydantic import BaseModel, Field
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from functools import lru_cache
from lib.stats.set_if_lower import (
    ensure_set_if_lower_script_exists,
    set_if_lower_unsafe,
)
import lib.visitors.deltas
import lib.utms.parse
import unix_dates
import pytz
import time

category = JobCategory.HIGH_RESOURCE_COST
# We mark this high resource since we don't want to block more urgent jobs on
# the low resource queue, plus this job can take a while to run.


class QueuedVisitorUTM(BaseModel):
    visitor_uid: str = Field(description="The unverified visitor's unique identifier")
    utm_source: str = Field()
    utm_medium: Optional[str] = Field(None)
    utm_campaign: Optional[str] = Field(None)
    utm_term: Optional[str] = Field(None)
    utm_content: Optional[str] = Field(None)
    clicked_at: float = Field(description="When we were told of the association")


def time_per(n: int, time_taken: float) -> float:
    """Returns the number of items per second given the number of items and the time taken"""
    return n / time_taken if time_taken != 0 else 0


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, limit: int = 5000, now: float = None
):
    """Processes queued visitor utm associations on the redis key
    `visitors:utms`. Any entries which are more than 1 hour old and
    are on the previous day are deleted without processing. Otherwise,
    processing includes the following potential side effects:

    - Inserting a row into `visitor_utms`
    - Setting the redis key `stats:visitors:daily:earliest`
    - Adding the utm to the redis key `stats:visitors:daily:{unix_date}:utms`
    - Incrementing any of the subkeys of the redis key
        `stats:visitors:daily:{utm}:{unix_date}:counts`

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        limit (int): the maximum number of associations to process
        now (float): The current time for processing, in seconds since the epoch. If
            not provided, the current time will be used. Generally only needed in unit
            tests.
    """
    if now is None:
        now = time.time()

    tz = pytz.timezone("America/Los_Angeles")
    current_unix_date = unix_dates.unix_date_today(tz=tz, now=now)
    cutoff_unix_time = min(
        now - 3600, unix_dates.unix_date_to_timestamp(current_unix_date, tz=tz)
    )

    started_at = time.perf_counter()
    last_print_at = started_at

    # this is a bit awkward of a way to do it, but seems to be the only way
    # to expose the built-in lru cache. The value is an empty list until used, at which
    # point we append 0 to it to make it truthy.
    @lru_cache(maxsize=128)
    def is_visitor_recent(visitor_uid: str) -> list:
        return []

    redis = await itgs.redis()
    for i in range(limit):
        if gd.received_term_signal:
            logging.debug(
                f"process_visitor_utms interrupted by signal before starting iteration {i+1}/{limit}"
            )
            break

        if i > 0 and time.perf_counter() - last_print_at > 1:
            last_print_at = time.perf_counter()
            time_taken = last_print_at - started_at
            logging.debug(
                f"process_visitor_utms processed {i} of a max of {limit} associations in {time_taken:.2f}s: "
                f"{time_per(i, time_taken):.2f} associations/s"
            )

        next_raw_entry = await redis.lpop(b"visitors:utms")
        if next_raw_entry is None:
            time_taken = time.perf_counter() - started_at
            logging.debug(
                f"process_visitor_utms finished: processed {i} of a max of {limit} associations in {time_taken:.2f}s: "
                f"{time_per(i, time_taken):.2f} associations/s"
            )
            break

        next_entry = QueuedVisitorUTM.parse_raw(
            next_raw_entry, content_type="application/json"
        )

        if next_entry.clicked_at < cutoff_unix_time:
            await handle_warning(
                f"{__name__}:cutoff",
                f"Skipping visitor utm association {next_entry.json()}: too old (are we getting behind?)",
            )
            continue

        utm = lib.visitors.deltas.UTM(
            source=next_entry.utm_source,
            medium=next_entry.utm_medium,
            campaign=next_entry.utm_campaign,
            term=next_entry.utm_term,
            content=next_entry.utm_content,
        )

        recent_list = is_visitor_recent(next_entry.visitor_uid)
        is_recent = not not recent_list
        if is_recent:
            recent_list.append(0)

        consistency = "weak" if is_recent else "none"
        state = await lib.visitors.deltas.get_visitor_utm_state(
            itgs,
            visitor_uid=next_entry.visitor_uid,
            consistency=consistency,
            retry_on_fail="weak",
        )

        if state is None:
            await handle_contextless_error(
                extra_info=(
                    f"process_visitor_utms: failed to get visitor state for {next_entry.visitor_uid}"
                )
            )
            # We can change this to continue if we see this happening and we know it's
            # harmless; it is possible for this to be harmless if the visitor was actually
            # deleted
            break

        success = await lib.visitors.deltas.try_insert_visitor_utm(
            itgs, state=state, utm=utm, clicked_at=next_entry.clicked_at
        )

        if not success and consistency == "none":
            consistency = "weak"
            state = await lib.visitors.deltas.get_visitor_utm_state(
                itgs,
                visitor_uid=next_entry.visitor_uid,
                consistency=consistency,
                retry_on_fail="weak",
            )
            if state is None:
                await handle_contextless_error(
                    extra_info=f"process_visitor_utms: after raced, failed to get visitor state for {next_entry.visitor_uid}"
                )
                # We can change this to continue if we see this happening and we know it's
                # harmless; it is possible for this to be harmless if the visitor was actually
                # deleted
                break
            success = await lib.visitors.deltas.try_insert_visitor_utm(
                itgs, state=state, utm=utm, clicked_at=next_entry.clicked_at
            )
            if success:
                logging.debug(
                    "process_visitor_utms: raced at none consistency, but inserted at weak consistency"
                )

        if not success:
            await handle_contextless_error(
                extra_info=f"process_visitor_utms: failed to insert visitor utm (raced?): {next_entry.json()} (requeuing)"
            )
            await redis.rpush(b"visitors:utms", next_raw_entry)
            # we probably always want to break here since we want ~1 of these jobs running at most
            # at a time to not fight each other
            break

        changes = lib.visitors.deltas.compute_changes_from_visitor_utm(
            state, utm, next_entry.clicked_at
        )

        clicked_at_unix_date = unix_dates.unix_timestamp_to_unix_date(
            next_entry.clicked_at, tz=tz
        )
        canonical_utm = lib.utms.parse.get_canonical_utm_representation_from_wrapped(
            utm
        )
        get_counts_key = lambda utm: f"stats:visitors:daily:{lib.utms.parse.get_canonical_utm_representation_from_wrapped(utm)}:{clicked_at_unix_date}:counts".encode(
            "utf-8"
        )
        await ensure_set_if_lower_script_exists(redis)
        async with redis.pipeline() as pipe:
            pipe.multi()

            await set_if_lower_unsafe(
                pipe,
                b"stats:visitors:daily:earliest",
                str(clicked_at_unix_date).encode("ascii"),
            )

            await pipe.sadd(
                f"stats:visitors:daily:{clicked_at_unix_date}:utms".encode("ascii"),
                canonical_utm.encode("utf-8"),
            )
            await pipe.hincrby(get_counts_key(utm), b"visits", 1)

            for last_click_removed in changes.last_clicks_changed_to_any_click:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        last_click_removed.clicked_at, tz=tz
                    )
                    < clicked_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(last_click_removed.utm),
                    b"holdover_last_click_signups"
                    if is_holdover
                    else b"last_click_signups",
                    -1,
                )

            for last_click_added in changes.new_last_click:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        last_click_added.clicked_at, tz=tz
                    )
                    < clicked_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(last_click_added.utm),
                    b"holdover_last_click_signups"
                    if is_holdover
                    else b"last_click_signups",
                    1,
                )

            for any_click_added in changes.new_any_click:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        any_click_added.clicked_at, tz=tz
                    )
                    < clicked_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(any_click_added.utm),
                    b"holdover_any_click_signups"
                    if is_holdover
                    else b"any_click_signups",
                    1,
                )

            for preexisting_added in changes.new_preexisting:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        preexisting_added.clicked_at, tz=tz
                    )
                    < clicked_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(preexisting_added.utm),
                    b"holdover_preexisting" if is_holdover else b"preexisting",
                    1,
                )

            await pipe.execute()
