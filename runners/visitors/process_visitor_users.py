import asyncio
from typing import Optional, Union, cast
from error_middleware import handle_contextless_error, handle_warning
from itgs import Itgs
from pydantic import BaseModel, Field
from graceful_death import GracefulDeath
import logging
import time
from jobs import JobCategory
from functools import lru_cache
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
import lib.visitors.deltas
import lib.visitors.interests
import lib.utms.parse
import unix_dates
import pytz

category = JobCategory.HIGH_RESOURCE_COST
# We mark this high resource since we don't want to block more urgent jobs on
# the low resource queue, plus this job can take a while to run.


class QueuedVisitorUser(BaseModel):
    visitor_uid: str = Field(description="The unverified visitor's unique identifier")
    user_sub: str = Field(description="The verified user sub")
    seen_at: float = Field(description="When we were told of the association")


def time_per(n: int, time_taken: float) -> float:
    """Returns the number of items per second given the number of items and the time taken"""
    return n / time_taken if time_taken != 0 else 0


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    limit: int = 5000,
    now: Optional[float] = None,
    trigger_races: bool = False,
):
    """Processes queued visitor user associations on the redis key
    `visitors:user_associations`. Any entries which are more than 1 hour old and
    are on the previous day are deleted without processing. Otherwise,
    processing includes the following potential side effects:

    - Inserting a row into `visitor_users`, or updating the `last_seen_at` column in a row in `visitor_users`
    - Setting the redis key `stats:visitors:daily:earliest`
    - Incrementing any of the subkeys of the redis key
        `stats:visitors:daily:{utm}:{unix_date}:counts`

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        limit (int): the maximum number of associations to process
        now (float or None): The current time, primarily for unit tests, otherwise None to
            get the current time from `time.time()`
        trigger_races (bool): Primarily for unit tests. Defaults to False. If true, we will
            sleep for 1 second at the worst possible time, making it extremely likely that
            race conditions will be triggered if this is being run concurrently with another
            process_visitor_* job.
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
    def _is_visitor_recent_raw(visitor_uid: str) -> list:
        return []

    def is_visitor_recent(visitor_uid: str) -> bool:
        res = _is_visitor_recent_raw(visitor_uid)
        if res:
            return True
        res.append(0)
        return False

    @lru_cache(maxsize=128)
    def _is_user_recent_raw(user_sub: str) -> list:
        return []

    def is_user_recent(user_sub: str) -> bool:
        res = _is_user_recent_raw(user_sub)
        if res:
            return True
        res.append(0)
        return False

    redis = await itgs.redis()
    for i in range(limit):
        if gd.received_term_signal:
            logging.debug(
                f"process_visitor_users interrupted by signal before starting iteration {i+1}/{limit}"
            )
            break

        if i > 0 and time.perf_counter() - last_print_at > 1:
            last_print_at = time.perf_counter()
            time_taken = last_print_at - started_at
            logging.debug(
                f"process_visitor_users processed {i} of a max of {limit} associations in {time_taken:.2f}s: "
                f"{time_per(i, time_taken):.2f} associations/s"
            )

        next_raw_entry = cast(
            Union[str, bytes, None],
            await redis.lpop(b"visitors:user_associations"),  # type: ignore
        )
        if next_raw_entry is None:
            time_taken = time.perf_counter() - started_at
            logging.debug(
                f"process_visitor_users finished: processed {i} of a max of {limit} associations in {time_taken:.2f}s: "
                f"{time_per(i, time_taken):.2f} associations/s"
            )
            break

        next_entry = QueuedVisitorUser.model_validate_json(next_raw_entry)

        if next_entry.seen_at < cutoff_unix_time:
            await handle_warning(
                f"{__name__}:cutoff",
                f"Skipping visitor user association {next_entry.model_dump_json()}: too old (are we getting behind?)",
            )
            continue

        is_recent = is_visitor_recent(next_entry.visitor_uid) or is_user_recent(
            next_entry.user_sub
        )

        consistency = "weak" if is_recent else "none"
        state = await lib.visitors.deltas.get_visitor_user_state(
            itgs,
            user_sub=next_entry.user_sub,
            visitor_uid=next_entry.visitor_uid,
            consistency=consistency,
            retry_on_fail="weak",
        )
        if trigger_races:
            await asyncio.sleep(1)

        if state is None:
            # The user was deleted
            continue

        success = await lib.visitors.deltas.try_upsert_visitor_user(
            itgs,
            state=state,
            visitor_uid=next_entry.visitor_uid,
            user_sub=next_entry.user_sub,
            seen_at=next_entry.seen_at,
        )

        if success == "failed" and consistency == "none":
            consistency = "weak"
            state = await lib.visitors.deltas.get_visitor_user_state(
                itgs,
                user_sub=next_entry.user_sub,
                visitor_uid=next_entry.visitor_uid,
                consistency=consistency,
                retry_on_fail="weak",
            )
            if trigger_races:
                await asyncio.sleep(1)
            if state is None:
                await handle_contextless_error(
                    extra_info=f"process_visitor_users: after raced, failed to get state for visitor {next_entry.visitor_uid} and user {next_entry.user_sub}"
                )
                # We can change this to continue if we see this happening and we know it's
                # harmless; it is possible for this to be harmless if the visitor was actually
                # deleted
                break
            success = await lib.visitors.deltas.try_upsert_visitor_user(
                itgs,
                state=state,
                visitor_uid=next_entry.visitor_uid,
                user_sub=next_entry.user_sub,
                seen_at=next_entry.seen_at,
            )
            if success != "failed":
                logging.debug(
                    "process_visitor_users: raced at none consistency, but inserted at weak consistency"
                )

        if success == "failed":
            await handle_contextless_error(
                extra_info=f"process_visitor_users: failed to insert visitor user (raced?): {next_entry.model_dump_json()} (requeuing)"
            )
            await redis.rpush(b"visitors:user_associations", next_raw_entry)  # type: ignore
            continue

        if success == "updated":
            continue

        assert success == "inserted"

        changes = lib.visitors.deltas.compute_changes_from_visitor_user(state)

        seen_at_unix_date = unix_dates.unix_timestamp_to_unix_date(
            next_entry.seen_at, tz=tz
        )
        get_counts_key = lambda utm: f"stats:visitors:daily:{lib.utms.parse.get_canonical_utm_representation_from_wrapped(utm)}:{seen_at_unix_date}:counts".encode(
            "utf-8"
        )
        await ensure_set_if_lower_script_exists(redis)
        async with redis.pipeline() as pipe:
            pipe.multi()

            await set_if_lower(
                pipe,
                b"stats:visitors:daily:earliest",
                str(seen_at_unix_date).encode("ascii"),  # type: ignore
            )

            for last_click_removed in changes.last_clicks_changed_to_any_click:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        last_click_removed.clicked_at, tz=tz
                    )
                    < seen_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(last_click_removed.utm),  # type: ignore
                    b"holdover_last_click_signups"  # type: ignore
                    if is_holdover
                    else b"last_click_signups",
                    -1,
                )

            for last_click_added in changes.new_last_clicks:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        last_click_added.clicked_at, tz=tz
                    )
                    < seen_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(last_click_added.utm),  # type: ignore
                    b"holdover_last_click_signups"  # type: ignore
                    if is_holdover
                    else b"last_click_signups",
                    1,
                )

            for any_click_added in changes.new_any_clicks:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        any_click_added.clicked_at, tz=tz
                    )
                    < seen_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(any_click_added.utm),  # type: ignore
                    b"holdover_any_click_signups"  # type: ignore
                    if is_holdover
                    else b"any_click_signups",
                    1,
                )

            for preexisting_added in changes.new_preexisting:
                is_holdover = (
                    unix_dates.unix_timestamp_to_unix_date(
                        preexisting_added.clicked_at, tz=tz
                    )
                    < seen_at_unix_date
                )
                await pipe.hincrby(
                    get_counts_key(preexisting_added.utm),  # type: ignore
                    b"holdover_preexisting" if is_holdover else b"preexisting",  # type: ignore
                    1,
                )

            await pipe.execute()

        await lib.visitors.interests.copy_interests_from_visitor_to_user(
            itgs, visitor_uid=next_entry.visitor_uid, user_sub=next_entry.user_sub
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.visitors.process_visitor_users")

    asyncio.run(main())
