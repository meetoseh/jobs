"""Helper module for working with links within the touch system. In particular,
if you want to provide a call-to-action for the user to be directed somewhere,
this generates a code which the frontend can exchange for information about where
to direct the user. This allows for a small code despite a potentially intricate
action, which means short urls in e.g., SMS (which have a character limit) and
QR codes (which are easier to scan with less data), or not having to worry about
push notification data limits.

A side effect of this system is that when the frontend exchanges the code we can
track that the user took the call-to-action, which is often an important metric
for determining if the notification was useful.
"""
import json
from itgs import Itgs
from typing import Any, Dict, Literal, Optional
import time
from lib.touch.link_info import TouchLink
from lib.touch.link_stats import LinkStatsPreparer
from redis_helpers.run_with_prep import run_with_prep
from redis_helpers.set_if_lower import ensure_set_if_lower_script_exists, set_if_lower
import secrets
from redis_helpers.touch_click_try_abandon import (
    ensure_touch_click_try_abandon_script_exists,
    touch_click_try_abandon,
)
from redis_helpers.touch_click_try_create import touch_click_try_create
from redis_helpers.touch_click_try_persist import (
    ensure_touch_click_try_persist_script_exists,
    touch_click_try_persist,
)
from redis_helpers.touch_link_try_create import (
    ensure_touch_link_try_create_script_exists,
    touch_link_try_create,
)
import unix_dates
import pytz

tz = pytz.timezone("America/Los_Angeles")


async def create_buffered_link(
    itgs: Itgs,
    *,
    touch_uid: str,
    page_identifier: str,
    page_extra: Dict[str, Any],
    preview_identifier: str,
    preview_extra: Dict[str, Any],
    now: Optional[float] = None,
    code_style: Literal["short", "normal", "long"] = "normal",
) -> TouchLink:
    """Creates a link which is immediately usable but not yet persisted to the
    database. The callee is responsible for either abandoning or persisting the
    link via `abandon_link` or `persist_link` respectively.

    When a short code is used, this code will be guessable in a potentially
    trivial amount of time. If a short code is not used, the code is long
    enough that it is not feasible to guess it in less than a week. If
    a long code is used the code is long enough that it is not feasible to
    guess it within a decade.

    Args:
        itgs (Itgs): the integrations to (re)use
        touch_uid (str): the uid of the touch this link will be sent in. this
            touch must be persisted to the database before the link can be
            persisted
        page_identifier (str): the identifier for the page the user will be
            directed to. see the documentation for the `user_touch_links`
            table for the available values
        page_extra (dict[str, any]): the extra information for the page;
            the structure depends on the page identifier
        preview_identifier (str): the identifier for how the open graph
            meta tags should be constructed on the standard url for the
            generated code, which will impact what the user sees as a
            preview in applications that support open graph meta tags
            (e.g., most sms applications). see the documentation for the
            `user_touch_links` table for the available values
        preview_extra (dict[str, any]): the extra information for the
            preview; the structure depends on the preview identifier
        now (float, optional): the time at which the link was created;
            defaults to the current time
        code_style ('short', 'normal', 'long'): Determines what strategy
            is used to select the code. A short code will require rejection
            sampling (greatly slowing down this process as we have to check with
            the database) in order to get a very short code. A normal or long
            code can be assumed to be unique without checking, but a long code
            is long enough to act as a secret for all but the most sensitive of
            applications. Defaults to normal, which is the fastest to generate

            ex short code: 'enxF'
            ex normal code: 'oH6EBTs7Ok9Oj4RCzCALiA'
            ex long code: '6W6IjKQf40DV5hOk8Auipfu4qy9aa0VLCW4mY4GeMfKgcNc2-tXLRtuXesK9Y4cM4hGZM574ZW8CIvgcjNu9iQ'

    Returns:
        TouchLink: the immediately usable touch link that needs to be
            persisted or abandoned once it's feasible to do so
    """
    if now is None:
        now = time.time()

    if code_style == "short":
        return await _create_buffered_link_using_rejection_sampling(
            itgs,
            touch_uid=touch_uid,
            page_identifier=page_identifier,
            page_extra=page_extra,
            preview_identifier=preview_identifier,
            preview_extra=preview_extra,
            now=now,
        )

    return await _create_buffered_link_assuming_no_collisions(
        itgs,
        touch_uid=touch_uid,
        page_identifier=page_identifier,
        page_extra=page_extra,
        preview_identifier=preview_identifier,
        preview_extra=preview_extra,
        now=now,
        code_length=16 if code_style == "normal" else 64,
    )


PERSIST_LINK_DELAY = 60 * 30
"""How much longer we wait after knowing a link can be persisted before
it is actually persisted. This improves performance of click tracking
under the fairly light assumption that a large number of clicks happen
shortly after a link is sent, followed by a long tail of clicks over
time. 
"""


async def persist_link(itgs: Itgs, *, code: str, now: Optional[float] = None) -> bool:
    """Queues the touch link with the given code to be persisted if it's
    in the Buffered Link sorted set and not already in the Persistable
    Buffered Link sorted set. This will delay persisting for a bit to
    improve performance of click tracking and ensure there is no racing
    the touch insert.

    Args:
        itgs (Itgs): the integrations to (re)use
        code (str): the code of the touch link to persist
        now (float, None): the current time, in unix seconds since the unix epoch,
            or None for the current system time

    Returns:
        bool: True if the link was actually queued to be persisted, False
            if it's either not in the buffer or already queued to be persisted
    """
    if now is None:
        now = time.time()

    redis = await itgs.redis()
    result = await run_with_prep(
        lambda force: ensure_touch_click_try_persist_script_exists(redis, force=force),
        lambda: touch_click_try_persist(
            redis, score=now + PERSIST_LINK_DELAY, code=code
        ),
    )

    if result.persist_queued:
        unix_date = unix_dates.unix_timestamp_to_unix_date(
            result.link.created_at, tz=tz
        )
        await (
            LinkStatsPreparer()
            .incr_touch_links(unix_date=unix_date, event="persist_queue_attempts")
            .incr_touch_links(
                unix_date=unix_date,
                event="persists_queued",
                event_extra=result.link.page_identifier.encode("utf-8"),
            )
            .store()
        )
        return True

    unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)
    await (
        LinkStatsPreparer()
        .incr_touch_links(unix_date=unix_date, event="persist_queue_attempts")
        .incr_touch_links(
            unix_date=unix_date,
            event="persist_queue_failed",
            event_extra=result.failure_reason.encode("utf-8"),
        )
        .store()
    )
    return False


async def abandon_link(itgs: Itgs, *, code: str, now: Optional[float] = None) -> bool:
    """Immediately abandons the touch link with the given code if it is in the
    buffered link sorted set, removing it from the persistable buffered link
    sorted set if it's there.

    Does nothing if the code is either not in the buffered link sorted set or is
    already in the persistable buffered link purgatory (i.e., it's being
    persisted right now)

    Args:
        itgs (Itgs): the integrations to (re)use
        code (str): the code of the touch link to abandon
        now (float, None): the current time, in unix seconds since the unix epoch,
            or None for the current system time

    Returns:
        bool: True if the link was actually abandoned, False if it's either not
            in the buffer or already being persisted
    """
    if now is None:
        now = time.time()

    redis = await itgs.redis()
    result = await run_with_prep(
        lambda force: ensure_touch_click_try_abandon_script_exists(redis, force=force),
        lambda: touch_click_try_abandon(redis, code=code.encode("utf-8")),
    )

    if result.abandoned_link is not None:
        link_created_at = result.abandoned_link.created_at
        link_unix_date = unix_dates.unix_timestamp_to_unix_date(link_created_at, tz=tz)
        await LinkStatsPreparer().incr_touch_links(
            unix_date=link_unix_date, event="abandons_attempted"
        ).incr_touch_links(
            unix_date=link_unix_date,
            event="abandoned",
            event_extra=f"{result.abandoned_link.page_identifier}:{result.number_of_clicks}".encode(
                "utf-8"
            ),
        ).store()
        return True

    unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)
    await (
        LinkStatsPreparer()
        .incr_touch_links(unix_date=unix_date, event="abandons_attempted")
        .incr_touch_links(
            unix_date=unix_date,
            event="abandon_failed",
            event_extra=result.failure_reason.encode("utf-8"),
        )
        .store()
    )
    return False


async def click_link(
    itgs: Itgs,
    *,
    code: str,
    visitor_uid: Optional[str],
    user_sub: Optional[str],
    track_type: Literal["on_click", "post_login"],
    parent_uid: Optional[str],
    clicked_at: Optional[float],
    should_track: bool,
    now: Optional[float] = None,
) -> Optional[TouchLink]:
    """Fetches the link with the given code and optionally tracks the click
    at the same time. This will check the buffered link sorted set and
    the database for the link information.

    When tracking the link, the link may be tracked via the Buffered Link Clicks
    pseudo-set, or the Delayed Click Links sorted set, or directly via the database,
    depending on where the link is located.

    Args:
        itgs (Itgs): the integrations to (re)use
        code (str): the code of the touch link which was clicked
        visitor_uid (str, None): the visitor uid the client provided, if any
        user_sub (str, None): the user that clicked, if known. this differs
            from who we sent the code if the user may have shared the link with
            someone else, or someone just guessed the code
        track_type (str): the type of track that was sent to the server
        parent_uid (str, None): iff track_type is post_login, the uid of the
            original on_click event this is augmenting
        clicked_at (float, None): the time the click was received by the server,
            or None for the current time
        should_track (bool): True to track the click if the link is found. Note
            that `post_login` tracks are discarded unless the parent `on_click`
            event can be located, which means the caller only needs to ratelimit
            `on_click` events to limit the number of stored clicks.
        now (float, None): the current time, in unix seconds since the unix epoch,
            or None for the current time

    Returns:
        TouchLink, None: the link that has the same code, if it could be found,
            otherwise None.
    """
    click_uid = f"oseh_utlc_{secrets.token_urlsafe(16)}"
    if now is None:
        now = time.time()

    click_unix_date = unix_dates.unix_timestamp_to_unix_date(clicked_at, tz=tz)
    now_unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)

    assert track_type in ("on_click", "post_login")
    if track_type == "on_click":
        assert parent_uid is None
    else:
        assert parent_uid is not None

    redis = await itgs.redis()

    buffer_result = await run_with_prep(
        lambda force: ensure_set_if_lower_script_exists(redis, force=force),
        lambda: touch_click_try_create(
            redis,
            code=code,
            visitor_uid=visitor_uid,
            user_sub=user_sub,
            track_type=track_type,
            parent_uid=parent_uid,
            clicked_at=clicked_at,
            click_uid=click_uid,
            now=now,
            should_track=should_track,
        ),
    )

    if buffer_result.link is not None:
        if should_track:
            stats = LinkStatsPreparer()
            stats.incr_touch_links(
                unix_date=click_unix_date if buffer_result.tracked else now_unix_date,
                event="click_attempts",
            )

            if buffer_result.tracked:
                stats.incr_touch_links(
                    unix_date=click_unix_date,
                    event="clicks_buffered"
                    if buffer_result.tracked_in_buffer
                    else "clicks_delayed",
                    event_extra=f"{track_type}:{buffer_result.link.page_identifier}:vis={visitor_uid is not None}:user={user_sub is not None}".encode(
                        "utf-8"
                    ),
                )
            elif buffer_result.failed_to_track_reason == "parent_has_child":
                stats.incr_touch_links(
                    unix_date=now_unix_date,
                    event="clicks_failed",
                    event_extra=f"post_login:{buffer_result.link.page_identifier}:redis:parent_has_child".encode(
                        "utf-8"
                    ),
                )
            elif buffer_result.failed_to_track_reason == "no_parent":
                stats.incr_touch_links(
                    unix_date=now_unix_date,
                    event="clicks_failed",
                    event_extra=f"post_login:{buffer_result.link.page_identifier}:parent_not_found".encode(
                        "utf-8"
                    ),
                )
            else:
                stats.incr_touch_links(
                    unix_date=now_unix_date,
                    event="clicks_failed",
                    event_extra=f"other:{buffer_result.failed_to_track_reason}".encode(
                        "utf-8"
                    ),
                )
            await stats.store(itgs)
        return buffer_result.link

    conn = await itgs.conn()
    cursor = conn.cursor()

    for attempt in range(2):
        response = await cursor.execute(
            """
            SELECT
                user_touch_links.uid,
                user_touches.uid,
                user_touch_links.code,
                user_touch_links.page_identifier,
                user_touch_links.page_extra,
                user_touch_links.preview_identifier,
                user_touch_links.preview_extra,
                user_touches.created_at
            FROM user_touch_links
            JOIN user_touches ON user_touches.id = user_touch_links.user_touch_id
            WHERE user_touch_links.code=?
            """,
            (code,),
            read_consistency="none" if attempt == 0 else "strong",
        )

        if response.results:
            break

    if not response.results:
        return None

    row = response.results[0]
    link = TouchLink(
        uid=row[0],
        code=row[2],
        touch_uid=row[1],
        page_identifier=row[3],
        page_extra=json.loads(row[4]),
        preview_identifier=row[5],
        preview_extra=json.loads(row[6]),
        created_at=row[7],
    )

    if not should_track:
        return link

    tracked_in_db = False
    parent_has_child_in_db = False
    if (
        not buffer_result.tracked
        and buffer_result.failed_to_track_reason != "parent_has_child"
    ):
        if track_type == "on_click":
            response = await cursor.execute(
                """
                INSERT INTO user_touch_link_clicks (
                    uid, user_touch_link_id, track_type, parent_id, user_id,
                    visitor_id, parent_known, user_known, visitor_known, child_known,
                    clicked_at, created_at
                )
                SELECT
                    ?, user_touch_links.id, ?, NULL, users.id,
                    visitors.id, 0, ?, ?, 0, ?, ?
                FROM user_touch_links
                LEFT JOIN users ON users.sub = ?
                LEFT JOIN visitors ON visitors.uid = ?
                WHERE user_touch_links.code=?
                """,
                (
                    click_uid,
                    track_type,
                    int(user_sub is not None),
                    int(visitor_uid is not None),
                    clicked_at,
                    now,
                    user_sub,
                    visitor_uid,
                    code,
                ),
            )
            assert response.rows_affected in (None, 1), f"{response.rows_affected=}"
            tracked_in_db = response.rows_affected == 1
        else:
            response = await cursor.executemany3(
                (
                    (
                        """
                        INSERT INTO user_touch_link_clicks (
                            uid, user_touch_link_id, track_type, parent_id, user_id,
                            visitor_id, parent_known, user_known, visitor_known, child_known,
                            clicked_at, created_at
                        )
                        SELECT
                            ?, user_touch_links.id, ?, parents.id, users.id, visitors.id,
                            1, ?, ?, 0, ?, ?
                        FROM user_touch_links
                        JOIN user_touch_link_clicks AS parents ON (
                            parents.uid = ? AND parents.child_known = 0
                        )
                        WHERE user_touch_links.code = ?
                        """,
                        (
                            click_uid,
                            track_type,
                            int(user_sub is not None),
                            int(visitor_uid is not None),
                            clicked_at,
                            now,
                            parent_uid,
                            code,
                        ),
                    ),
                ),
                (
                    "UPDATE user_touch_link_clicks SET child_known = 1 WHERE uid = ?",
                    (parent_uid,),
                ),
            )
            assert response[0].rows_affected in (
                None,
                1,
            ), f"{response[0].rows_affected=}"
            assert response[1].rows_affected in (
                None,
                1,
            ), f"{response[1].rows_affected=}"
            tracked_in_db = response[0].rows_affected == 1
            assert not tracked_in_db or response[1].rows_affected == 1
            parent_has_child_in_db = (
                not tracked_in_db and response[1].rows_affected == 1
            )

    stats = LinkStatsPreparer()
    stats.incr_touch_links(
        unix_date=click_unix_date if buffer_result.tracked else now_unix_date,
        event="click_attempts",
    )
    if buffer_result.tracked:
        assert buffer_result.tracked_in_delayed
        stats.incr_touch_links(
            unix_date=click_unix_date,
            event="clicks_delayed",
            event_extra=f"{track_type}:{link.page_identifier}:vis={visitor_uid is not None}:user={user_sub is not None}".encode(
                "utf-8"
            ),
        )
    elif buffer_result.failed_to_track_reason == "parent_has_child":
        stats.incr_touch_links(
            unix_date=now_unix_date,
            event="clicks_failed",
            event_extra=f"post_login:{link.page_identifier}:redis:parent_has_child".encode(
                "utf-8"
            ),
        )
    elif tracked_in_db:
        stats.incr_touch_links(
            unix_date=now_unix_date,
            event="clicks_direct_to_db",
            event_extra=f"{track_type}:{link.page_identifier}:vis={visitor_uid is not None}:user={user_sub is not None}".encode(
                "utf-8"
            ),
        )
    elif track_type == "on_click":
        stats.incr_touch_links(
            unix_date=now_unix_date,
            event="clicks_failed",
            event_extra=b"other:link_deleted_or_implementation_error",
        )
    elif track_type == "post_login":
        if parent_has_child_in_db:
            stats.incr_touch_links(
                unix_date=now_unix_date,
                event="clicks_failed",
                event_extra=f"post_login:{link.page_identifier}:db:parent_has_child".encode(
                    "utf-8"
                ),
            )
        else:
            stats.incr_touch_links(
                unix_date=now_unix_date,
                event="clicks_failed",
                event_extra=f"post_login:{link.page_identifier}:parent_not_found".encode(
                    "utf-8"
                ),
            )
    else:
        stats.incr_touch_links(
            unix_date=now_unix_date,
            event="clicks_failed",
            event_extra=b"other:implementation_error:db_if_chain",
        )

    await stats.store(itgs)
    return link


async def _create_buffered_link_using_rejection_sampling(
    itgs: Itgs,
    touch_uid: str,
    page_identifier: str,
    page_extra: Dict[str, Any],
    preview_identifier: str,
    preview_extra: Dict[str, Any],
    now: float,
) -> TouchLink:
    # Look at the no collisions version first to understand the goal; we do
    # the same thing when there isn't a collision.
    #
    # there are two spots we could get a collision; the database (an older link)
    # or redis (a recent link), where the database is much more likely. however, we
    # can't check both and store atomically, so we tentatively check the database,
    # check-and-store in redis, then double check the database and on a
    # collision we delete in redis and retry.
    #
    # this will increment the stats atomically when storing since having a collision
    # on the second database check is unlikely, and will skip it on retries
    #
    # in the event this function is interrupted, it is possible the value is in redis
    # despite a collision in the database. this will result in a leak eventually being
    # picked up by the leaked link detection job and it detecting it as a duplicate,
    # deleting it, which is what we would want to happen anyway.

    conn = await itgs.conn()
    cursor = conn.cursor()
    redis = await itgs.redis()

    uid = f"oseh_utl_{secrets.token_urlsafe(16)}"
    unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)

    stats_key = f"stats:touch_links:daily:{unix_date}".encode("ascii")

    encoded_link_kwargs: Dict[str, bytes] = {
        "uid": uid.encode("ascii"),
        "touch_uid": touch_uid.encode("ascii"),
        "page_identifier": page_identifier.encode("ascii"),
        "page_extra": json.dumps(page_extra).encode("ascii"),
        "preview_identifier": preview_identifier.encode("ascii"),
        "preview_extra": json.dumps(preview_extra).encode("ascii"),
        "created_at": str(now).encode("ascii"),
    }

    already_incremented_stats = False

    while True:
        code = _generate_short_code()

        response = await cursor.execute(
            "SELECT 1 FROM user_touch_links WHERE code=? LIMIT 1",
            (code,),
            read_consistency="none",
        )

        if response.results:
            _on_short_code_collision()
            continue

        async def _prep(force: bool):
            await ensure_touch_link_try_create_script_exists(redis, force=force)

        async def _func():
            return await touch_link_try_create(
                redis,
                buffer_key=b"touch_links:buffer",
                stats_key=stats_key,
                stats_earliest_key=b"stats:touch_links:daily:earliest",
                **encoded_link_kwargs,
                already_incremented_stats=already_incremented_stats,
                unix_date=unix_date,
            )

        redis_success = await run_with_prep(_prep, _func)

        if not redis_success:
            _on_short_code_collision()
            continue

        already_incremented_stats = True

        response = await cursor.execute(
            "SELECT 1 FROM user_touch_links WHERE code=? LIMIT 1",
            (code,),
            read_consistency="strong",
        )

        if response.results:
            async with redis.pipeline() as pipe:
                pipe.multi()
                await pipe.zrem(b"touch_links:buffer", code.encode("ascii"))
                await pipe.delete(f"touch_links:buffer:{code}".encode("ascii"))
                await pipe.execute()
            _on_short_code_collision()
            continue

        _on_short_code_valid()
        return TouchLink(
            uid=uid,
            code=code,
            touch_uid=touch_uid,
            page_identifier=page_identifier,
            page_extra=page_extra,
            preview_identifier=preview_identifier,
            preview_extra=preview_extra,
            created=now,
        )


async def _create_buffered_link_assuming_no_collisions(
    itgs: Itgs,
    touch_uid: str,
    page_identifier: str,
    page_extra: Dict[str, Any],
    preview_identifier: str,
    preview_extra: Dict[str, Any],
    now: float,
    code_length: int,
) -> TouchLink:
    uid = f"oseh_utl_{secrets.token_urlsafe(16)}"
    code = secrets.token_urlsafe(code_length)
    unix_date = unix_dates.unix_timestamp_to_unix_date(now, tz=tz)

    link = TouchLink(
        uid=uid,
        code=code,
        touch_uid=touch_uid,
        page_identifier=page_identifier,
        page_extra=page_extra,
        preview_identifier=preview_identifier,
        preview_extra=preview_extra,
        created=now,
    )

    redis = await itgs.redis()

    async def _prep(force: bool):
        await ensure_set_if_lower_script_exists(redis, force=force)

    async def _func():
        async with redis.pipeline() as pipe:
            pipe.multi()
            await set_if_lower(pipe, b"stats:touch_links:daily:earliest", unix_date)
            await pipe.zadd(b"touch_links:buffer", mapping={code.encode("ascii"): now})
            await pipe.hset(
                f"touch_links:buffer:{code}".encode("ascii"),
                mapping=link.as_redis_mapping(),
            )
            await pipe.hincrby(
                f"stats:touch_links:daily:{unix_date}".encode("ascii"), b"created"
            )
            await pipe.execute()

    await run_with_prep(_prep, _func)


_shortest_code_bytes: int = 3
_shortest_code_collisions: int = 0


def _generate_short_code() -> str:
    """Generates the shortest code that we haven't seen a streak of collisions for
    since this process started (starting at 3 bytes / 4 characters). If this
    results in a collision, call _on_short_code_collision and try again. If this
    doesn't result in a collision, call _on_short_code_valid to reset the streak
    (if any). This leads to rapidly identifying the shortest length that has an
    acceptable collision rate with no need for a central store or coordinating.

    This also has the positive self-balancing effect where the more codes we are
    generating per instance, the lower the acceptable collision rate (i.e., the
    faster the codes are generated) (assuming relatively consistent instance
    restarts regardless of # of codes generated)
    """
    return secrets.token_urlsafe(_shortest_code_bytes)


def _on_short_code_collision() -> None:
    global _shortest_code_bytes
    global _shortest_code_collisions

    assert _shortest_code_bytes < 16, f"{_shortest_code_bytes=}"
    # something has definitely gone wrong if we get collisions with 16 bytes

    _shortest_code_collisions += 1

    # for a streak of N, the collision rate that results in a 50% chance of
    # increasing the length is found as follows:
    #
    # P(N heads in N tosses with P=r) = 0.5
    # r^N = 0.5
    # r = 0.5^(1/N)

    # choosing a streak of 5 -> ~87% chance of collision before a 50% chance
    # of incrementing the length on every attempt

    # can also be easier to interpret when solving for the 1% chance of incrementing
    # the length on every attempt, for which:
    #
    # r = 0.01^(1/N)
    # streak of 5 -> ~40% chance of collision before 1% chance of incrementing
    if _shortest_code_collisions > 5:
        _shortest_code_bytes += 1
        _shortest_code_collisions = 0


def _on_short_code_valid() -> None:
    global _shortest_code_collisions

    _shortest_code_collisions = 0
