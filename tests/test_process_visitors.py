import logging
import time
from typing import Optional, Tuple
import unittest

import yaml
from lib.utms.parse import (
    UTM,
    get_canonical_utm_representation_from_wrapped as get_canon_utm,
)

try:
    import helper
except:
    import tests.helper
import main
import runners.visitors.process_visitor_users as process_visitor_users
import runners.visitors.process_visitor_utms as process_visitor_utms
import datetime
import asyncio
from itgs import Itgs
from redis.asyncio import Redis
from contextlib import asynccontextmanager
import secrets
from dataclasses import dataclass
import unix_dates
import pytz
import aiohttp
import os
import jwt


@dataclass
class Visitor:
    uid: str
    created_at: float


@dataclass
class User:
    sub: str
    created_at: float


tz = pytz.timezone("America/Los_Angeles")
default_utm = UTM(
    source="test",
    medium="robot",
    campaign="test-process-visitors",
)
second_utm = UTM(
    source="test",
    medium="robot",
    campaign="test-process-visitors-2",
)


def time_in_day(unix_date: float, *, hour: int, minute: int, pm: bool) -> float:
    return (
        unix_dates.unix_date_to_timestamp(unix_date, tz=tz)
        + hour * 3600
        + minute * 60
        + pm * 12 * 3600
    )


@asynccontextmanager
async def temp_visitor(itgs: Itgs, *, created_at: Optional[float] = None):
    """Acts as an async context manager for a visitor"""
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    uid = f"oseh_v_{secrets.token_urlsafe(16)}"
    created_at = created_at or time.time()
    await cursor.execute(
        """
        INSERT INTO visitors (
            uid, version, source, created_at
        )
        VALUES (?, ?, ?, ?)
        """,
        (uid, 1, "browser", created_at),
    )
    try:
        yield Visitor(uid, created_at)
    finally:
        await cursor.execute("DELETE FROM visitors WHERE uid = ?", (uid,))


@asynccontextmanager
async def temp_user(itgs: Itgs, *, created_at: Optional[float] = None):
    # we'll use the backend to make sure the user is properly initialized
    # and not in too unusual of a state
    root_backend_url = os.environ["ROOT_BACKEND_URL"]
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{root_backend_url}/api/1/dev/login",
            json={"sub": f"tes t_{secrets.token_urlsafe(16)}"},
        ) as response:
            response.raise_for_status()
            result = await response.json()
            id_token = result["id_token"]

            claims = jwt.decode(
                id_token,
                key=os.environ["OSEH_ID_TOKEN_SECRET"],
                algorithms=["HS256"],
                options={"require": ["sub"]},
                audience="oseh-id",
                issuer="oseh",
            )
            sub = claims["sub"]

        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        if created_at is not None:
            response = await cursor.execute(
                "UPDATE users SET created_at=? WHERE sub=?", (created_at, sub)
            )
            assert response.rows_affected is not None and response.rows_affected > 0
        else:
            response = await cursor.execute(
                "SELECT created_at FROM users WHERE sub=?", (sub,)
            )
            assert response.results
            created_at = response.results[0][0]

        try:
            yield User(sub, created_at)
        finally:
            async with session.delete(
                f"{root_backend_url}/api/1/users/me/account?force=1",
                headers={"authorization": f"bearer {id_token}"},
            ) as response:
                response.raise_for_status()


class FakeGracefulDeath:
    def __init__(self) -> None:
        self.received_term_signal = False


class Test(unittest.TestCase):
    def test_increments_visits(self):
        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)

                visits_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"visits",
                )
                visits = int(visits_raw) if visits_raw is not None else 0

                async with temp_visitor(itgs) as vis:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=vis.created_at + 15,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_utms.execute(itgs, FakeGracefulDeath())

                visits_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"visits",
                )
                visits2 = int(visits_raw) if visits_raw is not None else 0

                self.assertEqual(visits + 1, visits2)

        asyncio.run(_inner())

    def test_increments_holdover_preexisting(self):
        # 11:58 PM - user created
        # 11:59 PM - visitor created, clicked utm
        # 12:01 AM - user seen with visitor
        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)
                now = time_in_day(today, hour=0, minute=1, pm=False)

                holdover_preexisting_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"holdover_preexisting",
                )

                holdover_preexisting = (
                    int(holdover_preexisting_raw)
                    if holdover_preexisting_raw is not None
                    else 0
                )

                async with temp_user(itgs, created_at=now - 180) as user, temp_visitor(
                    itgs, created_at=now - 120
                ) as vis:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=vis.created_at,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_utms.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )
                    await redis.rpush(
                        b"visitors:user_associations",
                        process_visitor_users.QueuedVisitorUser(
                            visitor_uid=vis.uid,
                            user_sub=user.sub,
                            seen_at=now,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_users.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )

                holdover_preexisting_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"holdover_preexisting",
                )
                holdover_preexisting2 = (
                    int(holdover_preexisting_raw)
                    if holdover_preexisting_raw is not None
                    else 0
                )

                self.assertEqual(holdover_preexisting + 1, holdover_preexisting2)

        asyncio.run(_inner())

    def test_increments_holdover_last_click_signups(self):
        # 11:59 PM - visitor created, clicked utm
        # 12:01 AM - user created, seen with visitor
        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)
                now = time_in_day(today, hour=0, minute=1, pm=False)

                holdover_last_click_signups_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"holdover_last_click_signups",
                )

                holdover_last_click_signups = (
                    int(holdover_last_click_signups_raw)
                    if holdover_last_click_signups_raw is not None
                    else 0
                )

                async with temp_visitor(itgs, created_at=now - 120) as vis, temp_user(
                    itgs, created_at=now
                ) as user:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=vis.created_at,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_utms.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )
                    await redis.rpush(
                        b"visitors:user_associations",
                        process_visitor_users.QueuedVisitorUser(
                            visitor_uid=vis.uid,
                            user_sub=user.sub,
                            seen_at=now,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_users.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )

                holdover_last_click_signups_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"holdover_last_click_signups",
                )
                holdover_last_click_signups2 = (
                    int(holdover_last_click_signups_raw)
                    if holdover_last_click_signups_raw is not None
                    else 0
                )

                self.assertEqual(
                    holdover_last_click_signups + 1, holdover_last_click_signups2
                )

        asyncio.run(_inner())

    def test_increments_holdover_any_click_signups(self):
        # note: the last click also counts towards any click, so here we focus
        # on generating an any click that is not a last click

        # 11:58 AM - visitor created, clicked default utm
        # 11:59 AM - visitor clicked second utm
        # 12:01 AM - user created, seen with visitor
        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)
                now = time_in_day(today, hour=0, minute=1, pm=False)

                holdover_any_click_signups_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"holdover_any_click_signups",
                )

                holdover_any_click_signups = (
                    int(holdover_any_click_signups_raw)
                    if holdover_any_click_signups_raw is not None
                    else 0
                )

                async with temp_visitor(itgs, created_at=now - 180) as vis, temp_user(
                    itgs, created_at=now
                ) as user:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=now - 180,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=second_utm.source,
                            utm_medium=second_utm.medium,
                            utm_campaign=second_utm.campaign,
                            utm_content=second_utm.content,
                            utm_term=second_utm.term,
                            clicked_at=now - 120,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_utms.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )
                    await redis.rpush(
                        b"visitors:user_associations",
                        process_visitor_users.QueuedVisitorUser(
                            visitor_uid=vis.uid,
                            user_sub=user.sub,
                            seen_at=now,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_users.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )

                holdover_any_click_signups_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"holdover_any_click_signups",
                )
                holdover_any_click_signups2 = (
                    int(holdover_any_click_signups_raw)
                    if holdover_any_click_signups_raw is not None
                    else 0
                )

                self.assertEqual(
                    holdover_any_click_signups + 1, holdover_any_click_signups2
                )

        asyncio.run(_inner())

    def test_increments_preexisting(self):
        # 12:01 AM - visitor created
        # 12:02 AM - user created
        # 12:03 AM - visitor clicks utm
        # 12:04 AM - visitor seen with user

        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)
                now = time_in_day(today, hour=0, minute=4, pm=False)

                preexisting_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"preexisting",
                )

                preexisting = int(preexisting_raw) if preexisting_raw is not None else 0

                async with temp_visitor(itgs, created_at=now - 180) as vis, temp_user(
                    itgs, created_at=now - 120
                ) as user:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=now - 60,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_utms.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )
                    await redis.rpush(
                        b"visitors:user_associations",
                        process_visitor_users.QueuedVisitorUser(
                            visitor_uid=vis.uid,
                            user_sub=user.sub,
                            seen_at=now,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_users.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )

                preexisting_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"preexisting",
                )
                preexisting2 = (
                    int(preexisting_raw) if preexisting_raw is not None else 0
                )

                self.assertEqual(preexisting + 1, preexisting2)

        asyncio.run(_inner())

    def test_increments_last_click(self):
        # 12:01 AM - visitor created, clicks utm
        # 12:02 AM - user created, associated with visitor

        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)
                now = time_in_day(today, hour=0, minute=2, pm=False)

                last_click_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"last_click_signups",
                )

                last_click = int(last_click_raw) if last_click_raw is not None else 0

                async with temp_visitor(itgs, created_at=now - 60) as vis, temp_user(
                    itgs, created_at=now
                ) as user:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=now - 60,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_utms.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )
                    await redis.rpush(
                        b"visitors:user_associations",
                        process_visitor_users.QueuedVisitorUser(
                            visitor_uid=vis.uid,
                            user_sub=user.sub,
                            seen_at=now,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_users.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )

                last_click_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"last_click_signups",
                )
                last_click2 = int(last_click_raw) if last_click_raw is not None else 0

                self.assertEqual(last_click + 1, last_click2)

        asyncio.run(_inner())

    def test_increments_any_click(self):
        # 12:01 AM - visitor created, clicks default utm
        # 12:02 AM - visitor clicks second utm
        # 12:03 AM - user created, associated with visitor

        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)
                now = time_in_day(today, hour=0, minute=3, pm=False)

                any_click_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"any_click_signups",
                )

                any_click = int(any_click_raw) if any_click_raw is not None else 0

                async with temp_visitor(itgs, created_at=now - 120) as vis, temp_user(
                    itgs, created_at=now
                ) as user:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=now - 120,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=second_utm.source,
                            utm_medium=second_utm.medium,
                            utm_campaign=second_utm.campaign,
                            utm_content=second_utm.content,
                            utm_term=second_utm.term,
                            clicked_at=now - 60,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_utms.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )
                    await redis.rpush(
                        b"visitors:user_associations",
                        process_visitor_users.QueuedVisitorUser(
                            visitor_uid=vis.uid,
                            user_sub=user.sub,
                            seen_at=now,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await process_visitor_users.execute(
                        itgs, FakeGracefulDeath(), now=now
                    )

                any_click_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"any_click_signups",
                )
                any_click2 = int(any_click_raw) if any_click_raw is not None else 0

                self.assertEqual(any_click + 1, any_click2)

        asyncio.run(_inner())

    def test_raced_last_click(self):
        # 12:01 AM - visitor created, clicks utm
        # 12:02 AM - user created, associated with visitor
        async def _inner():
            async with Itgs() as itgs:
                redis = await itgs.redis()
                today = unix_dates.unix_date_today(tz=tz)
                now = time_in_day(today, hour=0, minute=2, pm=False)

                last_click_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"last_click_signups",
                )

                last_click = int(last_click_raw) if last_click_raw is not None else 0
                async with temp_visitor(itgs, created_at=now - 60) as vis, temp_user(
                    itgs, created_at=now
                ) as user:
                    await redis.rpush(
                        b"visitors:utms",
                        process_visitor_utms.QueuedVisitorUTM(
                            visitor_uid=vis.uid,
                            utm_source=default_utm.source,
                            utm_medium=default_utm.medium,
                            utm_campaign=default_utm.campaign,
                            utm_content=default_utm.content,
                            utm_term=default_utm.term,
                            clicked_at=now - 60,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    await redis.rpush(
                        b"visitors:user_associations",
                        process_visitor_users.QueuedVisitorUser(
                            visitor_uid=vis.uid,
                            user_sub=user.sub,
                            seen_at=now,
                        )
                        .json()
                        .encode("utf-8"),
                    )
                    task1 = asyncio.create_task(
                        process_visitor_utms.execute(
                            itgs, FakeGracefulDeath(), now=now, trigger_races=True
                        )
                    )
                    task2 = asyncio.create_task(
                        process_visitor_users.execute(
                            itgs, FakeGracefulDeath(), now=now, trigger_races=True
                        )
                    )
                    await asyncio.wait(
                        [task1, task2], return_when=asyncio.ALL_COMPLETED
                    )

                last_click_raw = await redis.hget(
                    f"stats:visitors:daily:{get_canon_utm(default_utm)}:{today}:counts".encode(
                        "utf-8"
                    ),
                    b"last_click_signups",
                )
                last_click2 = int(last_click_raw) if last_click_raw is not None else 0

                self.assertEqual(last_click + 1, last_click2)

        asyncio.run(_inner())


if __name__ == "__main__":
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)
    logging.config.dictConfig(logging_config)
    unittest.main()
