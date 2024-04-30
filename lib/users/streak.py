import time
from typing import List, Literal, Optional, Sequence, Union, cast, overload
from pydantic import BaseModel, Field
from graceful_death import GracefulDeath
from lib.basic_redis_lock import basic_redis_lock
from lib.resources.patch.query import Query
import unix_dates
from itgs import Itgs
from lib.users.timezones import get_user_timezone
import logging
from rqdb.result import ResultItem
import pytz


DayOfWeek = Literal[
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
]
days_of_week: Sequence[DayOfWeek] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


class UserStreak(BaseModel):
    streak: int = Field(0, description="The streak of the user, in days", ge=0)
    days_of_week: List[DayOfWeek] = Field(
        default_factory=list,
        description="Which days this week the user has practiced, where weeks start on Monday",
    )
    goal_days_per_week: Optional[int] = Field(
        None,
        description="How many days per week the user wants to practice, if they've chosen",
        ge=1,
        le=7,
    )
    journeys: int = Field(
        0, description="how many journeys the user has taken in total"
    )
    prev_best_all_time_streak: int = Field(
        0,
        description="Excluding the users current streak, the longest streak the user has ever had",
        ge=0,
    )
    checked_at: int = Field(
        default_factory=lambda: int(time.time()),
        description="When this data was retrieved in seconds since the epoch - mostly for debugging",
    )


@overload
async def read_user_streak(
    itgs: Itgs,
    gd: GracefulDeath,
    /,
    *,
    sub: str,
    prefer: Literal["model"],
    user_tz: Optional[pytz.BaseTzInfo] = None,
) -> UserStreak: ...


@overload
async def read_user_streak(
    itgs: Itgs,
    gd: GracefulDeath,
    /,
    *,
    sub: str,
    prefer: Literal["bytes"] = "bytes",
    user_tz: Optional[pytz.BaseTzInfo] = None,
) -> bytes: ...


async def read_user_streak(
    itgs: Itgs,
    gd: GracefulDeath,
    /,
    *,
    sub: str,
    prefer: Literal["model", "bytes"] = "bytes",
    user_tz: Optional[pytz.BaseTzInfo] = None,
) -> Union[UserStreak, bytes]:
    """Reads the current streak for the user with the given sub. This
    returns the response either as the already json-encoded user streak or as
    the actual user streak model, based on `prefer`. By specifying this, we
    avoid unnecessary serialization/deserialization trips when the desired value
    is already available.

    Currently this uses a rather simplistic microcache implementation with a short
    duration with the understanding that these queries are relatively well
    optimized. If the db needs more protection, cache-busting mechanisms may be
    necessary.

    Args:
        itgs (Itgs): the integrations to (re)use
        sub (str): the sub of the user whose streak we are checking
        prefer ("model" or "bytes"): whether to return the response as the
            already json-encoded read streak response or as the actual read
            streak response model object
    """
    cache_key = f"users:{sub}:streak".encode("utf-8")

    redis = await itgs.redis()
    cached_value = await redis.get(cache_key)
    if cached_value is not None:
        if prefer == "model":
            return UserStreak.model_validate_json(cached_value)
        return cached_value

    # We encode/decode after releasing the lock
    raw_value: Optional[bytes] = None
    model_value: Optional[UserStreak] = None
    for _ in range(1):  # used so we can break out / goto
        async with basic_redis_lock(
            itgs, f"users:{sub}:streak:lock".encode("utf-8"), gd=gd, spin=True
        ):
            # recheck redis with the lock
            cached_value = await redis.get(cache_key)
            if cached_value is not None:
                raw_value = cached_value
                break

            if user_tz is None:
                user_tz = await get_user_timezone(itgs, user_sub=sub)

            result = UserStreak.model_validate({})
            unix_date_today = unix_dates.unix_timestamp_to_unix_date(
                result.checked_at, tz=user_tz
            )
            queries: List[Query] = [
                await _read_streak_query(
                    itgs, result=result, user_sub=sub, unix_date_today=unix_date_today
                ),
                await _read_days_of_week_query(
                    itgs, result=result, user_sub=sub, unix_date_today=unix_date_today
                ),
                await _read_goal_days_per_week_query(itgs, result=result, user_sub=sub),
                await _read_total_journeys_query(itgs, result=result, user_sub=sub),
                await _read_prev_best_streak_query(
                    itgs, result=result, user_sub=sub, unix_date_today=unix_date_today
                ),
            ]

            conn = await itgs.conn()
            cursor = conn.cursor("weak")
            response = await cursor.executeunified2(
                [q.sql for q in queries], [q.args for q in queries]
            )

            assert len(response) == len(queries)
            for item, query in zip(response, queries):
                await query.process_result(item)

            model_value = result
            raw_value = UserStreak.__pydantic_serializer__.to_json(result)
            await redis.set(cache_key, raw_value, ex=30)
            break

    assert raw_value is not None

    if prefer == "model":
        if model_value is None:
            return UserStreak.model_validate_json(raw_value)
        return model_value
    return raw_value


async def purge_user_streak_cache(itgs: Itgs, *, sub: str) -> None:
    """Purges the user streak cache for the user with the given sub"""
    logging.info(f"purging user streak cache for {sub=}")
    redis = await itgs.redis()
    redis.delete(f"users:{sub}:streak".encode("utf-8"))


async def _read_goal_days_per_week_query(
    itgs: Itgs, *, result: UserStreak, user_sub: str
) -> Query:
    """Creates a standard patch-like query to read the goal days per week for the
    user with the given sub and store the result in result.goal_days_per_week.
    """

    async def _process(res: ResultItem) -> None:
        if not res.results:
            result.goal_days_per_week = None
            return

        result.goal_days_per_week = res.results[0][0]

    return Query(
        sql="""
SELECT
    user_goals.days_per_week
FROM user_goals, users
WHERE
    user_goals.user_id = users.id
    AND users.sub = ?
""",
        args=[user_sub],
        process_result=_process,
    )


async def read_goal_days_per_week(itgs: Itgs, *, user_sub: str) -> Optional[int]:
    """Determines how many days per week the user wants to practice.

    Args:
        itgs (Itgs): The integrations to (re)use
        user_sub (str): The sub of the user whose goal we are checking

    Returns:
        int, None: The number of days per week the user wants to practice, or
            None if the user has not set a goal
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    res = UserStreak.model_validate({})
    query = await _read_goal_days_per_week_query(itgs, result=res, user_sub=user_sub)
    response = await cursor.execute(query.sql, query.args)
    await query.process_result(response)
    return res.goal_days_per_week


async def _read_days_of_week_query(
    itgs: Itgs, *, result: UserStreak, user_sub: str, unix_date_today: int
):
    """Reads the days of the week the user with the given sub has taken
    classes this week, storing the result in result.days_of_week, as
    a standard patch-like query
    """

    async def _process(response: ResultItem) -> None:
        if not response.results:
            result.days_of_week = []
        else:
            result.days_of_week = [
                name
                for (name, value) in zip(days_of_week, response.results[0])
                if value
            ]

    datetime_date_today = unix_dates.unix_date_to_date(unix_date_today)
    day_of_week_today = datetime_date_today.weekday()

    days_to_check = list(range(day_of_week_today + 1))

    query = "SELECT"
    qargs = []

    for day in days_to_check:
        days_before = day_of_week_today - day
        unix_date = unix_date_today - days_before

        if day != 0:
            query += ", "

        query += (
            """
    EXISTS (
        SELECT 1 FROM user_journeys
        WHERE
            user_journeys.user_id = users.id
            AND user_journeys.created_at_unix_date = ?
    )"""
            + f" AS b{day}"
        )

        qargs.append(unix_date)

    query += "\nFROM users WHERE users.sub = ?"
    qargs.append(user_sub)

    return Query(sql=query, args=qargs, process_result=_process)


async def read_days_of_week_from_db(
    itgs: Itgs, *, user_sub: str, unix_date_today: int
) -> List[DayOfWeek]:
    """Determines which days this week the user has practiced. The day corresponding
    to a particular class is determined at the time they took the class, based on their
    timezone at that point.

    Args:
        itgs (Itgs): The integrations to (re)use
        user_sub (str): The sub of the user whose streak we are calculating
        unix_date_today (int): The current unix date, from the users perspective
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    res = UserStreak.model_validate({})
    query = await _read_days_of_week_query(
        itgs, result=res, user_sub=user_sub, unix_date_today=unix_date_today
    )
    response = await cursor.execute(query.sql, query.args)
    await query.process_result(response)
    return res.days_of_week


async def _read_streak_query(
    itgs: Itgs, *, result: UserStreak, user_sub: str, unix_date_today: int
) -> Query:
    """Reads the users current streak and stores the result in the provided
    model, returning a standard patch-like query.
    """

    async def _process(response: ResultItem) -> None:
        assert response.results, response
        result.streak = response.results[0][0] - 1

    return Query(
        sql="""
WITH RECURSIVE events(unix_date) AS (
    VALUES(?)
    UNION ALL
    SELECT
        unix_date - 1
    FROM events
    WHERE
        EXISTS (
            SELECT 1 FROM user_journeys, users
            WHERE
                user_journeys.user_id = users.id
                AND users.sub = ?
                AND user_journeys.created_at_unix_date = unix_date
        )
)
SELECT COUNT(*) FROM events
""",
        args=[unix_date_today, user_sub],
        process_result=_process,
    )


async def read_streak_from_db(
    itgs: Itgs, *, user_sub: str, unix_date_today: int
) -> int:
    """Computes the users current streak for taking journeys. The day assigned
    to when a user took a particular journey is decided when that journey was
    taken, to account for differing timezones. However, the current date is
    based on the users currently assigned timezone and the current timestamp.

    Args:
        itgs (Itgs): The integrations to (re)use
        user_sub (str): The sub of the user whose streak we are calculating
        now_unix_date (int): The current unix date from the suers perspective

    Returns:
        int: The streak of the user, in days, non-negative
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    res = UserStreak.model_validate({})
    query = await _read_streak_query(
        itgs, result=res, user_sub=user_sub, unix_date_today=unix_date_today
    )
    response = await cursor.execute(query.sql, query.args)
    await query.process_result(response)
    return res.streak


async def _read_total_journeys_query(
    itgs: Itgs, *, result: UserStreak, user_sub: str
) -> Query:
    """Determines how many journeys the user with the given sub has taken
    and stores the result in the provided model, returning a standard patch-like query.
    """

    async def _process(response: ResultItem) -> None:
        assert response.results, response
        result.journeys = response.results[0][0]

    return Query(
        sql="SELECT COUNT(*) from user_journeys, users WHERE user_journeys.user_id = users.id AND users.sub = ?",
        args=[user_sub],
        process_result=_process,
    )


async def read_total_journeys_from_db(itgs: Itgs, *, user_sub: str) -> int:
    """Determines how many journeys the user with the given sub has taken"""
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    res = UserStreak.model_validate({})
    query = await _read_total_journeys_query(itgs, result=res, user_sub=user_sub)
    response = await cursor.execute(query.sql, query.args)
    await query.process_result(response)
    return res.journeys


async def _read_prev_best_streak_query(
    itgs: Itgs, *, result: UserStreak, user_sub: str, unix_date_today: int
) -> Query:
    """Determines the longest streak the user has ever had, excluding the current streak
    and stores the result in the provided model, returning a standard patch-like query.
    """

    async def _process(response: ResultItem) -> None:
        assert response.results, response

        max_streak = cast(Optional[int], response.results[0][0])
        if max_streak is None:
            result.prev_best_all_time_streak = 0
        else:
            result.prev_best_all_time_streak = max_streak

    return Query(
        sql="""
WITH RECURSIVE events(unix_date, streak) AS (
    SELECT
        user_journeys.created_at_unix_date,
        1
    FROM user_journeys, users
    WHERE
        user_journeys.user_id = users.id
        AND users.sub = ?
        AND user_journeys.created_at_unix_date < ?
        AND NOT EXISTS (
            SELECT 1 FROM user_journeys AS uj
            WHERE
                uj.user_id = (SELECT id FROM users WHERE users.sub = ?)
                AND uj.created_at_unix_date = user_journeys.created_at_unix_date + 1
        )
    UNION ALL
    SELECT
        unix_date - 1,
        streak + 1
    FROM events
    WHERE
        EXISTS (
            SELECT 1 FROM user_journeys AS uj
            WHERE
                uj.user_id = (SELECT id FROM users WHERE users.sub = ?)
                AND uj.created_at_unix_date = events.unix_date - 1
        )
)
SELECT MAX(streak) FROM events
        """,
        args=[user_sub, unix_date_today, user_sub, user_sub],
        process_result=_process,
    )


async def read_prev_best_streak_from_db(
    itgs: Itgs, *, user_sub: str, unix_date_today: int
) -> int:
    """Determines the longest streak the user has ever had, excluding the current streak

    Args:
        itgs (Itgs): The integrations to (re)use
        user_sub (str): The sub of the user whose streak we are calculating
        unix_date_today (int): The current unix date from the users perspective

    Returns:
        int: The length of the longest streak which has already ended strictly before
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    res = UserStreak.model_validate({})
    query = await _read_prev_best_streak_query(
        itgs, result=res, user_sub=user_sub, unix_date_today=unix_date_today
    )
    response = await cursor.execute(query.sql, query.args)
    await query.process_result(response)
    return res.prev_best_all_time_streak


if __name__ == "__main__":
    import asyncio

    gd = GracefulDeath()

    async def main():
        user_sub = input("enter a user sub: ")

        now = time.time()
        async with Itgs() as itgs:
            user_tz = await get_user_timezone(itgs, user_sub=user_sub)
            unix_date_today = unix_dates.unix_timestamp_to_unix_date(now, tz=user_tz)
            streak = await read_streak_from_db(
                itgs, user_sub=user_sub, unix_date_today=unix_date_today
            )
            days_of_week = await read_days_of_week_from_db(
                itgs, user_sub=user_sub, unix_date_today=unix_date_today
            )
            goal_days_per_week = await read_goal_days_per_week(itgs, user_sub=user_sub)
            journeys = await read_total_journeys_from_db(itgs, user_sub=user_sub)
            prev_best_all_time_streak = await read_prev_best_streak_from_db(
                itgs, user_sub=user_sub, unix_date_today=unix_date_today
            )
            joined = await read_user_streak(itgs, gd, sub=user_sub, prefer="model")

        print(f"{user_sub=} has a streak of {streak} days")
        print(f"{user_sub=} has practiced on {days_of_week} this week")
        print(f"{user_sub=} has a goal of {goal_days_per_week} days per week")
        print(f"{user_sub=} has taken {journeys} journeys")
        print(f"{user_sub=} has a prev best streak of {prev_best_all_time_streak}")
        print(f"{user_sub=} joined streak information, possibly from cache: {joined}")

    asyncio.run(main())
