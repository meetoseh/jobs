import secrets
from typing import Literal, cast
from dataclasses import dataclass
from itgs import Itgs
import logging
import pytz


TimezoneTechniqueSlug = Literal["browser", "app", "app-guessed"]


@dataclass
class TimezoneLogDataFromUser:
    style: Literal["browser", "app", "input"]
    guessed: bool = False


def convert_timezone_technique_slug_to_db(
    timezone_technique: TimezoneTechniqueSlug,
) -> TimezoneLogDataFromUser:
    """Converts the given timezone technique slug, as specified in requests,
    to the value that we store in the database. We disambiguate the combined
    terms to make processing simpler.
    """
    if timezone_technique == "app-guessed":
        return TimezoneLogDataFromUser(style="app", guessed=True)
    elif timezone_technique == "app":
        return TimezoneLogDataFromUser(style="app")
    else:
        assert timezone_technique == "browser", timezone_technique
        return TimezoneLogDataFromUser(style="browser")


async def need_set_timezone(itgs: Itgs, *, user_sub: str, timezone: str) -> bool:
    """Returns True if the users timezone value should be updated to the provided
    timezone and false if the set can be skipped as we know it will be a no-op
    """
    redis = await itgs.redis()
    cache_key = f"user:timezones:{user_sub}".encode("utf-8")
    encoded_timezone = timezone.encode("utf-8")

    old = await redis.set(cache_key, encoded_timezone, ex=15 * 60, get=True)
    return old != encoded_timezone


async def get_user_timezone(itgs: Itgs, *, user_sub: str) -> pytz.BaseTzInfo:
    """Gets the currently assumed timezone for the user with the given
    sub.

    It's often faster to include this with some other request, either
    via pipelining the cache key or just fetching from the user_timezones
    table when a query is already being made, so this is primarily intended
    as a compatibility function for lesser-used endpoints that didn't
    originally need a timezone but whose dependencies now require it.
    """
    req_id = secrets.token_urlsafe(4)

    logging.debug(f"Getting timezone for user {user_sub}, assigned {req_id=}")
    cache_key = f"user:timezones:{user_sub}".encode("utf-8")

    redis = await itgs.redis()
    resp = await redis.get(cache_key)
    if resp is not None:
        assert isinstance(resp, bytes), resp
        res = resp.decode("utf-8")
        logging.info(f"{req_id=} CACHE HIT -> {res=}")
        try:
            return pytz.timezone(res)
        except pytz.UnknownTimeZoneError:
            logging.warning(
                f"{req_id=} Unknown timezone {res=} in cache, using default"
            )
            return pytz.timezone("America/Los_Angeles")

    logging.info(f"{req_id=} CACHE MISS")
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        "SELECT timezone FROM users WHERE sub=?",
        (user_sub,),
    )
    if not response.results or response.results[0][0] is None:
        logging.info(f"{req_id=} DB MISS")
        return pytz.timezone("America/Los_Angeles")

    tz = cast(str, response.results[0][0])
    await redis.set(cache_key, tz.encode("utf-8"), nx=True, ex=15 * 60)

    logging.info(f"{req_id=} DB HIT -> {tz=}")
    try:
        return pytz.timezone(tz)
    except pytz.UnknownTimeZoneError:
        logging.warning(f"{req_id=} Unknown timezone {tz=} in DB, using default")
        return pytz.timezone("America/Los_Angeles")
