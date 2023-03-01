"""This module allows converting unix timestamps specified as seconds
from the unix epoch to the date in the format of an integer number
expressing the number of days since the unix epoch. This is a convenient
and unambiguous way to express dates when date comparisons are needed.

E.g, 100 days before 2022-03-01 is a complicated question to answer, without any
library, but 100 days before 19052 is 18952 - easy to answer.

Note this format does not indicate anything about timezones.
"""
import datetime
import pytz


def unix_timestamp_to_unix_date(
    unix_time: float, *, tz: datetime.timezone = datetime.timezone.utc
) -> int:
    """Converts the given unix timestamp to a unix date, i.e., converts
    the number of seconds since the unix epoch to the number of days
    since the unix epoch.

    Args:
        unix_time (float): The unix timestamp to convert
        tz (datetime.timezone, optional): The timezone for the returned
            date. If, for example, the unix time is 3AM UTC, then for PST (-8)
            the date will be the previous day. Defaults to UTC

    Returns:
        int: The unix date
    """
    return int(
        (
            unix_time
            + tz.utcoffset(
                datetime.datetime.utcfromtimestamp(unix_time)
            ).total_seconds()
        )
        // 86400
    )


def unix_timestamp_to_unix_month(
    unix_time: float, *, tz: datetime.timezone = datetime.timezone.utc
) -> int:
    """Converts the given unix timestamp to a unix month, i.e., converts
    the number of seconds since the unix epoch to the number of months
    since the unix epoch.

    The conversion to and from unix month is slightly more complicated
    than to unix dates due to months having different lengths, but it
    operates in conceptually the same way.

    Args:
        unix_time (float): The unix timestamp to convert
        tz (datetime.timezone, optional): The timezone for the returned
            date. If, for example, the unix time is 3AM UTC on the first
            of the month, then for PST (-8) the month will be the previous
            month. Defaults to UTC

    Returns:
        int: The unix month
    """
    as_unix_date = unix_timestamp_to_unix_date(unix_time, tz=tz)
    as_naive_datetime_date = unix_date_to_date(as_unix_date)

    year_offset = as_naive_datetime_date.year - 1970
    return year_offset * 12 + as_naive_datetime_date.month - 1


def unix_date_to_date(unix_date: int) -> datetime.date:
    """Converts the given unix date to a timezone-naive date object.

    Args:
        unix_date (int): The unix date to convert

    Returns:
        datetime.date: The date
    """
    midnight_utc = datetime.datetime.utcfromtimestamp(unix_date * 86400)
    return midnight_utc.date()


def date_to_unix_date(date: datetime.date) -> int:
    """Converts the given date to a unix date.

    Args:
        date (datetime.date): The date to convert

    Returns:
        int: The unix date
    """
    as_naive_datetime = datetime.datetime.combine(
        date, datetime.time(), tzinfo=pytz.utc
    )
    return int(as_naive_datetime.timestamp() // 86400)


def unix_month_to_date_of_first(unix_month: int) -> datetime.date:
    """Converts the given unix month to a timezone-naive date object
    representing the first day of the month.

    Args:
        unix_month (int): The unix month to convert

    Returns:
        datetime.date: The date
    """
    year = 1970 + unix_month // 12
    month = unix_month % 12 + 1
    return datetime.date(year, month, 1)
