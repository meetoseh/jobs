from enum import Enum
import pytz
import datetime


class TimeOfDay(str, Enum):
    morning = "morning"
    """Between 3am and noon"""

    afternoon = "afternoon"
    """Between noon and 5pm"""

    evening = "evening"
    """Between 5pm and 3am"""


def get_time_of_day(at: float, tz: pytz.BaseTzInfo) -> TimeOfDay:
    """Determines the time of day in the given timezone"""
    dt = datetime.datetime.fromtimestamp(at, pytz.utc).astimezone(tz)

    hour = dt.hour
    if 3 <= hour < 12:
        return TimeOfDay.morning
    elif 12 <= hour < 17:
        return TimeOfDay.afternoon
    else:
        return TimeOfDay.evening
