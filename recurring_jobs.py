"""Entry point for regularly scheduling jobs on a cron-like schedule.
This is done in a coordinated way, such that the performance on each
machine should tend toward O(N/M) where N is the number of recurring
jobs and M is the number of instances.

The following constraints are imposed:

- Ability to schedule a job with the same flexibility of a
  cron job (i.e., repeat hourly, every other week on monday, etc), since
  recurring jobs are used for all sorts of reasons
- Forced to specify timezones, since manually converting to UTC is error-prone.
- Regardless of the uptime of recurring_jobs, no job should be scheduled
  more frequently than the period (i.e., jobs do not "pile up")
- Concurrently running instances of recurring_jobs, especially in different
  processes & servers, should at worst not effect each other's performance
  (i.e., correct behavior and no destructive interference).
- Recurring jobs should be specified in the code and be easy to change, such
  that as soon as all instances of recurring_jobs are updated, deleted jobs
  are no longer scheduled and new jobs are scheduled as appropriate. No leaks
  should be possible (such as unremoved redis keys)
- For the purposes of this module, "running" a job is the same as queueing it
  on the hot queue. Jobs should not be inspected or modified once they are on
  the hot queue.
- There should not be any surprising performance characteristics of job intervals,
  i.e., the time and memory required to process the recurring jobs list should
  not significantly depend on the interval choices of the jobs (beyond actually
  queueing the jobs if they are more frequent)

This is typically run as a daemon process from the main.py via the
"run_forever_sync" function.
"""
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
import importlib
import inspect
import json
from graceful_death import graceful_sleep
from error_middleware import handle_error
from redis.asyncio.client import Redis, Pipeline
from redis.exceptions import NoScriptError
import threading
import datetime
from itgs import Itgs
import bisect
import calendar
import pytz
import time
import hashlib
import os

from mp_helper import adapt_threading_event_to_asyncio

WeekDay = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
"""The days of the week as a type"""

WeekDays = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
"""The days of the week as a list"""

WeekDayToIndex = {day: i for i, day in enumerate(WeekDays)}
"""A lookup from the day of the week to the index of the day of the week"""


class JobInterval:
    """Describes when a job should be run.

    Each field acts as a filter - the job is scheduled to
    run whenever all the filters match. This means that the
    interval is not necessarily constant - e.g., a job can
    run on the "1st monday of every month" by specifying it's
    the first week of the month and it's a monday.

    A valid job interval must have at least one filter set.
    """

    __slots__ = (
        "tz",
        "months",
        "days_of_month",
        "days_of_week",
        "hours",
        "minutes",
        "seconds",
    )

    def __init__(
        self,
        tz: datetime.timezone,
        *,
        months: Optional[List[int]] = None,
        days_of_month: Optional[List[int]] = None,
        days_of_week: Optional[List[WeekDay]] = None,
        hours: Optional[List[int]] = None,
        minutes: Optional[List[int]] = None,
        seconds: Optional[List[int]] = None,
    ):
        self.tz: datetime.timezone = tz
        """The time zone relevant for this job"""

        self.months: Optional[List[int]] = months
        """the months in which the job runs, where 1 is january, in
        sorted order. None means any month is valid.
        """

        self.days_of_month: Optional[List[int]] = days_of_month
        """the days of the month when the job runs, where 1 is the
        first day of the month, in sorted order. None means any day
        of the month is valid.
        """

        self.days_of_week: Optional[List[WeekDay]] = days_of_week
        """the days of the week when the job runs, in sorted order
        (e.g., ["mon", "tue", "wed"]). None means any day of the week
        is valid.
        """

        self.hours: Optional[List[int]] = hours
        """the hours of the day when the job runs, where 0 is midnight and
        23 is 11pm, in sorted order. None means any hour is valid.
        """

        self.minutes: Optional[List[int]] = minutes
        """the minutes of the hour when the job runs, where 0 is the
        beginning of the hour and 59 is the end of the hour, in sorted order
        (e.g., [0, 15, 30, 45]). None means any minute is valid.
        """

        self.seconds: Optional[List[int]] = seconds
        """the seconds of the minute when the job runs, where 0 is the
        beginning of the minute and 59 is the end of the minute, in sorted order
        (e.g., [0, 15, 30, 45]). None means any second is valid.
        """

        self.verify()

    def verify(self) -> None:
        """Raises an AssertionError if the interval is invalid.

        This also raises an assertion error for very likely mistakes, such as
        specifying hours but not minutes and seconds - since that means every
        second of every minute in that hour, while you probably mean for it to
        run once per hour.
        """
        assert any(
            [
                self.months is not None,
                self.days_of_month is not None,
                self.days_of_week is not None,
                self.hours is not None,
                self.minutes is not None,
                self.seconds is not None,
            ]
        ), "A job interval must have at least one filter set"

        assert (
            self.months is None or self.months
        ), f"{self.months=} must be None or non-empty"
        assert (
            self.days_of_month is None or self.days_of_month
        ), f"{self.days_of_month=} must be None or non-empty"
        assert (
            self.days_of_week is None or self.days_of_week
        ), f"{self.days_of_week=} must be None or non-empty"
        assert (
            self.hours is None or self.hours
        ), f"{self.hours=} must be None or non-empty"
        assert (
            self.minutes is None or self.minutes
        ), f"{self.minutes=} must be None or non-empty"
        assert (
            self.seconds is None or self.seconds
        ), f"{self.seconds=} must be None or non-empty"

        assert (
            sum(
                [
                    self.days_of_month is not None,
                    self.days_of_week is not None,
                ],
                0,
            )
            <= 1
        ), f"Only one of {self.days_of_month=} and {self.days_of_week=}can be set"

        if self.months is not None:
            assert all(
                1 <= m <= 12 for m in self.months
            ), f"{self.months=} must be in the range [1, 12]"
            assert self.months == tuple(
                sorted(self.months)
            ), f"{self.months=} must be sorted"
            assert len(self.months) == len(
                frozenset(self.months)
            ), f"{self.months=} must not have duplicates"
            assert all(
                [
                    self.hours is not None,
                    self.minutes is not None,
                    self.seconds is not None,
                ]
            ), f"{self.months=} must be accompanied at least by {self.hours=}, {self.minutes=}, and {self.seconds=}"

        if self.days_of_month is not None:
            assert all(
                1 <= d <= 31 for d in self.days_of_month
            ), f"{self.days_of_month=} must be in the range [1, 31]"
            assert self.days_of_month == tuple(
                sorted(self.days_of_month)
            ), f"{self.days_of_month=} must be sorted"
            assert len(self.days_of_month) == len(
                frozenset(self.days_of_month)
            ), f"{self.days_of_month=} must not have duplicates"

            assert all(
                [
                    self.hours is not None,
                    self.minutes is not None,
                    self.seconds is not None,
                ]
            ), f"{self.days_of_month=} must be accompanied at least by {self.hours=}, {self.minutes=}, and {self.seconds=}"

        if self.days_of_week is not None:
            assert all(
                d in WeekDayToIndex for d in self.days_of_week
            ), f"{self.days_of_week=} must be in the set of valid days of week"
            assert self.days_of_week == tuple(
                sorted(self.days_of_week, key=lambda d: WeekDayToIndex[d])
            ), f"{self.days_of_week=} must be sorted"
            assert len(self.days_of_week) == len(
                frozenset(self.days_of_week)
            ), f"{self.days_of_week=} must not have duplicates"

            assert all(
                [
                    self.hours is not None,
                    self.minutes is not None,
                    self.seconds is not None,
                ]
            ), f"{self.days_of_week=} must be accompanied at least by {self.hours=}, {self.minutes=}, and {self.seconds=}"

        if self.hours is not None:
            assert all(
                0 <= h <= 23 for h in self.hours
            ), f"{self.hours=} must be in the range [0, 23]"
            assert self.hours == tuple(
                sorted(self.hours)
            ), f"{self.hours=} must be sorted"
            assert len(self.hours) == len(
                frozenset(self.hours)
            ), f"{self.hours=} must not have duplicates"
            assert all(
                [
                    self.minutes is not None,
                    self.seconds is not None,
                ]
            ), f"{self.hours=} must be accompanied at least by {self.minutes=} and {self.seconds=}"

        if self.minutes is not None:
            assert all(
                0 <= m <= 59 for m in self.minutes
            ), f"{self.minutes=} must be in the range [0, 59]"
            assert self.minutes == tuple(
                sorted(self.minutes)
            ), f"{self.minutes=} must be sorted"
            assert len(self.minutes) == len(
                frozenset(self.minutes)
            ), f"{self.minutes=} must not have duplicates"
            assert (
                self.seconds is not None
            ), f"{self.minutes=} must be accompanied by {self.seconds=}"

        if self.seconds is not None:
            assert all(
                0 <= s <= 59 for s in self.seconds
            ), f"{self.seconds=} must be in the range [0, 59]"
            assert self.seconds == tuple(
                sorted(self.seconds)
            ), f"{self.seconds=} must be sorted"
            assert len(self.seconds) == len(
                frozenset(self.seconds)
            ), f"{self.seconds=} must not have duplicates"

    def next_runtime_after(self, time: Union[int, float]) -> Union[int, float]:
        """Determines smallest unix time in fractional seconds strictly after the
        given time matching all of the filters

        Args:
            time (float): The unix time to consider

        Returns:
            float: the earliest unix time in fractional seconds strictly after the
            given time matching all of the filters
        """
        match = datetime.datetime.fromtimestamp(int(time + 1), tz=self.tz)
        if self.seconds is not None:
            cur_second_left_idx = bisect.bisect_left(self.seconds, match.second)
            if cur_second_left_idx < len(self.seconds):
                match = match.replace(second=self.seconds[cur_second_left_idx])
            else:
                match = match + datetime.timedelta(
                    seconds=self.seconds[0] - match.second + 60
                )

        if self.minutes is not None:
            cur_minute_left_idx = bisect.bisect_left(self.minutes, match.minute)
            if cur_minute_left_idx < len(self.minutes):
                match = match.replace(minute=self.minutes[cur_minute_left_idx])
            else:
                match = match + datetime.timedelta(
                    minutes=self.minutes[0] - match.minute + 60
                )

        if self.hours is not None:
            cur_hour_left_idx = bisect.bisect_left(self.hours, match.hour)
            if cur_hour_left_idx < len(self.hours):
                match = match.replace(hour=self.hours[cur_hour_left_idx])
            else:
                match = match + datetime.timedelta(
                    hours=self.hours[0] - match.hour + 24
                )

        if self.days_of_week is not None:
            days_of_week_indices = tuple(WeekDayToIndex[d] for d in self.days_of_week)
            cur_day_of_week_left_idx = bisect.bisect_left(
                days_of_week_indices, match.weekday()
            )
            if cur_day_of_week_left_idx < len(self.days_of_week):
                match = match + datetime.timedelta(
                    days=WeekDayToIndex[self.days_of_week[cur_day_of_week_left_idx]]
                    - match.weekday()
                )
            else:
                match = match + datetime.timedelta(
                    days=WeekDayToIndex[self.days_of_week[0]] - match.weekday() + 7
                )

        if self.days_of_month is not None:
            cur_day_of_month_left_idx = bisect.bisect_left(
                self.days_of_month, match.day
            )
            if cur_day_of_month_left_idx < len(self.days_of_month):
                match = match.replace(day=self.days_of_month[cur_day_of_month_left_idx])
            else:
                match = match + datetime.timedelta(
                    days=self.days_of_month[0]
                    - match.day
                    + calendar.monthrange(match.year, match.month)[1]
                )

        if self.months is not None:
            cur_month_left_idx = bisect.bisect_left(self.months, match.month)
            if cur_month_left_idx < len(self.months):
                match = match.replace(month=self.months[cur_month_left_idx])
            else:
                match = match.replace(year=match.year + 1, month=self.months[0])

        return match.timestamp()

    def __eq__(self, other) -> bool:
        if not isinstance(other, JobInterval):
            return False

        return (
            self.seconds == other.seconds
            and self.minutes == other.minutes
            and self.hours == other.hours
            and self.days_of_week == other.days_of_week
            and self.days_of_month == other.days_of_month
            and self.months == other.months
            and self.tz == other.tz
        )

    def __hash__(self) -> int:
        def clip(x):
            return x % (2**64)

        result = 31

        try:
            current_utc_offset = self.tz.utcoffset(
                datetime.datetime.now()
            ).total_seconds()
        except pytz.exceptions.NonExistentTimeError:
            # it's currently daily savings skip hour, use the previous offset
            current_utc_offset = self.tz.utcoffset(
                datetime.datetime.utcfromtimestamp(time.time() - 60 * 60 * 2)
            ).total_seconds()

        result = 17 * result + int(current_utc_offset)
        for val in (
            self.seconds,
            self.minutes,
            self.hours,
            self.days_of_week,
            self.days_of_month,
            self.months,
        ):
            if val is None:
                result = clip(17 * result + 100002961)
            else:
                result = clip(17 * result + len(val))
                for v in val:
                    if isinstance(v, int):
                        result = clip(17 * result + v)
                    elif isinstance(v, str):
                        result = clip(17 * result + len(v))
                        for c in v:
                            result = clip(17 * result + ord(c))

        return result

    def __repr__(self) -> str:
        result = [self.__class__.__name__, "(", repr(self.tz)]
        for attr in (
            "seconds",
            "minutes",
            "hours",
            "days_of_week",
            "days_of_month",
            "months",
        ):
            if getattr(self, attr) is not None:
                result.append(", ")
                result.append(attr)
                result.append("=")
                result.append(repr(getattr(self, attr)))
        result.append(")")
        return "".join(result)

    def stable_hash(self) -> int:
        return self.__hash__()


@dataclass(frozen=True)
class Job:
    name: str
    """The name of the job, which is the module path within this repo, e.g., runners.example"""

    kwargs: List[Tuple[str, Any]]
    """The keyword arguments to the job, specified as a immutable list of key,
    value pairs. Must be hashable"""

    interval: JobInterval
    """The interval at which the job should be run"""

    def verify(self) -> None:
        """Checks if this is a possible viable job; we'd rather get this error as soon
        as possible rather than when we try to run the job
        """
        hash(self)
        self.interval.verify()
        module = importlib.import_module(self.name)
        assert hasattr(
            module, "execute"
        ), f"Module {self.name=} must have an execute function"

        args = inspect.signature(module.execute).parameters
        kwargs_dict = dict(self.kwargs)

        for name, param in args.items():
            if param.kind != param.KEYWORD_ONLY:
                continue
            if (
                param.name not in kwargs_dict
                and param.default is inspect.Parameter.empty
            ):
                raise AssertionError(
                    f"Module {self.name=} does not have a default value for argument {name=}, yet its missing from {self.kwargs=}"
                )

        for name, _ in self.kwargs:
            if name not in args:
                raise AssertionError(
                    f"Module {self.name=} does not have an argument {name=}, yet its present in {self.kwargs=}"
                )

            param = args[name]
            if param.kind != param.KEYWORD_ONLY:
                raise AssertionError(
                    f"Module {self.name=} has an argument {name=} that is not a keyword only argument, yet it is present in {self.kwargs=}"
                )

    def stable_hash(self) -> int:
        """A hash that is stable across runs"""

        def clip(x):
            return x % (2**64)

        result = 31
        result = clip(17 * result + len(self.name))
        for c in self.name:
            result = clip(17 * result + ord(c))

        result = clip(17 * result + len(self.kwargs))
        for k, v in self.kwargs:
            result = clip(17 * result + len(k))
            for c in k:
                result = clip(17 * result + ord(c))

            json_val = json.dumps(v)
            result = clip(17 * result + len(json_val))
            for c in json_val:
                result = clip(17 * result + ord(c))

        result = clip(17 * result + self.interval.stable_hash())
        return result


pst = pytz.timezone("US/Pacific")
is_dev = os.environ.get("ENVIRONMENT") == "dev"
JOBS: List[Job] = (
    Job(
        name="runners.example",
        kwargs=(("message", "Ping from recurring_jobs"),),
        interval=JobInterval(pst, seconds=(0,)),
    ),
    Job(
        name="runners.sweep_partial_file_uploads",
        kwargs=tuple(),
        interval=(
            JobInterval(pst, hours=(0,), minutes=(0,), seconds=(0,))
            if not is_dev
            else JobInterval(pst, seconds=(0, 20, 40))
        ),
    ),
    Job(
        name="runners.revenue_cat.sweep_open_stripe_checkout_sessions",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0, 30)),
    ),
    Job(
        name="runners.stats.daily_active_users",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_new_users",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.instructors_monthly_counts",
        kwargs=tuple(),
        interval=JobInterval(
            pst, days_of_month=(1,), hours=(2,), minutes=(0,), seconds=(0,)
        ),
    ),
    Job(
        name="runners.stats.journey_monthly_counts",
        kwargs=tuple(),
        interval=JobInterval(
            pst, days_of_month=(1,), hours=(2,), minutes=(0,), seconds=(0,)
        ),
    ),
    Job(
        name="runners.stats.interactive_prompt_sessions_daily_subcategory_views",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.interactive_prompt_sessions_monthly_counts",
        kwargs=tuple(),
        interval=JobInterval(
            pst, days_of_month=(1,), hours=(2,), minutes=(0,), seconds=(0,)
        ),
    ),
    Job(
        name="runners.stats.monthly_active_users",
        kwargs=tuple(),
        interval=JobInterval(
            pst, days_of_month=(1,), hours=(2,), minutes=(0,), seconds=(0,)
        ),
    ),
    Job(
        name="runners.stats.users_retention",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_utm_conversion_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.visitors.process_visitor_users",
        kwargs=tuple(),
        interval=JobInterval(pst, minutes=tuple(range(0, 60, 5)), seconds=(0,)),
    ),
    Job(
        name="runners.visitors.process_visitor_utms",
        kwargs=tuple(),
        interval=JobInterval(pst, minutes=tuple(range(0, 60, 5)), seconds=(5,)),
    ),
    # we run backups just before daily jobs and just after
    Job(
        name="runners.backup_database",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(1,), minutes=(45,), seconds=(0,)),
    ),
    Job(
        name="runners.backup_redis",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(1,), minutes=(45,), seconds=(0,)),
    ),
    Job(
        name="runners.backup_database",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(3,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.backup_redis",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(3,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.reseed_emotion_votes",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(0,), minutes=(5,), seconds=(0,)),
    ),
    Job(
        name="runners.check_database",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0,)),
    ),
    Job(
        name="runners.process_missing_emotions",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(0,), minutes=(5,), seconds=(0,)),
    ),
    Job(
        name="runners.process_missing_videos",
        kwargs=tuple(),
        interval=JobInterval(
            pst, days_of_week=("sat",), hours=(0,), minutes=(10,), seconds=(0,)
        ),
    ),
    Job(
        name="runners.slack_stats.emotion_selections",
        kwargs=tuple(),
        interval=JobInterval(
            pst, days_of_week=("fri",), hours=(9,), minutes=(0,), seconds=(0,)
        ),
    ),
    Job(
        name="runners.stats.daily_push_receipt_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(0,), minutes=(15,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_push_ticket_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(0,), minutes=(15,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_push_token_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(0,), minutes=(15,), seconds=(0,)),
    ),
    Job(
        name="runners.push.send",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0,)),
    ),
    Job(
        name="runners.push.ticket_hot_to_cold",
        kwargs=tuple(),
        interval=JobInterval(pst, minutes=tuple(range(0, 60, 5)), seconds=(0,)),
    ),
    Job(
        name="runners.push.check",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(30,)),
    ),
    Job(
        name="runners.stats.daily_sms_send_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.sms.send",
        kwargs=tuple(),
        interval=JobInterval(
            pst,
            seconds=(
                0,
                15,
                30,
                45,
            ),
        ),
    ),
    Job(
        name="runners.stats.daily_sms_polling_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.sms.receipt_stale_detection",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(5,)),
    ),
    Job(
        name="runners.sms.receipt_recovery",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(10,)),
    ),
    Job(
        name="runners.sms.receipt_reconciliation",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(20,)),
    ),
    Job(
        name="runners.stats.daily_sms_webhook_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.emails.receipt_reconciliation",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0,)),
    ),
    Job(
        name="runners.emails.send",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0, 15, 30, 45)),
    ),
    Job(
        name="runners.emails.stale_receipt_detection",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_email_send_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_email_event_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_touch_send_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_touch_stale_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.touch.send",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0, 15, 30, 45)),
    ),
    Job(
        name="runners.touch.log",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(3,)),
    ),
    Job(
        name="runners.stats.daily_touch_link_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.touch.persist_link",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0,)),
    ),
    Job(
        name="runners.touch.leaked_link_detection",
        kwargs=tuple(),
        interval=JobInterval(pst, minutes=(0, 15, 30, 45), seconds=(0,)),
    ),
    Job(
        name="runners.touch.delayed_click_persist",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(0,)),
    ),
    Job(
        name="runners.touch.stale_detection",
        kwargs=tuple(),
        interval=JobInterval(pst, minutes=(0, 15, 30, 45), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_daily_reminder_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.daily_reminders.assign_time",
        kwargs=tuple(),
        interval=JobInterval(pst, minutes=(0, 15, 30, 45), seconds=(0,)),
    ),
    Job(
        name="runners.daily_reminders.send",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(5, 20, 35, 50)),
    ),
    Job(
        name="runners.stats.daily_daily_reminder_registration_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.alerting.check_frontend",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=tuple(range(0, 60, 5))),
    ),
    Job(
        name="runners.stats.daily_siwo_authorize_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_siwo_verify_email_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.stats.daily_siwo_exchange_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.siwo.send_delayed_email_verifications",
        kwargs=tuple(),
        interval=JobInterval(pst, seconds=(55,)),
    ),
    Job(
        name="runners.stats.daily_contact_method_stats",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.daily_reminders.recheck_counts",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(0,), seconds=(0,)),
    ),
    Job(
        name="runners.sms.disable_phones_in_dev",
        kwargs=tuple(),
        interval=JobInterval(pst, hours=(2,), minutes=(5,), seconds=(0,)),
    ),
)
"""The jobs that should be run."""


def _stable_hash_jobs(jobs: List[Job]) -> int:
    """Returns a hash of the jobs that is stable across runs"""

    def clip(x):
        return x % (2**64)

    result = 31
    for job in jobs:
        result = clip(17 * result + job.stable_hash())
    return result


JOBS_HASH = _stable_hash_jobs(JOBS)
"""The hash of the jobs, used to determine if we have the latest version of the jobs"""

JOBS_BY_HASH: Dict[int, Job] = dict((job.stable_hash(), job) for job in JOBS)

assert len(JOBS) == len(JOBS_BY_HASH), "hash collision in jobs"


async def update_jobs(itgs: Itgs) -> None:
    """Ensures that the jobs in the rjobs sset are up to date. In particular,
    this checks if the jobs hash in redis matches the hash of the jobs we've
    calculated. If it does not, this will atomically clear the jobs and add
    the new jobs.

    This preserves the next runtime of any jobs which are not changed, though
    doing so is only necessary if the recurring_jobs instance is behind -- i.e.,
    if the jobs are already due to be run.

    This is a no-op if the jobs are already up to date.

    Args:
        itgs (Itgs): The integrations to use
    """
    redis = await itgs.redis()

    async def inner(pipe: Pipeline) -> None:
        current_hash: bytes = await pipe.get("rjobs:hash")
        if current_hash is not None and int(current_hash) == JOBS_HASH:
            return

        now = time.time()

        new_scores_by_hash: Dict[int, float] = dict(
            (job.stable_hash(), job.interval.next_runtime_after(now)) for job in JOBS
        )

        resp = await pipe.zrange("rjobs", "0", "-1", withscores=True)
        for job_hash, score in resp:
            job_hash = int(job_hash)
            score = float(score)
            if job_hash in new_scores_by_hash:
                new_scores_by_hash[job_hash] = min(score, new_scores_by_hash[job_hash])

        pipe.multi()
        await pipe.delete("rjobs")
        await pipe.delete("rjobs:hash")
        await pipe.delete("rjobs:purgatory")
        await pipe.zadd("rjobs", mapping=new_scores_by_hash)
        await pipe.set("rjobs:hash", bytes(str(JOBS_HASH), "ascii"))

    await redis.transaction(inner, "rjobs:hash", "rjobs")


async def clean_purgatory(itgs: Itgs) -> None:
    """Cleans the purgatory of jobs that have been started but haven't yet been
    pushed onto the jobs queue by requeuing them at the appropriate time on the
    jobs queue. This is not necessary except if the recurring_jobs instance
    crashes while queueing a job to be run, which is pretty unlikely.

    Args:
        itgs (Itgs): The integrations to use
    """
    redis = await itgs.redis()

    async def inner(pipe: Pipeline) -> None:
        current_hash: bytes = await pipe.get("rjobs:hash")
        assert (
            current_hash is not None and int(current_hash) == JOBS_HASH
        ), f"{current_hash=}"

        resp = await pipe.smembers("rjobs:purgatory")
        if not resp:
            return

        new_scores: Dict[bytes, float] = {}
        for job_hash in resp:
            if int(job_hash) not in JOBS_BY_HASH:
                continue

            job = JOBS_BY_HASH[int(job_hash)]
            new_scores[job_hash] = job.interval.next_runtime_after(time.time())

        if not new_scores:
            return

        pipe.multi()
        await pipe.zadd("rjobs", mapping=new_scores)
        await pipe.delete("rjobs:purgatory")

    await redis.transaction(inner, "rjobs:hash", "rjobs:purgatory")


CONDITIONALLY_ZPOPMIN = """
local max_score = tonumber(ARGV[1])
local expected_jobs_hash = tonumber(ARGV[2])
local key = KEYS[1]
local jobs_hash_key = KEYS[2]
local purgatory_key = KEYS[3]

local job_hash_val = redis.call("GET", jobs_hash_key)
if not job_hash_val then return {'-1', nil} end

local job_hash = tonumber(job_hash_val)
if job_hash ~= expected_jobs_hash then return {'-1', job_hash} end

local resp = redis.call("zrangebyscore", key, "-inf", max_score, "limit", 0, 1, "withscores")
if #resp == 0 then
    return {'0', nil}
end

local result = redis.call('zpopmin', key, 1)
redis.call('SADD', purgatory_key, tostring(result[1]))

return {'1', result}
"""

CONDITIONALLY_ZPOPMIN_HASH = hashlib.sha1(
    bytes(CONDITIONALLY_ZPOPMIN, "ascii")
).hexdigest()


async def conditionally_zpopmin(
    redis: Redis,
    key: str,
    max_score: float,
    hash_key: str,
    purgatory_key: str,
) -> Optional[Tuple[bytes, float]]:
    """Pops a job off the queue if its score is less than or equal to max_score

    Args:
        redis (Redis): The redis connection to use
        key (str): The key to pop from, typically rjobs
        max_score (float): The maximum score to pop, typically the current time in seconds
        hash_key (str): The key to check the hash of the jobs against, typically rjobs:hash
        purgatory_key (str): The key to add the popped job to, typically rjobs:purgatory

    Returns:
        bytes: The job hash
        float: The time the job was supposed to run

    Raises:
        AssertionError: If the hash of the jobs in redis does not match JOBS_HASH
    """
    try:
        res = await redis.evalsha(
            CONDITIONALLY_ZPOPMIN_HASH,
            3,
            key,
            hash_key,
            purgatory_key,
            max_score,
            JOBS_HASH,
        )
        if res[0] == b"0":
            return None
        if res[0] == b"-1":
            raise RuntimeError(
                f"jobs hash mismatch; expected {JOBS_HASH} in {hash_key=}, got {repr(res[1] if len(res) > 1 else None)}"
            )
        return res[1][0], float(res[1][1])
    except NoScriptError:
        await redis.script_load(CONDITIONALLY_ZPOPMIN)
        return await conditionally_zpopmin(
            redis, key, max_score, hash_key, purgatory_key
        )


MOVE_FROM_PURGATORY = """
local job_hash = ARGV[1]
local next_runtime = tonumber(ARGV[2])
local expected_jobs_hash = tonumber(ARGV[3])
local purgatory_key = KEYS[1]
local rjobs_key = KEYS[2]
local jobs_hash_key = KEYS[3]

local jobs_hash_val = redis.call("GET", jobs_hash_key)
if not jobs_hash_val then return '-1' end

local jobs_hash = tonumber(jobs_hash_val)
if jobs_hash ~= expected_jobs_hash then return '-1' end

local resp = redis.call("SREM", purgatory_key, job_hash)
if resp == 0 then return '0' end

redis.call("ZADD", rjobs_key, next_runtime, job_hash)
return '1'
"""

MOVE_FROM_PURGATORY_HASH = hashlib.sha1(bytes(MOVE_FROM_PURGATORY, "ascii")).hexdigest()


async def move_from_purgatory(
    redis: Redis,
    purgatory_key: str,
    rjobs_key: str,
    jobs_hash_key: str,
    job_hash: int,
    next_runtime: float,
) -> bool:
    """Moves a job from the purgatory to the rjobs queue with the given next_runtime

    Args:
        redis (Redis): The redis client
        purgatory_key (str): The key of the purgatory set, typically rjobs:purgatory
        rjobs_key (str): The key of the rjobs queue, typically rjobs
        jobs_hash_key (str): The key of the jobs hash, typically rjobs:hash
        job_hash (int): The hash of the job to move
        next_runtime (float): The next runtime of the job

    Returns:
        bool: True if the job was moved, False if it was not in the purgatory

    Raises:
        AssertionError: If the jobs hash does not match the expected hash
    """
    try:
        res = await redis.evalsha(
            MOVE_FROM_PURGATORY_HASH,
            3,
            purgatory_key,
            rjobs_key,
            jobs_hash_key,
            job_hash,
            next_runtime,
            JOBS_HASH,
        )
        if res == b"-1":
            raise RuntimeError(
                f"jobs hash mismatch; expected {JOBS_HASH} in {jobs_hash_key=}"
            )
        return res == b"1"
    except NoScriptError:
        await redis.script_load(MOVE_FROM_PURGATORY)
        return await move_from_purgatory(
            redis, purgatory_key, rjobs_key, jobs_hash_key, job_hash, next_runtime
        )


async def _run_forever():
    if not JOBS:
        return  # nothing to do

    while True:
        try:
            async with Itgs() as itgs:
                await update_jobs(itgs)
                await clean_purgatory(itgs)

                redis = await itgs.redis()
                jobs = await itgs.jobs()
                while True:
                    result = await conditionally_zpopmin(
                        redis, "rjobs", time.time(), "rjobs:hash", "rjobs:purgatory"
                    )
                    if result is None:
                        # peek the next one
                        next_jobs: List[Tuple[bytes, float]] = await redis.zrange(
                            "rjobs", 0, 0, withscores=True
                        )
                        if not next_jobs:
                            # all of them are running!
                            await asyncio.sleep(1)
                            continue
                        now = time.time()
                        next_score = next_jobs[0][1]
                        if next_score > now:
                            await asyncio.sleep(next_score - now)
                        continue

                    job_hash = int(result[0])
                    assert job_hash in JOBS_BY_HASH, f"unexpected {job_hash=} in rjobs"
                    job = JOBS_BY_HASH[job_hash]
                    await jobs.enqueue(job.name, **dict(job.kwargs))
                    await move_from_purgatory(
                        redis,
                        "rjobs:purgatory",
                        "rjobs",
                        "rjobs:hash",
                        job_hash,
                        job.interval.next_runtime_after(time.time()),
                    )
        except Exception as e:
            await handle_error(e)
            await asyncio.sleep(10)


async def run_forever(stop_event: threading.Event, stopping_event: threading.Event):
    asyncio_event = adapt_threading_event_to_asyncio(stop_event)
    asyncio_stopping_event = adapt_threading_event_to_asyncio(stopping_event)

    try:
        _, pending = await asyncio.wait(
            [
                asyncio.create_task(_run_forever()),
                asyncio.create_task(asyncio_stopping_event.wait()),
                asyncio.create_task(asyncio_event.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()

        if asyncio_stopping_event.is_set():
            print("Recurring jobs stopped on stopping event, waiting for real stop")
            await asyncio.wait_for(asyncio_event.wait(), timeout=300)
    except Exception as e:
        await handle_error(e)
    finally:
        stop_event.set()
        print("Recurring jobs stopped")


def run_forever_sync(stop_event: threading.Event, stopping_event: threading.Event):
    """Handles recurring jobs, queuing them to the hot queue as it's time for
    them to run. Recurring jobs stop immediately upon an update being requested
    rather than waiting our turn, in case the recurring jobs change
    """
    asyncio.run(run_forever(stop_event, stopping_event))
