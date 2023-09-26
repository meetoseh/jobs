from typing import Optional, Literal
from lib.redis_stats_preparer import RedisStatsPreparer


Channel = Literal["sms", "email", "push"]


class DailyReminderStatsPreparer(RedisStatsPreparer):
    """A basic helper class for updating daily reminder stats"""

    def __init__(self):
        super().__init__()

    def incr_daily_reminders(
        self,
        unix_date: int,
        event: str,
        *,
        event_extra: Optional[bytes] = None,
        amt: int = 1,
    ) -> "DailyReminderStatsPreparer":
        """Updates the given event in stats:daily_reminders:daily:{unix_date}"""
        super().incrby(
            unix_date=unix_date,
            basic_key_format="stats:daily_reminders:daily:{unix_date}",
            earliest_key=b"stats:daily_reminders:daily:earliest",
            event=event,
            event_extra_format="stats:daily_reminders:daily:{unix_date}:extra:{event}",
            event_extra=event_extra,
            amt=amt,
        )
        return self

    def incr_attempted(self, unix_date: int, *, amt: int = 1):
        return self.incr_daily_reminders(unix_date, "attempted", amt=amt)

    def incr_overdue(self, unix_date: int, *, amt: int = 1):
        return self.incr_daily_reminders(unix_date, "overdue", amt=amt)

    def incr_skipped_assigning_time(
        self, unix_date: int, *, channel: Channel, amt: int = 1
    ):
        return self.incr_daily_reminders(
            unix_date,
            "skipped_assigning_time",
            event_extra=channel.encode("ascii"),
            amt=amt,
        )

    def incr_time_assigned(self, unix_date: int, *, channel: Channel, amt: int = 1):
        return self.incr_daily_reminders(
            unix_date,
            "time_assigned",
            event_extra=channel.encode("ascii"),
            amt=amt,
        )

    def incr_sends_attempted(self, unix_date: int, *, amt: int = 1):
        return self.incr_daily_reminders(unix_date, "sends_attempted", amt=amt)

    def incr_sends_lost(self, unix_date: int, *, amt: int = 1):
        return self.incr_daily_reminders(unix_date, "sends_lost", amt=amt)

    def incr_skipped_sending(self, unix_date: int, *, channel: Channel, amt: int = 1):
        return self.incr_daily_reminders(
            unix_date,
            "skipped_sending",
            event_extra=channel.encode("ascii"),
            amt=amt,
        )

    def incr_links(self, unix_date: int, *, amt: int = 1):
        return self.incr_daily_reminders(unix_date, "links", amt=amt)

    def incr_sent(self, unix_date: int, *, channel: Channel, amt: int = 1):
        return self.incr_daily_reminders(
            unix_date,
            "sent",
            event_extra=channel.encode("ascii"),
            amt=amt,
        )
