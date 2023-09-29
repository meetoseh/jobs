from typing import Optional, Literal
from lib.redis_stats_preparer import RedisStatsPreparer


Channel = Literal["sms", "email", "push"]


class DailyReminderRegistrationStatsPreparer(RedisStatsPreparer):
    """A basic helper class for updating daily reminder registration stats"""

    def __init__(self):
        super().__init__()

    def incr_daily_reminder_registrations(
        self,
        unix_date: int,
        event: str,
        *,
        event_extra: Optional[bytes] = None,
        amt: int = 1,
    ) -> "DailyReminderRegistrationStatsPreparer":
        """Updates the given event in stats:daily_reminder_registrations:daily:{unix_date}"""
        super().incrby(
            unix_date=unix_date,
            basic_key_format="stats:daily_reminder_registrations:daily:{unix_date}",
            earliest_key=b"stats:daily_reminder_registrations:daily:earliest",
            event=event,
            event_extra_format="stats:daily_reminder_registrations:daily:{unix_date}:extra:{event}",
            event_extra=event_extra,
            amt=amt,
        )
        return self

    def incr_subscribed(
        self, unix_date: int, channel: Channel, reason: str, *, amt: int = 1
    ) -> "DailyReminderRegistrationStatsPreparer":
        """Increments the number of users subscribed to the given channel"""
        return self.incr_daily_reminder_registrations(
            unix_date,
            "subscribed",
            event_extra=f"{channel}:{reason}".encode("utf-8"),
            amt=amt,
        )

    def incr_unsubscribed(
        self, unix_date: int, channel: Channel, reason: str, *, amt: int = 1
    ) -> "DailyReminderRegistrationStatsPreparer":
        """Increments the number of users subscribed to the given channel"""
        return self.incr_daily_reminder_registrations(
            unix_date,
            "unsubscribed",
            event_extra=f"{channel}:{reason}".encode("utf-8"),
            amt=amt,
        )
