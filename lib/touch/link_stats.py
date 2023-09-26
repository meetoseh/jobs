from typing import Optional
from lib.redis_stats_preparer import RedisStatsPreparer


class LinkStatsPreparer(RedisStatsPreparer):
    """A basic helper class for updating touch link stats"""

    def __init__(self):
        super().__init__()

    def incr_touch_links(
        self,
        unix_date: int,
        event: str,
        *,
        event_extra: Optional[bytes] = None,
        amt: int = 1,
    ) -> "LinkStatsPreparer":
        """Updates the given event in stats:touch_links:daily:{unix_date}"""
        super().incrby(
            unix_date=unix_date,
            basic_key_format="stats:touch_links:daily:{unix_date}",
            earliest_key=b"stats:touch_links:daily:earliest",
            event=event,
            event_extra_format="stats:touch_links:daily:{unix_date}:extra:{event}",
            event_extra=event_extra,
            amt=amt,
        )
        return self
