from typing import Dict, Optional
from redis.asyncio import Redis as AsyncioRedisClient
from redis_helpers.set_if_lower import set_if_lower, ensure_set_if_lower_script_exists
from redis_helpers.run_with_prep import run_with_prep
from itgs import Itgs


class RedisStatsPreparer:
    """Helper object for constructing what changes to write to daily stats keys
    in redis, e.g., stats:touch_send_daily:{unix_date}
    """

    def __init__(self) -> None:
        self.stats: Dict[bytes, Dict[bytes, int]] = dict()
        """A dictionary from redis keys to which key/value pairs to
        increment and by how much
        """

        self.earliest_keys: Dict[bytes, int] = dict()
        """The earliest keys which need to be set to the minimum of the value
        in the dictionary or their current value
        """

    def get_for_key(self, key: bytes) -> Dict[bytes, int]:
        """Initializes, if necessary, the stats for the given redis key and
        returns the corresponding dictionary. Mutating the dictionary will
        mutate the prepared stats
        """
        assert isinstance(key, bytes)
        result = self.stats.get(key)
        if result is None:
            result = dict()
            self.stats[key] = result
        return result

    def set_earliest(self, key: bytes, unix_date: int) -> None:
        """Sets the earliest key to the minimum of the value in the dictionary
        or the specified value
        """
        assert isinstance(key, bytes)
        assert isinstance(unix_date, int)
        current = self.earliest_keys.get(key)
        if current is None:
            self.earliest_keys[key] = unix_date
        else:
            self.earliest_keys[key] = min(current, unix_date)

    def incrby(
        self,
        *,
        unix_date: int,
        basic_key_format: str,
        earliest_key: bytes,
        event: str,
        event_extra_format: Optional[str] = None,
        event_extra: Optional[bytes] = None,
        amt=1
    ):
        """Increments the basic stats key and, optionally, extra stats key by the
        given amount.

        Example:

        ```py
        stats.incrby(
            unix_date=unix_date,
            basic_key_format="stats:touch_send:daily:{unix_date}",
            earliest_key=b"stats:touch_send:daily:earliest",
            event="attempted",
            event_extra_format="stats:touch_send:daily:{unix_date}:extra:{event}",
            event_extra=b"daily_reminder:sms"
        )
        ```

        Args:
            unix_date (int): The unix date for the stats
            basic_key_format (str): The format string for the basic stats key that
                the event is within, e.g., `stats:touch_send:daily:{unix_date}`
            earliest_key (bytes): The key for the earliest key that needs to be set
                to the minimum of the current value and the given unix date
            event (str): The event that occurred, e.g., `attempted`, as a string so
                that it can be properly formatted in the event extra key if necessary
            event_extra_format (str, optional): The format string for the event extra
                key that contains the breakdown information for the basic key, e.g.,
                `stats:touch_send:daily:{unix_date}:extra:{event}`. Defaults to None.
                Ignored if event_extra is None.
            event_extra (bytes, optional): The event extra that occurred, e.g., the
                key within the event extra hash that should be incremented. Defaults
                to None.
            amt (int, optional): The amount to increment by. Defaults to 1.
        """
        assert isinstance(unix_date, int)
        assert isinstance(basic_key_format, str)
        assert isinstance(earliest_key, bytes)
        assert isinstance(event, str)
        assert isinstance(event_extra_format, (str, type(None)))
        assert isinstance(event_extra, (bytes, type(None)))
        assert isinstance(amt, int)
        assert amt >= 0

        if amt == 0:
            return

        self.set_earliest(earliest_key, unix_date)

        basic = self.get_for_key(
            basic_key_format.format(unix_date=unix_date).encode("utf-8")
        )
        event_bytes = event.encode("utf-8")
        basic[event_bytes] = basic.get(event_bytes, 0) + amt

        if event_extra is not None:
            assert event_extra_format is not None

            extra = self.get_for_key(
                event_extra_format.format(unix_date=unix_date, event=event).encode(
                    "utf-8"
                )
            )
            extra[event_extra] = extra.get(event_extra, 0) + amt

    def merge_with(self, other: "RedisStatsPreparer") -> None:
        """Merges all the stats from the other preparer into this one. Primarily
        useful when subclassing rather than composing to add utility functions, but
        now you want multiple preparers to be able to be used together
        """
        for key, updates in other.stats.items():
            data = self.get_for_key(key)
            for subkey, amt in updates.items():
                data[subkey] = data.get(subkey, 0) + amt

        for key, earliest in other.earliest_keys.items():
            self.set_earliest(key, earliest)

    async def write_earliest(self, pipe: AsyncioRedisClient) -> None:
        """Writes the earliest updates to the given pipe using the set_if_lower script"""
        for key, val in self.earliest_keys.items():
            await set_if_lower(pipe, key, val)

    async def write_increments(self, pipe: AsyncioRedisClient) -> None:
        """Writes the stat increments to the given pipe"""
        for key, updates in self.stats.items():
            for subkey, amt in updates.items():
                await pipe.hincrby(key, subkey, amt)  # type: ignore

    async def store(self, itgs: Itgs) -> None:
        """Stores the prepared stats in redis within their own transaction"""
        if not self.stats:
            return

        redis = await itgs.redis()

        async def _prep(force: bool):
            await ensure_set_if_lower_script_exists(redis, force=force)

        async def _func():
            async with redis.pipeline() as pipe:
                pipe.multi()
                await self.write_earliest(pipe)
                await self.write_increments(pipe)
                await pipe.execute()

        await run_with_prep(_prep, _func)
