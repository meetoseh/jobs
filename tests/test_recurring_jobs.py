from typing import Optional, Tuple
import unittest

try:
    import helper  # type: ignore
except:
    import tests.helper  # type: ignore
import main  # type: ignore
from recurring_jobs import JobInterval, conditionally_zpopmin, JOBS_HASH
import datetime
import asyncio
from itgs import Itgs
from redis.asyncio import Redis
import pytz

tz = pytz.utc


class Test(unittest.TestCase):
    def test_every_minute(self):
        itvl = JobInterval(tz, seconds=(0,))
        itvl.verify()

        self.assertEqual(itvl.next_runtime_after(0), 60)
        self.assertEqual(itvl.next_runtime_after(1.5), 60)
        self.assertEqual(itvl.next_runtime_after(55.5), 60)
        self.assertEqual(itvl.next_runtime_after(60), 120)

    def test_every_15_seconds(self):
        itvl = JobInterval(tz, seconds=(0, 15, 30, 45))
        itvl.verify()

        self.assertEqual(itvl.next_runtime_after(0), 15)
        self.assertEqual(itvl.next_runtime_after(1.5), 15)
        self.assertEqual(itvl.next_runtime_after(14.5), 15)
        self.assertEqual(itvl.next_runtime_after(15), 30)
        self.assertEqual(itvl.next_runtime_after(16), 30)
        self.assertEqual(itvl.next_runtime_after(55), 60)

    def test_every_half_hour(self):
        itvl = JobInterval(tz, minutes=(0, 30), seconds=(0,))
        itvl.verify()

        self.assertEqual(itvl.next_runtime_after(0), 1800)
        self.assertEqual(itvl.next_runtime_after(1.5), 1800)
        self.assertEqual(itvl.next_runtime_after(1800), 3600)
        self.assertEqual(itvl.next_runtime_after(1801), 3600)

    def test_twice_per_day(self):
        itvl = JobInterval(tz, hours=(0, 12), minutes=(0,), seconds=(0,))
        itvl.verify()

        self.assertEqual(itvl.next_runtime_after(0), 43200)
        self.assertEqual(itvl.next_runtime_after(1.5), 43200)
        self.assertEqual(itvl.next_runtime_after(43200), 86400)
        self.assertEqual(itvl.next_runtime_after(43201), 86400)

    def test_twice_per_month(self):
        itvl = JobInterval(
            tz, days_of_month=(1, 15), hours=(0,), minutes=(0,), seconds=(0,)
        )
        itvl.verify()

        self.assertEqual(
            itvl.next_runtime_after(0),
            datetime.datetime(
                year=1970,
                month=1,
                day=15,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(1.5),
            datetime.datetime(
                year=1970,
                month=1,
                day=15,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=15,
                    hour=0,
                    minute=0,
                    second=1,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=2,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=2,
                    day=1,
                    hour=0,
                    minute=0,
                    second=1,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=2,
                day=15,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )

    def test_twice_per_year(self):
        itvl = JobInterval(
            tz,
            months=(1, 7),
            days_of_month=(1,),
            hours=(0,),
            minutes=(0,),
            seconds=(0,),
        )
        itvl.verify()

        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=1,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=7,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=1,
                    hour=0,
                    minute=0,
                    second=1,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=7,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=7,
                    day=1,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1971,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=7,
                    day=1,
                    hour=0,
                    minute=0,
                    second=1,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1971,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )

    def test_twice_weekly(self):
        itvl = JobInterval(
            tz, days_of_week=("fri", "sun"), hours=(0,), minutes=(0,), seconds=(0,)
        )
        itvl.verify()

        # jan 1st 1970 was a thursday
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=1,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=1,
                day=2,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=1,
                    hour=0,
                    minute=0,
                    second=1,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=1,
                day=2,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=2,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=1,
                day=4,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=2,
                    hour=0,
                    minute=0,
                    second=1,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=1,
                day=4,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )
        self.assertEqual(
            itvl.next_runtime_after(
                datetime.datetime(
                    year=1970,
                    month=1,
                    day=4,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                    tzinfo=tz,
                ).timestamp(),
            ),
            datetime.datetime(
                year=1970,
                month=1,
                day=9,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=tz,
            ).timestamp(),
        )

    def test_conditionally_zpopmin(self):
        async def _inner():
            key = "test_conditionally_zpopmin"
            hash_key = "test_conditionally_zpopmin:hash"
            purg_key = "test_conditionally_zpopmin:purgatory"

            async def czpopmin(
                redis: Redis, key: str, max_score: int
            ) -> Optional[Tuple[bytes, float]]:
                assert hash_key == "test_conditionally_zpopmin:hash"
                return await conditionally_zpopmin(
                    redis, key, max_score, hash_key, purg_key
                )

            async with Itgs() as itgs:
                redis = await itgs.redis()
                try:
                    await redis.zadd(key, mapping={"a": 1, "b": 2, "c": 3})
                    await redis.set(hash_key, bytes(str(JOBS_HASH), "ascii"))
                    self.assertEqual(await redis.zcard(key), 3)
                    self.assertEqual(await redis.scard(purg_key), 0)  # type: ignore
                    self.assertIsNone(await czpopmin(redis, key, 0))
                    self.assertEqual(await redis.zcard(key), 3)
                    self.assertEqual(await redis.scard(purg_key), 0)  # type: ignore
                    self.assertEqual(await czpopmin(redis, key, 1), (b"a", 1.0))
                    self.assertEqual(await redis.zcard(key), 2)
                    self.assertEqual(await redis.scard(purg_key), 1)  # type: ignore
                    members = await redis.smembers(purg_key)  # type: ignore
                    self.assertEqual(members, {b"a"})
                    self.assertIsNone(await czpopmin(redis, key, 1))
                    self.assertEqual(await redis.zcard(key), 2)
                    self.assertEqual(await redis.scard(purg_key), 1)  # type: ignore
                    self.assertEqual(await czpopmin(redis, key, 2), (b"b", 2.0))
                    self.assertEqual(await redis.zcard(key), 1)
                    self.assertEqual(await redis.scard(purg_key), 2)  # type: ignore
                    self.assertEqual(await czpopmin(redis, key, 3), (b"c", 3.0))
                    self.assertEqual(await redis.zcard(key), 0)
                    self.assertEqual(await redis.scard(purg_key), 3)  # type: ignore
                    self.assertIsNone(await czpopmin(redis, key, 3))
                    self.assertEqual(await redis.zcard(key), 0)
                    self.assertEqual(await redis.scard(purg_key), 3)  # type: ignore
                finally:
                    await redis.delete(key)
                    await redis.delete(hash_key)
                    await redis.delete(purg_key)

        asyncio.run(_inner())


if __name__ == "__main__":
    unittest.main()
