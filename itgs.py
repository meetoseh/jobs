"""This module allows for easily accessing common integrations -
the integration is only loaded upon request.
"""
from typing import Callable, Coroutine, List, Optional
import rqdb
import rqdb.async_connection
import redis.asyncio
import diskcache
import os
import slack
import jobs
import file_service
import revenue_cat
import asyncio


our_diskcache: diskcache.Cache = diskcache.Cache(
    "tmp/diskcache", eviction_policy="least-recently-stored"
)
"""diskcache does a particularly good job ensuring it's safe to reuse a single Cache object
without having to worry, and doing so offers significant performance gains. In particular,
it's fine if:
- this is built before we are forked
- this is used in different threads
"""


class Itgs:
    """The collection of integrations available. Acts as an
    async context manager
    """

    def __init__(self) -> None:
        """Initializes a new integrations with nothing loaded.
        Must be __aenter__ 'd and __aexit__'d.
        """
        self._lock: asyncio.Lock = asyncio.Lock()
        """A lock for when mutating our state"""

        self._conn: Optional[rqdb.async_connection.AsyncConnection] = None
        """the rqlite connection, if it has been opened"""

        self._sentinel: Optional[redis.asyncio.Sentinel] = None
        """the redis sentinel connection, if it has been opened"""

        self._redis_main: Optional[redis.asyncio.Redis] = None
        """the redis main connection, if it has been detected via the sentinel"""

        self._slack: Optional[slack.Slack] = None
        """the slack connection if it has been opened"""

        self._jobs: Optional[jobs.Jobs] = None
        """the jobs connection if it had been opened"""

        self._file_service: Optional[file_service.FileService] = None
        """the file service connection if it had been opened"""

        self._revenue_cat: Optional[revenue_cat.RevenueCat] = None
        """the revenue cat connection if it had been opened"""

        self._closures: List[Callable[["Itgs"], Coroutine]] = []
        """functions to run on __aexit__ to cleanup opened resources"""

    async def __aenter__(self) -> "Itgs":
        """allows support as an async context manager"""
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """closes any managed resources"""
        async with self._lock:
            for closure in self._closures:
                await closure(self)
            self._closures = []

    async def conn(self) -> rqdb.async_connection.AsyncConnection:
        """Gets or creates and initializes the rqdb connection.
        The connection will be closed when the itgs is closed
        """
        if self._conn is not None:
            return self._conn

        async with self._lock:
            if self._conn is not None:
                return self._conn

            rqlite_ips = os.environ.get("RQLITE_IPS").split(",")
            if not rqlite_ips:
                raise ValueError("RQLITE_IPS not set -> cannot connect to rqlite")

            async def cleanup(me: "Itgs") -> None:
                if me._conn is not None:
                    await me._conn.__aexit__(None, None, None)
                    me._conn = None

            self._closures.append(cleanup)
            self._conn = rqdb.connect_async(hosts=rqlite_ips)
            await self._conn.__aenter__()

        return self._conn

    async def redis(self) -> redis.asyncio.Redis:
        """returns or cerates and returns the main redis connection"""
        if self._redis_main is not None:
            return self._redis_main

        async with self._lock:
            if self._redis_main is not None:
                return self._redis_main

            redis_ips = os.environ.get("REDIS_IPS").split(",")
            if not redis_ips:
                raise ValueError(
                    "REDIS_IPs is not set and so a redis connection cannot be established"
                )

            async def cleanup(me: "Itgs") -> None:
                if me._redis_main is not None:
                    await me._redis_main.close()
                    me._redis_main = None

                me._sentinel = None

            self._closures.append(cleanup)
            self._sentinel = redis.asyncio.Sentinel(
                sentinels=[(ip, 26379) for ip in redis_ips],
                min_other_sentinels=len(redis_ips) // 2,
            )
            self._redis_main = self._sentinel.master_for("mymaster")
        return self._redis_main

    async def slack(self) -> slack.Slack:
        """gets or creates and gets the slack connection"""
        if self._slack is not None:
            return self._slack

        async with self._lock:
            if self._slack is not None:
                return self._slack

            self._slack = slack.Slack()
            await self._slack.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await me._slack.__aexit__(None, None, None)
                me._slack = None

            self._closures.append(cleanup)

        return self._slack

    async def jobs(self) -> jobs.Jobs:
        """gets or creates the jobs connection"""
        if self._jobs is not None:
            return self._jobs

        async with self._lock:
            if self._jobs is not None:
                return self._jobs

            self._jobs = jobs.Jobs(await self.redis())
            await self._jobs.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await me._jobs.__aexit__(None, None, None)
                me._jobs = None

            self._closures.append(cleanup)

        return self._jobs

    async def files(self) -> file_service.FileService:
        """gets or creates the file service for large binary blobs"""
        if self._file_service is not None:
            return self._file_service

        async with self._lock:
            if self._file_service is not None:
                return self._file_service

            default_bucket = os.environ["OSEH_S3_BUCKET_NAME"]

            if os.environ.get("ENVIRONMENT", default="production") == "dev":
                root = os.environ["OSEH_S3_LOCAL_BUCKET_PATH"]
                self._file_service = file_service.LocalFiles(
                    root, default_bucket=default_bucket
                )
            else:
                self._file_service = file_service.S3(default_bucket=default_bucket)

            await self._file_service.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await me._file_service.__aexit__(None, None, None)
                me._file_service = None

            self._closures.append(cleanup)

        return self._file_service

    async def local_cache(self) -> diskcache.Cache:
        """gets or creates the local cache for storing files transiently on this instance"""
        return our_diskcache

    async def revenue_cat(self) -> revenue_cat.RevenueCat:
        """gets or creates the revenue cat connection"""
        if self._revenue_cat is not None:
            return self._revenue_cat

        async with self._lock:
            if self._revenue_cat is not None:
                return self._revenue_cat

            sk = os.environ["OSEH_REVENUE_CAT_SECRET_KEY"]
            stripe_pk = os.environ["OSEH_REVENUE_CAT_STRIPE_PUBLIC_KEY"]

            self._revenue_cat = revenue_cat.RevenueCat(sk=sk, stripe_pk=stripe_pk)

            await self._revenue_cat.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await me._revenue_cat.__aexit__(None, None, None)
                me._revenue_cat = None

            self._closures.append(cleanup)

        return self._revenue_cat
