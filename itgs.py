"""This module allows for easily accessing common integrations -
the integration is only loaded upon request.
"""

import json
import random
from typing import Callable, Coroutine, Dict, Literal, Optional
import rqdb
import rqdb.async_connection
import rqdb.logging
import redis.asyncio
import diskcache
import os
from error_middleware import handle_warning
import slack
import jobs
import file_service
import revenue_cat
import asyncio
import twilio.rest
import lib.gender.api
from dataclasses import dataclass
import threading
import logging as logger
import importlib


our_diskcache: diskcache.Cache = diskcache.Cache(
    "tmp/diskcache", eviction_policy="least-recently-stored"
)
"""diskcache does a particularly good job ensuring it's safe to reuse a single Cache object
without having to worry, and doing so offers significant performance gains. In particular,
it's fine if:
- this is built before we are forked
- this is used in different threads
"""

ItgsCleanupIdentifier = Literal[
    "conn",
    "redis_main",
    "slack",
    "jobs",
    "file_service",
    "revenue_cat",
    "twilio",
    "gender_api",
]


@dataclass
class _ItgsGuard:
    tid: int
    """thread id that aentered"""
    pid: int
    """process id that aentered"""


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

        self._twilio: Optional[twilio.rest.Client] = None
        """the twilio connection if it had been opened"""

        self._gender_api: Optional[lib.gender.api.GenderAPI] = None
        """the gender api connection if it had been opened"""

        self._closures: Dict[ItgsCleanupIdentifier, Callable[["Itgs"], Coroutine]] = (
            dict()
        )
        """functions to run on __aexit__ to cleanup opened resources"""

        self._guard: Optional[_ItgsGuard] = None
        """If we've been aentered, guard info, otherwise none. Used to protect
        against the following issue very easy to make in python:

        ```py
        def foo():
          async with Itgs() as itgs:
            ...
          redis = await itgs.redis()  # leaks without guard
        ```
        """

    async def __aenter__(self) -> "Itgs":
        """allows support as an async context manager"""
        async with self._lock:
            if self._guard is not None:
                raise ValueError("Cannot __aenter__ twice")
            self._guard = _ItgsGuard(tid=threading.get_ident(), pid=os.getpid())
        return self

    async def _check_guard_with_lock(self) -> None:
        assert self._guard is not None
        pid = os.getpid()
        assert pid == self._guard.pid, f"{pid=} {self._guard.pid=}"
        tid = threading.get_ident()
        assert tid == self._guard.tid, f"{tid=} {self._guard.tid=}"

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """closes any managed resources"""
        async with self._lock:
            await self._check_guard_with_lock()
            for closure in self._closures.values():
                await closure(self)
            self._closures = dict()
            self._guard = None

    async def conn(self) -> rqdb.async_connection.AsyncConnection:
        """Gets or creates and initializes the rqdb connection.
        The connection will be closed when the itgs is closed
        """
        if self._conn is not None:
            return self._conn

        async with self._lock:
            await self._check_guard_with_lock()
            if self._conn is not None:
                return self._conn

            rqlite_ips = os.environ["RQLITE_IPS"].split(",")
            if not rqlite_ips:
                raise ValueError("RQLITE_IPS not set -> cannot connect to rqlite")

            async def cleanup(me: "Itgs") -> None:
                if me._conn is not None:
                    await me._conn.__aexit__(None, None, None)
                    me._conn = None

            bknd_tasks = set()

            def on_slow_query(
                info: rqdb.logging.QueryInfo,
                /,
                *,
                duration_seconds: float,
                host: str,
                response_size_bytes: int,
                started_at: float,
                ended_at: float,
            ):
                task = asyncio.create_task(
                    handle_warning(
                        "jobs:slow_query",
                        f"query to {host} took {duration_seconds:.3f}s to return {response_size_bytes} bytes:\n\n```\n{json.dumps(info.operations, indent=1)}\n```",
                    )
                )
                bknd_tasks.add(task)
                task.add_done_callback(lambda _: bknd_tasks.remove(task))

            self._closures["conn"] = cleanup
            c = rqdb.connect_async(
                hosts=rqlite_ips,
                log=rqdb.LogConfig(
                    slow_query={
                        "enabled": True,
                        "threshold_seconds": 1,
                        "method": on_slow_query,
                    },
                ),
            )
            await c.__aenter__()
            self._conn = c

        return self._conn

    async def redis(self) -> redis.asyncio.Redis:
        """returns or creates and returns the main redis connection"""
        if self._redis_main is not None:
            return self._redis_main

        async with self._lock:
            await self._check_guard_with_lock()
            if self._redis_main is not None:
                return self._redis_main

            redis_ips = os.environ["REDIS_IPS"].split(",")
            if not redis_ips:
                raise ValueError(
                    "REDIS_IPs is not set and so a redis connection cannot be established"
                )

            random.shuffle(redis_ips)

            async def cleanup(me: "Itgs") -> None:
                if me._redis_main is not None:
                    await me._redis_main.close()
                    me._redis_main = None

            self._closures["redis_main"] = cleanup
            for idx, ip in enumerate(redis_ips):
                sentinel_conn = redis.asyncio.Redis(
                    host=ip,
                    port=26379,
                    socket_connect_timeout=3,
                    single_connection_client=True,
                )
                try:
                    response = await sentinel_conn.execute_command(
                        "SENTINEL", "MASTER", "mymaster"
                    )
                    assert isinstance(response, (list, tuple)), response
                    assert len(response) % 2 == 0, response

                    master_ip: Optional[str] = None
                    master_port: Optional[int] = None
                    num_other_sentinels: Optional[int] = None
                    for entry_idx in range(0, len(response), 2):
                        entry = (response[entry_idx], response[entry_idx + 1])
                        assert isinstance(entry[0], bytes), response

                        key = entry[0]
                        if key == b"ip":
                            assert isinstance(entry[1], bytes), response
                            master_ip = entry[1].decode("utf-8")
                        elif key == b"port":
                            assert isinstance(entry[1], bytes), response
                            master_port = int(entry[1])
                        elif key == b"num-other-sentinels":
                            assert isinstance(entry[1], bytes), response
                            num_other_sentinels = int(entry[1])

                    if (
                        master_ip is None
                        or master_port is None
                        or num_other_sentinels is None
                    ):
                        raise ValueError(f"Could not parse {response=}")

                    assert num_other_sentinels >= (
                        len(redis_ips) // 2
                    ), f"{num_other_sentinels=}, {len(redis_ips)=}"

                    self._redis_main = redis.asyncio.Redis(
                        host=master_ip, port=master_port
                    )
                    return self._redis_main
                except:
                    if idx == len(redis_ips) - 1:
                        raise
                finally:
                    await sentinel_conn.close()

            raise ValueError("Could not find a master redis")

    async def slack(self) -> slack.Slack:
        """gets or creates and gets the slack connection"""
        if self._slack is not None:
            return self._slack

        async with self._lock:
            await self._check_guard_with_lock()
            if self._slack is not None:
                return self._slack

            s = slack.Slack()
            await s.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await s.__aexit__(None, None, None)
                me._slack = None

            self._closures["slack"] = cleanup
            self._slack = s

        return self._slack

    async def jobs(self) -> jobs.Jobs:
        """gets or creates the jobs connection"""
        if self._jobs is not None:
            return self._jobs

        _redis = await self.redis()
        async with self._lock:
            await self._check_guard_with_lock()
            if self._jobs is not None:
                return self._jobs

            allowed_job_categories = list(
                jobs.JobCategory(int(s.strip()))
                for s in os.environ["OSEH_JOB_CATEGORIES"].split(",")
            )
            j = jobs.Jobs(
                _redis,
                allowed_job_categories=allowed_job_categories,
                get_job_category=get_job_category,
            )
            await j.__aenter__()
            await j.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await j.__aexit__(None, None, None)
                me._jobs = None

            self._closures["jobs"] = cleanup
            self._jobs = j

        return self._jobs

    async def files(self) -> file_service.FileService:
        """gets or creates the file service for large binary blobs"""
        if self._file_service is not None:
            return self._file_service

        async with self._lock:
            await self._check_guard_with_lock()
            if self._file_service is not None:
                return self._file_service

            default_bucket = os.environ["OSEH_S3_BUCKET_NAME"]

            if os.environ.get("ENVIRONMENT", default="production") == "dev":
                root = os.environ["OSEH_S3_LOCAL_BUCKET_PATH"]
                fs = file_service.LocalFiles(root, default_bucket=default_bucket)
            else:
                fs = file_service.S3(default_bucket=default_bucket)

            await fs.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await fs.__aexit__(None, None, None)
                me._file_service = None

            self._closures["file_service"] = cleanup
            self._file_service = fs

        return self._file_service

    async def local_cache(self) -> diskcache.Cache:
        """gets or creates the local cache for storing files transiently on this instance"""
        async with self._lock:
            await self._check_guard_with_lock()
        return our_diskcache

    async def revenue_cat(self) -> revenue_cat.RevenueCat:
        """gets or creates the revenue cat connection"""
        if self._revenue_cat is not None:
            return self._revenue_cat

        async with self._lock:
            await self._check_guard_with_lock()
            if self._revenue_cat is not None:
                return self._revenue_cat

            sk = os.environ["OSEH_REVENUE_CAT_SECRET_KEY"]
            stripe_pk = os.environ["OSEH_REVENUE_CAT_STRIPE_PUBLIC_KEY"]
            playstore_pk = os.environ["OSEH_REVENUE_CAT_GOOGLE_PLAY_PUBLIC_KEY"]
            appstore_pk = os.environ["OSEH_REVENUE_CAT_APPLE_PUBLIC_KEY"]

            rc = revenue_cat.RevenueCat(
                sk=sk,
                stripe_pk=stripe_pk,
                playstore_pk=playstore_pk,
                appstore_pk=appstore_pk,
            )

            await rc.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await rc.__aexit__(None, None, None)
                me._revenue_cat = None

            self._closures["revenue_cat"] = cleanup
            self._revenue_cat = rc

        return self._revenue_cat

    async def twilio(self) -> twilio.rest.Client:
        """gets or creates the twilio connection"""
        if self._twilio is not None:
            return self._twilio

        async with self._lock:
            await self._check_guard_with_lock()
            if self._twilio is not None:
                return self._twilio

            sid = os.environ["OSEH_TWILIO_ACCOUNT_SID"]
            token = os.environ["OSEH_TWILIO_AUTH_TOKEN"]

            tw = twilio.rest.Client(sid, token)

            async def cleanup(me: "Itgs") -> None:
                me._twilio = None

            self._closures["twilio"] = cleanup
            self._twilio = tw

        return self._twilio

    async def gender_api(self) -> lib.gender.api.GenderAPI:
        """gets or creates the GenderAPI connection"""
        if self._gender_api is not None:
            return self._gender_api

        async with self._lock:
            await self._check_guard_with_lock()
            if self._gender_api is not None:
                return self._gender_api

            api_key = os.environ["OSEH_GENDER_API_KEY"]

            gender = lib.gender.api.GenderAPI(api_key)
            await gender.__aenter__()

            async def cleanup(me: "Itgs") -> None:
                await gender.__aexit__(None, None, None)
                me._gender_api = None

            self._closures["gender_api"] = cleanup
            self._gender_api = gender

        return self._gender_api

    async def reconnect_redis(self) -> None:
        """If we are connected to redis, closes the connection. This will
        also close any other connections that depend on it. They connections
        will be reinitialized when they are next requested.
        """
        if self._redis_main is None:
            return

        async with self._lock:
            await self._check_guard_with_lock()
            if self._redis_main is None:
                return

            if self._jobs is not None:
                await self._closures["jobs"](self)
                del self._closures["jobs"]

            await self._closures["redis_main"](self)
            del self._closures["redis_main"]

    async def ensure_redis_liveliness(self) -> None:
        """Tries to ping the redis connection; if it fails, reconnects"""
        logger.debug("Checking redis connection for liveliness...")
        redis = await self.redis()
        try:
            await redis.ping()
            logger.debug("Redis connection is alive")
        except:
            logger.debug("Redis connection is dead; reconnecting...")
            await self.reconnect_redis()


def get_job_category(name: str) -> jobs.JobCategory:
    return importlib.import_module(name).category
