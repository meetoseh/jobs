import redis.asyncio
import json
import time
from typing import (
    Callable,
    FrozenSet,
    Iterable,
    List,
    Literal,
    Optional,
    TypedDict,
    Union,
)
from enum import IntEnum
from redis_helpers.push_job_progress import (
    ensure_push_job_progress_script_exists,
    push_job_progress,
)

from redis_helpers.run_with_prep import run_with_prep


class JobProgressIndicatorBar(TypedDict):
    """describes a hint that a progress bar indicator should be displayed"""

    type: Literal["bar"]
    """discriminative field"""
    at: Union[int, float]
    """How much progress has been made, out of `of`"""
    of: Union[int, float]
    """How much progress is needed to complete the job or step"""


class JobProgressIndicatorSpinner(TypedDict):
    """describes a hint that a progress spinner indicator should be displayed"""

    type: Literal["spinner"]
    """discriminative field"""


class JobProgressIndicatorFinal(TypedDict):
    """describes a hint that no more messages will be sent"""

    type: Literal["final"]
    """discriminative field"""


JobProgressIndicator = Union[
    JobProgressIndicatorBar, JobProgressIndicatorSpinner, JobProgressIndicatorFinal
]


JobProgressTypeSimple = Literal[
    "queued", "started", "bounce", "progress", "failed", "succeeded"
]


class JobProgressSimple(TypedDict):
    """describes a simple job progress message"""

    type: JobProgressTypeSimple
    """the type of progress message"""
    message: str
    """the message to display to the user"""
    indicator: Optional[JobProgressIndicator]
    """a hint about how to display the progress or None if no indicator
    should be displayed
    """
    occurred_at: float
    """the time when the progress message was created"""


class JobProgressSpawnedInfo(TypedDict):
    uid: str
    """the uid of the spawned job; this is a job progress uid"""
    name: str
    """a hint for the name of this job for the client, e.g., 'extract thumbnail'"""


class JobProgressSpawned(TypedDict):
    """describes a job progress message which indicates theres another
    related job with a different job progress uid
    """

    type: Literal["spawned"]
    """the type of progress message"""
    message: str
    """the message to display to the user"""
    spawned: JobProgressSpawnedInfo
    """info about the spawned job"""
    indicator: Optional[JobProgressIndicator]
    """a hint about how to display the progress or None if no indicator"""
    occurred_at: float
    """the time when the progress message was created"""


JobProgress = Union[JobProgressSimple, JobProgressSpawned]
JobProgressType = Union[JobProgressTypeSimple, Literal["spawned"]]


class JobCategory(IntEnum):
    """The category of a job. Stored exclusively in the job runner source control under
    the "category" module variable. Only instances which have the same category in their
    configuration can run the job.
    """

    HIGH_RESOURCE_COST = 1
    """A high resource cost job, like processing an a large image. A reduced number of
    servers will run these jobs to ensure some servers are available at a lower latency,
    so processing may be delayed.
    """

    LOW_RESOURCE_COST = 2
    """A low resource cost job, like processing a profile picture. Run on all instances,
    so tends to complete as quickly as is possible.
    """


class Job(TypedDict):
    """describes a job dictionary"""

    name: str
    """the name of the job which corresponds to the import path in the jobs module
    e.g., 'runners.charge' corresponds to the excecute function in jobs/charge.py
    relative to the jobs root directory
    """
    kwargs: dict
    """the keyword arguments to pass to the job; must be json serializable
    the jobs will automatically be sent the integrations and graceful death handler
    """
    queued_at: float
    """the time when the job was enqueued"""


class Jobs:
    """interface for queueing and retreiving jobs acts as an asynchronous context
    manager

    Jobs are always sent initially to the jobs:hot queue. Instances will attempt
    to pick jobs up from the jobs:hot:{category} queues in a random order,
    unless all of them are empty, in which case they will retrieve from the
    jobs:hot queue. After retrieving a job, if it's the wrong category for this
    instance (either because it came from jobs:hot or its category changed since
    it was queued), it is returned to the end of the category-specific queue.

    This means the number of categorizations is the most important factor in
    job dequeue throughput, but doesn't effect job enqueue throughput. Since
    job dequeue bottlenecks are typically resolvable by batching, this should
    result in a good balance between throughput and categorization flexibility.
    """

    def __init__(
        self,
        conn: redis.asyncio.Redis,
        *,
        allowed_job_categories: Iterable[JobCategory] = tuple(),
        get_job_category: Callable[
            [str], JobCategory
        ] = lambda x: JobCategory.LOW_RESOURCE_COST,
    ) -> None:
        """initializes a new interface for queueing and retreiving jobs

        Args:
            conn (redis.asyncio.Redis): the redis connection to use
            allowed_job_categories (list[JobCategory]): the categories of jobs
                that are useful to this instance when retrieving. Ignored except
                when retrieving jobs. If jobs in multiple categories are available,
                this prefers jobs in categories with a lower index, but only if another
                instance has already processed the jobs categorization and it's still
                accurate. Generally, jobs are processed in time-order.
            get_job_category (Callable[[str], JobCategory]): a function which takes
                the name of a job and returns the category of the job. Used to determine
                whether a job is allowed to be retrieved by this instance. Ignored except
                when retrieving jobs. By default always returns low resource cost.
        """
        self.conn: redis.asyncio.Redis = conn
        """the redis connection containing the jobs queue"""

        self.queue_key: bytes = b"jobs:hot"
        """the key for the main list in redis"""

        self.allowed_job_categories: FrozenSet[JobCategory] = frozenset(
            allowed_job_categories
        )
        """The categories of jobs that are useful to this instance when retrieving."""

        self.all_queues: List[bytes] = [
            f"jobs:hot:{category.value}".encode("utf-8")
            for category in allowed_job_categories
        ] + [self.queue_key]
        """all of the queues that are retrieved from, in the order they are retrieved from"""

        self.get_job_category: Callable[[str], JobCategory] = get_job_category
        """A function which takes the name of a job and returns the category of the job."""

    async def __aenter__(self) -> "Jobs":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    async def enqueue(self, name: str, **kwargs) -> None:
        """queues the job with the given name and key word arguments

        the job is run as soon as possible, and is not retried regardless of success.

        Args:
            name (str): the name of the job which corresponds to the import path in the jobs module
                e.g., 'runners.example' corresponds to the execute function in runners/example.py
                relative to the jobs root directory
            kwargs (dict): the keyword arguments to pass to the job; must be json serializable
                the jobs will automatically be sent the integrations and graceful death handler
        """
        await self.enqueue_in_pipe(self.conn, name, **kwargs)

    async def enqueue_with_progress(
        self, name: str, progress_uid: str, /, **kwargs
    ) -> None:
        """queues the job with the given name and key word arguments, and also
        pushes the initial progress message to the progress event list with the
        given uid.

        the job is run as soon as possible, and is not retried regardless of success.

        the job is not passed the progress uid automatically, but will need it

        Args:
            name (str): the name of the job which corresponds to the import path in the jobs module
                e.g., 'runners.charge' corresponds to the execute function in jobs/charge.py
                relative to the jobs root directory
            progress_uid (str): the unique identifier for the progress event list
            kwargs (dict): the keyword arguments to pass to the job; must be json serializable
                the jobs will automatically be sent the integrations and graceful death handler
        """

        conn = self.conn
        progress: JobProgress = {
            "type": "queued",
            "message": "waiting for an available worker",
            "indicator": {"type": "spinner"},
            "occurred_at": time.time(),
        }

        async def _prepare(force: bool):
            await self.prepare_progress(conn, force=force)

        async def _execute():
            async with conn.pipeline() as pipe:
                pipe.multi()
                await self.push_progress_in_pipe(
                    pipe, progress_uid=progress_uid, progress=progress
                )
                await self.enqueue_in_pipe(pipe, name, **kwargs)
                await pipe.execute()

        await run_with_prep(_prepare, _execute)

    async def enqueue_in_pipe(
        self, pipe: redis.asyncio.Redis, name: str, **kwargs
    ) -> None:
        """queues the job with the given name and key word arguments, using the
        specified redis connection. This is primarily for batching jobs or for
        performing other redis operations in the same transaction.

        the job is run as soon as possible, and is not retried regardless of success.

        Args:
            pipe (redis.asyncio.Redis): the redis connection to use
            name (str): the name of the job which corresponds to the import path in the jobs module
                e.g., 'runners.example' corresponds to the execute function in runners/example.py
                relative to the jobs root directory
            kwargs (dict): the keyword arguments to pass to the job; must be json serializable
                the jobs will automatically be sent the integrations and graceful death handler
        """
        job = {"name": name, "kwargs": kwargs, "queued_at": time.time()}
        job_serd = json.dumps(job)
        await pipe.rpush(self.queue_key, job_serd.encode("utf-8"))  # type: ignore

    async def prepare_progress(self, conn: redis.asyncio.Redis, *, force: bool) -> None:
        """Ensures the required scripts for `push_progress_in_pipe` are loaded.

        Args:
            conn (redis.asyncio.Redis): the redis connection to use
            force (bool): whether to force the script to be loaded or verified to
                be loaded even if we have done so recently
        """
        await ensure_push_job_progress_script_exists(conn, force=force)

    async def push_progress_in_pipe(
        self, pipe: redis.asyncio.Redis, progress_uid: str, progress: JobProgress
    ) -> None:
        """Pushes the given job progress message to the progress event list with the
        given uid. This is primarily for batching progress messages or for
        performing other redis operations in the same transaction.

        This may fail with NOSCRIPT if `prepare_progress` has not been called or
        the script was deleted.
        """
        await push_job_progress(
            pipe, progress_uid.encode("utf-8"), json.dumps(progress).encode("utf-8")
        )

    async def push_progress(self, progress_uid: str, progress: JobProgress) -> None:
        """Pushes the given job progress message to the progress event list with the
        given uid.
        """
        conn = self.conn

        async def _prepare(force: bool):
            await self.prepare_progress(conn, force=force)

        async def _execute():
            await self.push_progress_in_pipe(conn, progress_uid, progress)

        await run_with_prep(_prepare, _execute)

    async def retrieve(self, timeout: float) -> Optional[Job]:
        """blocking retrieve of the oldest job in the queue, if there is one, respecting
        our categorization limits.

        Args:
            timeout (float): maximum time in seconds to wait for a job to be enqueued.
                A value of zero blocks indefinitely.

        Returns:
            (Job, None): The oldest job, if there is one
        """
        started_at = time.time() if timeout != 0 else 0
        while True:
            if timeout != 0:
                remaining_timeout = timeout - (time.time() - started_at)
                if remaining_timeout <= 0:
                    return None
            else:
                remaining_timeout = 0

            response: Optional[tuple] = await self.conn.blpop(  # type: ignore
                self.all_queues, timeout=int(remaining_timeout)  # type: ignore
            )
            if response is None:
                return None
            job_serd_and_encoded: bytes = response[1]
            job_serd = job_serd_and_encoded.decode("utf-8")
            job: Job = json.loads(job_serd)

            try:
                category: JobCategory = self.get_job_category(job["name"])
                if not isinstance(category, JobCategory):
                    raise TypeError(
                        f"get_job_category must return a JobCategory, not {type(category)=}, {category=}"
                    )
            except Exception:
                raise

            if category not in self.allowed_job_categories:
                await self.conn.rpush(  # type: ignore
                    f"jobs:hot:{category.value}".encode("utf-8"), job_serd_and_encoded  # type: ignore
                )
                continue

            return job
