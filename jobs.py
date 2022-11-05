import redis.asyncio
import json
import time
import logging
from typing import Optional, TypedDict


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
    """interface for queueing and retreiving jobs
    acts as an asynchronous context manager"""

    def __init__(self, conn: redis.asyncio.Redis) -> None:
        """initializes a new interface for queueing and retreiving jobs

        Args:
            conn (redis.asyncio.Redis): the redis connection to use
        """
        self.conn: redis.asyncio.Redis = conn
        """the redis connection containing the jobs queue"""

        self.queue_key: bytes = b"jobs:hot"
        """the key for the list in redis"""

    async def __aenter__(self) -> "Jobs":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    async def enqueue(self, name: str, **kwargs) -> None:
        """queues the job with the given name and key word arguments

        the job is run as soon as possible, and is not retried regardless of success.

        Args:
            name (str): the name of the job which corresponds to the import path in the jobs module
                e.g., 'runners.charge' corresponds to the excecute function in jobs/charge.py
                relative to the jobs root directory
            kwargs (dict): the keyword arguments to pass to the job; must be json serializable
                the jobs will automatically be sent the integrations and graceful death handler
        """
        job = {"name": name, "kwargs": kwargs, "queued_at": time.time()}
        job_serd = json.dumps(job)
        await self.conn.rpush(self.queue_key, job_serd.encode("utf-8"))

    async def retrieve(self, timeout: float) -> Optional[Job]:
        """blocking retrieve of the oldest job in the queue, if there is one

        Args:
            timeout (float): maximum time in seconds to wait for a job to be enqueued

        Returns:
            (Job, None): The oldest job, if there is one
        """
        response: Optional[tuple] = await self.conn.blpop(
            self.queue_key, timeout=timeout
        )
        if response is None:
            return None
        job_serd_and_encoded: bytes = response[1]
        job_serd = job_serd_and_encoded.decode("utf-8")
        logging.info(repr(job_serd))
        job = json.loads(job_serd)
        return job
