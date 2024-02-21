import time
from typing import Optional

from itgs import Itgs
from jobs import (
    JobProgress,
    JobProgressIndicator,
    JobProgressSpawnedInfo,
    JobProgressType,
)


class ProgressHelper:
    """Convenience class for reporting job progress which handles
    skipping reporting if the job_progress_uid is None and reduces
    repetition on the part of the caller.
    """

    def __init__(self, itgs: Itgs, job_progress_uid: Optional[str]):
        self.itgs = itgs
        self.job_progress_uid = job_progress_uid

    async def push_progress(
        self,
        message: str,
        /,
        *,
        indicator: Optional[JobProgressIndicator] = None,
        type: JobProgressType = "progress",
        spawned: Optional[JobProgressSpawnedInfo] = None,
    ) -> None:
        progress: JobProgress
        if type == "spawned":
            assert (
                spawned is not None
            ), "spawned info must be provided for spawned progress"
            progress = {
                "type": "spawned",
                "message": message,
                "indicator": indicator,
                "occurred_at": time.time(),
                "spawned": spawned,
            }
        else:
            progress = {
                "type": type,
                "message": message,
                "indicator": indicator,
                "occurred_at": time.time(),
            }

        if self.job_progress_uid is None:
            return

        jobs = await self.itgs.jobs()
        await jobs.push_progress(self.job_progress_uid, progress)
