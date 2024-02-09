import time
from typing import Optional

from itgs import Itgs
from jobs import JobProgressIndicator, JobProgressType


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
    ) -> None:
        if self.job_progress_uid is None:
            return

        jobs = await self.itgs.jobs()
        await jobs.push_progress(
            self.job_progress_uid,
            {
                "type": type,
                "message": message,
                "indicator": indicator,
                "occurred_at": time.time(),
            },
        )
