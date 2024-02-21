from contextlib import asynccontextmanager
import time
from typing import Optional

from itgs import Itgs
from lib.progressutils.progress_helper import ProgressHelper


class BouncedException(Exception):
    pass


class CustomFailureReasonException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class CustomSuccessReasonException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


@asynccontextmanager
async def success_or_failure_reporter(
    itgs: Itgs,
    *,
    job_progress_uid: Optional[str],
    start_message: str = "processing started",
    success_message: str = "processing finished",
    failure_message: str = "processing failed"
):
    """
    Reports a start message before yielding, success if the block completes
    without raising an exception, and and reports failure if an exception is
    raised unless that exception is "BouncedException", in which case a bounce
    is reported

    Does nothing if job_progress_uid is None.

    Reraises any exception except BounceException or CustomFailureReasonException
    that occurs in the block.

    If CustomFailureReasonException is raised from the block, the exception is
    not reraised and the message is used as the failure message.

    If CustomSuccessReasonException is raised from the block, the exception is
    not reraised and the message is used as the success message.

    For convenience this yields a ProgressHelper, but it can be ignored if not
    desired.
    """
    if job_progress_uid is None:
        try:
            yield ProgressHelper(itgs, job_progress_uid)
        except BouncedException:
            pass
        except CustomFailureReasonException:
            pass
        except CustomSuccessReasonException:
            pass
        return

    jobs = await itgs.jobs()
    try:
        await jobs.push_progress(
            job_progress_uid,
            {
                "type": "started",
                "message": start_message,
                "indicator": None,
                "occurred_at": time.time(),
            },
        )

        yield ProgressHelper(itgs, job_progress_uid)

        await jobs.push_progress(
            job_progress_uid,
            {
                "type": "succeeded",
                "message": success_message,
                "indicator": None,
                "occurred_at": time.time(),
            },
        )
    except BouncedException:
        await jobs.push_progress(
            job_progress_uid,
            {
                "type": "bounce",
                "message": "processing bounced to another worker",
                "indicator": None,
                "occurred_at": time.time(),
            },
        )
    except CustomFailureReasonException as e:
        await jobs.push_progress(
            job_progress_uid,
            {
                "type": "failed",
                "message": e.message,
                "indicator": None,
                "occurred_at": time.time(),
            },
        )
    except CustomSuccessReasonException as e:
        await jobs.push_progress(
            job_progress_uid,
            {
                "type": "succeeded",
                "message": e.message,
                "indicator": None,
                "occurred_at": time.time(),
            },
        )
    except BaseException:
        await jobs.push_progress(
            job_progress_uid,
            {
                "type": "failed",
                "message": failure_message,
                "indicator": None,
                "occurred_at": time.time(),
            },
        )
        raise
