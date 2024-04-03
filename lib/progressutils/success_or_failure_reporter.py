from contextlib import asynccontextmanager
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
    failure_message: str = "processing failed",
    log: bool = False,
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
    desired. If log is true, this progress helper will log all messages and we
    will log the result.
    """
    helper = ProgressHelper(itgs, job_progress_uid, log=log)
    try:
        await helper.push_progress(start_message, type="started")

        yield helper

        await helper.push_progress(success_message, type="succeeded")
    except BouncedException:
        await helper.push_progress(
            "processing bounced to another worker", type="bounce"
        )
    except CustomFailureReasonException as e:
        await helper.push_progress(e.message, type="failed")
    except CustomSuccessReasonException as e:
        await helper.push_progress(e.message, type="succeeded")
    except BaseException:
        await helper.push_progress(failure_message, type="failed")
        raise
