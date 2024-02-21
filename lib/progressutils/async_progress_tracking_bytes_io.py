from typing import Optional, Union
from file_service import AsyncReadableBytesIO, AsyncWritableBytesIO
from itgs import Itgs
import asyncio
import logging
from lib.progressutils.progress_helper import ProgressHelper


class AsyncProgressTrackingWritableBytesIO:
    """Writes to the delegate asynchronously, while reporting progress
    in a non-blocking manner to the given job progress uid. Acts as an
    non-reentrant async context manager

    This satisfies the AsyncWritableBytesIO protocol. Eats errors related
    to progress reporting, but not to writing to the delegate.
    """

    def __init__(
        self,
        itgs: Itgs,
        /,
        *,
        job_progress_uid: Optional[str],
        expected_file_size: Optional[int],
        delegate: AsyncWritableBytesIO,
        message: str,
    ):
        self.itgs = itgs
        """The integrations to (re)use"""
        self.job_progress_uid = job_progress_uid
        """The uid of the job progress to update, or None for no progress reporting"""
        self.expected_file_size = expected_file_size
        """The expected size of the file in bytes, if known, or None if not known. 
        When available, allows for a progress bar instead of a spinner
        """
        self.delegate = delegate
        """The delegate to write to"""
        self.message = message
        """The message for the progress updates"""
        self.progress_helper = ProgressHelper(itgs, job_progress_uid)
        """Helper to report progress"""
        self.bytes_written: Optional[int] = None
        """None if not aenter'd yet, otherwise the number of bytes written"""
        self.progress_job: Optional[asyncio.Task] = None
        """None if no progress task is running, otherwise, the currently running
        task to update progress
        """

    async def __aenter__(self):
        assert self.bytes_written is None, "not reentrant"
        assert self.progress_job is None, "inconsistent state"
        self.bytes_written = 0
        self.progress_job = asyncio.create_task(self._update_progress(0))
        return self

    async def __aexit__(self, *args):
        assert self.bytes_written is not None, "Not entered"
        self.bytes_written = None
        if self.progress_job is not None:
            await self.progress_job
            self.progress_job = None

    async def _update_progress(self, bytes_written: int) -> None:
        """Updates the progress of the download for the given number of
        bytes written.
        """
        if (
            self.expected_file_size is not None
            and bytes_written <= self.expected_file_size
        ):
            await self.progress_helper.push_progress(
                self.message,
                indicator={
                    "type": "bar",
                    "at": bytes_written,
                    "of": self.expected_file_size,
                },
            )
        else:
            await self.progress_helper.push_progress(
                f"{self.message} ({bytes_written} bytes so far)",
                indicator={"type": "spinner"},
            )

    async def write(self, b: Union[bytes, bytearray], /) -> int:
        """Writes the given bytes to the file-like object"""
        assert self.bytes_written is not None, "not entered"
        written = await self.delegate.write(b)
        self.bytes_written += written

        if self.progress_job is not None and self.progress_job.done():
            exc = self.progress_job.exception()
            if exc is not None:
                logging.warning("job progress update failed", exc_info=exc)
            self.progress_job = None

        if self.progress_job is None:
            self.progress_job = asyncio.create_task(
                self._update_progress(self.bytes_written)
            )
        return written


class AsyncProgressTrackingReadableBytesIO:
    """Reads from the delegate asynchronously, while reporting progress
    in a non-blocking manner to the given job progress uid. Acts as an
    non-reentrant async context manager

    This satisfies the AsyncReadableBytesIO protocol. Eats errors related
    to progress reporting, but not to reading from the delegate.
    """

    def __init__(
        self,
        itgs: Itgs,
        /,
        *,
        job_progress_uid: Optional[str],
        expected_file_size: Optional[int],
        delegate: AsyncReadableBytesIO,
        message: str,
    ):
        self.itgs = itgs
        """The integrations to (re)use"""
        self.job_progress_uid = job_progress_uid
        """The uid of the job progress to update, or None for no progress reporting"""
        self.expected_file_size = expected_file_size
        """The expected size of the file in bytes, if known, or None if not known. 
        When available, allows for a progress bar instead of a spinner
        """
        self.delegate = delegate
        """The delegate to read from"""
        self.message = message
        """The message for the progress updates"""
        self.progress_helper = ProgressHelper(itgs, job_progress_uid)
        """Helper to report progress"""
        self.bytes_read: Optional[int] = None
        """None if not aenter'd yet, otherwise the number of bytes read"""
        self.progress_job: Optional[asyncio.Task] = None
        """None if no progress task is running, otherwise, the currently running
        task to update progress
        """

    async def __aenter__(self):
        assert self.bytes_read is None, "not reentrant"
        assert self.progress_job is None, "inconsistent state"
        self.bytes_read = 0
        self.progress_job = asyncio.create_task(self._update_progress(0))
        return self

    async def __aexit__(self, *args):
        assert self.bytes_read is not None, "Not entered"
        self.bytes_read = None
        if self.progress_job is not None:
            await self.progress_job
            self.progress_job = None

    async def _update_progress(self, bytes_read: int) -> None:
        """Updates the progress of the download for the given number of
        bytes read.
        """
        if (
            self.expected_file_size is not None
            and bytes_read <= self.expected_file_size
        ):
            await self.progress_helper.push_progress(
                self.message,
                indicator={
                    "type": "bar",
                    "at": bytes_read,
                    "of": self.expected_file_size,
                },
            )
        else:
            await self.progress_helper.push_progress(
                f"{self.message} ({bytes_read} bytes so far)",
                indicator={"type": "spinner"},
            )

    async def read(self, n: int) -> bytes:
        """Reads up to n bytes from the file-like object"""
        assert self.bytes_read is not None, "not entered"
        b = await self.delegate.read(n)
        self.bytes_read += len(b)

        if self.progress_job is not None and self.progress_job.done():
            exc = self.progress_job.exception()
            if exc is not None:
                logging.warning("job progress update failed", exc_info=exc)
            self.progress_job = None

        if self.progress_job is None:
            self.progress_job = asyncio.create_task(
                self._update_progress(self.bytes_read)
            )
        return b
