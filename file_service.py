from abc import ABC
from io import BytesIO
from typing import Union
import aioboto3
import botocore.exceptions
import aiofiles
import os
import logging
from temp_files import temp_file
import io


class AsyncReadableBytesIO(ABC):
    """A type that represents a stream that can be read asynchronously"""

    async def read(self, n: int) -> bytes:
        """Reads n bytes from the file-like object"""
        raise NotImplementedError()


class AsyncWritableBytesIO(ABC):
    """A type that represents a stream that can be written asynchronously"""

    async def write(self, b: Union[bytes, bytearray]) -> int:
        """Writes the given bytes to the file-like object"""
        raise NotImplementedError()


class FileService(ABC):
    """Describes something suitable for storing large binary blobs as key-value pairs,
    where the keys are formed from two strings (a bucket and a key) and the values
    are binary blobs.
    """

    @property
    def default_bucket(self) -> str:
        raise NotImplementedError()

    async def __aenter__(self) -> "FileService":
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc, tb):
        raise NotImplementedError()

    async def upload(
        self,
        f: Union[BytesIO, AsyncReadableBytesIO],
        *,
        bucket: str,
        key: str,
        sync: bool,
    ) -> None:
        """Uploads the file from the given file-like object to the given bucket and key.
        This will overwrite any existing file at that location.

        sync should be true if f is a synchronous file-like object, and false if it is
        an asynchronous file-like object.
        """
        raise NotImplementedError()

    async def download(
        self,
        f: Union[BytesIO, AsyncWritableBytesIO],
        *,
        bucket: str,
        key: str,
        sync: bool,
    ) -> bool:
        """Downloads the file at the given bucket and key to the given file-like object.

        sync should be true if f is a synchronous file-like object, and false if it is
        an asynchronous file-like object.

        Returns:
            True if the file was downloaded, False if it did not exist.
        """
        raise NotImplementedError()

    async def delete(self, *, bucket: str, key: str) -> bool:
        """Deletes the file at the given bucket and key. Returns True if the file was
        deleted, False if it did not exist.
        """
        raise NotImplementedError()


class S3:
    """Adapts S3 via aioboto3 to act as a file service.

    Acts as an async context manager. An instance is typically retrieved through
    `files = await itgs.files()` when in production mode, whereas in dev mode
    that will return a `LocalFiles` instance.
    """

    def __init__(self, default_bucket: str) -> None:
        self.default_bucket = default_bucket
        """The recommended default bucket"""

        self._session = None
        """The session object, if we have one, i.e., if we have been aenter'd"""

        self.__s3_creator = None
        """The s3 client creator, if we have one, i.e., if we have been aenter'd"""

        self._s3 = None
        """The result from __aenter__ on the client creator"""

    async def __aenter__(self) -> "S3":
        self._session = aioboto3.Session()
        self.__s3_creator = self._session.client("s3")
        self._s3 = await self.__s3_creator.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.__s3_creator.__aexit__(exc_type, exc, tb)
        self._session = None
        self.__s3_creator = None
        self._s3 = None

    async def upload(
        self,
        f: Union[BytesIO, AsyncReadableBytesIO],
        *,
        bucket: str,
        key: str,
        sync: bool,
    ) -> None:
        logging.info(f"[file_service/s3]: upload {bucket=}, {key=}")
        if not sync:
            with temp_file() as tmp:
                async with aiofiles.open(tmp, "wb") as f2:
                    data = await f.read(8192)
                    while data:
                        await f2.write(data)
                        data = await f.read(8192)

                with open(tmp, "rb") as f2:
                    await self._s3.put_object(Bucket=bucket, Key=key, Body=f2)
            return

        if not isinstance(f, io.IOBase) and hasattr(f, "read"):
            # Typically this is from e.g., SpooledTemporaryFile, which is nearly an io-like
            # file since introduced, but not actually one until python 3.11. We take a pretty
            # big performance hit for converting spooled files this way, but since it goes
            # away once our python version is higher, we can live with it.

            with temp_file() as tmp:
                async with aiofiles.open(tmp, "wb") as f2:
                    data = f.read(8192)
                    while data:
                        await f2.write(data)
                        data = f.read(8192)

                with open(tmp, "rb") as f2:
                    await self._s3.put_object(Bucket=bucket, Key=key, Body=f2)

            return

        await self._s3.put_object(Bucket=bucket, Key=key, Body=f)

    async def download(
        self,
        f: Union[BytesIO, AsyncWritableBytesIO],
        *,
        bucket: str,
        key: str,
        sync: bool,
    ) -> bool:
        logging.info(f"[file_service/s3]: download {bucket=}, {key=}")
        try:
            s3_ob = await self._s3.get_object(Bucket=bucket, Key=key)

            # https://github.com/terrycain/aioboto3/issues/266
            stream = s3_ob["Body"]
            try:
                data = await stream.read(8192)
                if sync:
                    while data:
                        f.write(data)
                        data = await stream.read(8192)
                else:
                    while data:
                        await f.write(data)
                        data = await stream.read(8192)
            finally:
                stream.close()

            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return False
            raise

    async def delete(self, *, bucket: str, key: str) -> bool:
        logging.info(f"[file_service/s3]: delete {bucket=}, {key=}")
        try:
            await self._s3.delete_object(Bucket=bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return False
            raise


class LocalFiles:
    """Adapts a local folder as a file service."""

    def __init__(self, root: str, default_bucket: str) -> None:
        self.default_bucket = default_bucket
        """The recommended default bucket"""

        self._root = root
        """The root folder, the immediate subdirectories of which are the buckets"""

    async def __aenter__(self) -> "LocalFiles":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def upload(
        self,
        f: Union[BytesIO, AsyncReadableBytesIO],
        *,
        bucket: str,
        key: str,
        sync: bool,
    ) -> None:
        dst = os.path.join(self._root, bucket, key)
        dst_folder = os.path.dirname(dst)
        os.makedirs(dst_folder, exist_ok=True)
        async with aiofiles.open(dst, "wb") as f2:
            if sync:
                chunk = f.read(8192)
                while chunk:
                    await f2.write(chunk)
                    chunk = f.read(8192)
            else:
                chunk = await f.read(8192)
                while chunk:
                    await f2.write(chunk)
                    chunk = await f.read(8192)

    async def download(
        self,
        f: Union[BytesIO, AsyncWritableBytesIO],
        *,
        bucket: str,
        key: str,
        sync: bool,
    ) -> bool:
        logging.info(f"[file_service/local_files]: download {bucket=}, {key=}")
        try:
            async with aiofiles.open(os.path.join(self._root, bucket, key), "rb") as f2:
                chunk = await f2.read(8192)
                if sync:
                    while chunk:
                        f.write(chunk)
                        chunk = await f2.read(8192)
                else:
                    while chunk:
                        await f.write(chunk)
                        chunk = await f2.read(8192)

            return True
        except FileNotFoundError:
            return False

    async def delete(self, *, bucket: str, key: str) -> bool:
        logging.info(f"[file_service/local_files]: delete {bucket=}, {key=}")
        try:
            os.unlink(os.path.join(self._root, bucket, key))
            return True
        except FileNotFoundError:
            return False
