import asyncio
import io
import os
from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Union, cast
from content import hash_content
from error_middleware import handle_warning
from file_service import SyncReadableBytesIO, SyncWritableBytesIO
from itgs import Itgs
from dataclasses import dataclass
import numpy as np
import threading
import logging
import time
import struct

from recurring_jobs import JobInterval, pst
import temp_files


@dataclass
class JourneyEmbeddingsResultSuccess:
    type: Literal["success"]
    """
    - `success`: the current joureny embeddings were successfully retrieved
    """
    journey_uids: List[str]
    """
    The UIDs of journeys that we have embeddings for
    """
    journey_embeddings: np.ndarray
    """The embeddings as an MxN numpy array, where M is the number of journeys and 
    N is the embedding dimensionality. This must not be mutated as it may be shared
    across results
    """


@dataclass
class JourneyEmbeddingsResultUnavailable:
    type: Literal["unavailable"]
    """
    - `unavailable`: the `journey_embeddings` key was not found in redis
    """


@dataclass
class JourneyEmbeddingsResultLost:
    type: Literal["lost"]
    """
    - `lost`: the `journey_embeddings` key was found in redis, but we couldn't actually
    retrieve the embeddings from s3
    """


JourneyEmbeddingsResult = Union[
    JourneyEmbeddingsResultSuccess,
    JourneyEmbeddingsResultUnavailable,
    JourneyEmbeddingsResultLost,
]


class JourneyEmbeddingMetadata(BaseModel):
    uid: str = Field(description="the uid of the current preferred journey embeddings")
    s3_file_key: str = Field(
        description="the s3 file key where the embeddings are stored"
    )
    s3_file_bucket: str = Field(
        description="the s3 bucket where the embeddings are stored"
    )
    journal_uid_byte_length: int = Field(
        description="How many bytes are used for each journal uid"
    )
    embedding_byte_length: int = Field(
        description="How many bytes are used for each embedding (float64, big)"
    )
    sha512: str = Field(description="The hex sha512 digest of the s3 file")


__memory_cache = threading.local()


async def get_journey_embeddings(itgs: Itgs) -> JourneyEmbeddingsResult:
    """Retrieves the latest journey embeddings from the nearest cache."""
    result = cast(
        Optional[JourneyEmbeddingsResultSuccess],
        getattr(__memory_cache, "result", None),
    )
    if result is not None:
        result_expires_at = cast(
            Optional[float], getattr(__memory_cache, "result_expires_at", None)
        )
        if result_expires_at is not None and time.time() < result_expires_at:
            return result

    # I think this is safe because we can't be interrupted unless we yield, with respect
    # to the data this thread can see
    asyncio_lock = cast(
        Optional[asyncio.Lock], getattr(__memory_cache, "asyncio_lock", None)
    )
    if asyncio_lock is None:
        asyncio_lock = asyncio.Lock()
        setattr(__memory_cache, "asyncio_lock", asyncio_lock)

    async with asyncio_lock:
        result = cast(
            Optional[JourneyEmbeddingsResultSuccess],
            getattr(__memory_cache, "result", None),
        )
        if result is not None:
            return result

        logging.info(
            f"Retrieving journey embeddings for tid {threading.get_ident()}..."
        )
        cached = await _get_journal_embeddings_from_local_cache(itgs)
        if cached is not None:
            logging.info("  ...from local cache")
            result = await _parse_journey_embeddings(cached)
            setattr(__memory_cache, "result", result)
            setattr(__memory_cache, "result_expires_at", time.time() + 60 * 15)
            return result

        logging.info("  ...from source")
        into = io.BytesIO()
        source_result = await _read_journal_embeddings_from_source(itgs, into)
        if source_result is not None:
            logging.info(f"  ...error: {source_result.type}")
            return source_result

        await _write_journal_embeddings_to_local_cache(itgs, into.getvalue())
        into.seek(0)
        result = await _parse_journey_embeddings(into)
        setattr(__memory_cache, "result", result)
        setattr(__memory_cache, "result_expires_at", time.time() + 60 * 15)
        return result


async def _get_journal_embeddings_from_local_cache(
    itgs: Itgs,
) -> Optional[SyncReadableBytesIO]:
    cache = await itgs.local_cache()
    result = cast(
        Optional[Union[bytes, SyncReadableBytesIO]],
        cache.get(b"journey_embeddings", read=True),
    )
    if result is None:
        return None
    if isinstance(result, (bytes, bytearray, memoryview)):
        return io.BytesIO(result)
    return result


# job to update embeddings is at 2am, so by expiring at 3am we're very likely
# going to have the latest embeddings, without incurring a network request to
# check
expire_interval = JobInterval(pst, hours=(3,), minutes=(0,), seconds=(0,))
expire_interval.verify()


async def _write_journal_embeddings_to_local_cache(itgs: Itgs, raw: bytes) -> None:
    now = time.time()
    next_expire = expire_interval.next_runtime_after(now)

    cache = await itgs.local_cache()
    cache.set(b"journey_embeddings", raw, expire=max(next_expire - now, 60 * 15))


async def _parse_journey_embeddings(
    raw: SyncReadableBytesIO,
) -> JourneyEmbeddingsResultSuccess:
    metadata_length = int.from_bytes(raw.read(4), "big")
    metadata = JourneyEmbeddingMetadata.model_validate_json(raw.read(metadata_length))
    embeddings_length = int.from_bytes(raw.read(8), "big")

    journey_byte_length = (
        metadata.journal_uid_byte_length + metadata.embedding_byte_length
    )
    num_journeys = embeddings_length // journey_byte_length
    assert (
        journey_byte_length * num_journeys == embeddings_length
    ), f"{journey_byte_length=}, {num_journeys=}, {embeddings_length=}"

    dtype_size = struct.calcsize(">d")
    embedding_dimensionality = metadata.embedding_byte_length // dtype_size
    assert embedding_dimensionality * dtype_size == metadata.embedding_byte_length

    embeddings = np.ndarray((num_journeys, embedding_dimensionality), dtype=np.float64)
    journey_uids: List[str] = []
    for i in range(num_journeys):
        journey_uid_bytes = raw.read(metadata.journal_uid_byte_length)
        journey_uids.append(journey_uid_bytes.lstrip(b"\x00").decode("utf-8"))

        for j in range(embedding_dimensionality):
            embeddings[i, j] = struct.unpack(">d", raw.read(dtype_size))[0]

    return JourneyEmbeddingsResultSuccess(
        type="success",
        journey_uids=journey_uids,
        journey_embeddings=embeddings,
    )


async def _read_journal_embeddings_from_source(
    itgs: Itgs, into: SyncWritableBytesIO
) -> Optional[Union[JourneyEmbeddingsResultLost, JourneyEmbeddingsResultUnavailable]]:
    redis = await itgs.redis()
    metadata_raw = await redis.get(b"journey_embeddings")
    if metadata_raw is None:
        return JourneyEmbeddingsResultUnavailable(type="unavailable")

    metadata = JourneyEmbeddingMetadata.model_validate_json(metadata_raw)
    files = await itgs.files()

    with temp_files.temp_file() as embeddings_file:
        with open(embeddings_file, "wb") as f:
            await files.download(
                f, bucket=metadata.s3_file_bucket, key=metadata.s3_file_key, sync=True
            )

        downloaded_integrity = await hash_content(embeddings_file)
        if downloaded_integrity != metadata.sha512:
            await handle_warning(
                f"{__name__}:corrupted_s3_file",
                f"Expected {metadata.sha512}, got {downloaded_integrity} @ {metadata.s3_file_key} in {metadata.s3_file_bucket}",
            )
            return JourneyEmbeddingsResultLost(type="lost")

        into.write(len(metadata_raw).to_bytes(4, "big"))
        into.write(metadata_raw)
        file_size = os.path.getsize(embeddings_file)
        into.write(file_size.to_bytes(8, "big"))
        with open(embeddings_file, "rb") as f:
            buffer = bytearray(8192)
            while True:
                read = f.readinto(buffer)
                if read == 0:
                    break
                into.write(buffer[:read])
