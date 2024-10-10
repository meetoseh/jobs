import asyncio
import json
import secrets
import socket
import time
from typing import List, Optional, cast

import aiofiles
from audio import ProcessAudioAbortedException, process_audio
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import (
    JobCategory,
    JobProgressIndicatorSpinner,
    JobProgressSimple,
    JobProgressSpawned,
    JobProgressSpawnedInfo,
)
from lib.progressutils.async_progress_tracking_bytes_io import (
    AsyncProgressTrackingWritableBytesIO,
)
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    CustomFailureReasonException,
    success_or_failure_reporter,
)
from redis_helpers.voice_notes_transcoding_finished import (
    safe_voice_notes_transcoding_finished,
)
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    voice_note_uid: str,
    stitched_file_size_bytes: int,
    job_progress_uid: str,
):
    """Transcodes the voice note so it can be quickly and easily served to a variety of
    different platforms.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        voice_note_uid (str): what to transcode
        stitched_file_size_bytes (int): the size of the stitched file in bytes
        job_progress_uid (str): where to report progress
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.voice_notes.transcode",
            voice_note_uid=voice_note_uid,
            stitched_file_size_bytes=stitched_file_size_bytes,
            job_progress_uid=job_progress_uid,
        )

    async with success_or_failure_reporter(itgs, job_progress_uid=job_progress_uid):
        voice_note_uid_bytes = voice_note_uid.encode("utf-8")
        job_progress_uid_bytes = job_progress_uid.encode("utf-8")

        redis = await itgs.redis()
        (
            user_sub,
            voice_note_started_at,
            transcode_job_queued_at,
            old_transcode_job_finished_at,
            stitched_s3_key,
        ) = cast(
            List[Optional[bytes]],
            await redis.hmget(
                b"voice_notes:processing:" + voice_note_uid_bytes,  # type: ignore
                b"user_sub",  # type: ignore
                b"started_at",  # type: ignore
                b"transcode_job_queued_at",  # type: ignore
                b"transcode_job_finished_at",  # type: ignore
                b"stitched_s3_key",  # type: ignore
            ),
        )

        if (
            user_sub is None
            or voice_note_started_at is None
            or transcode_job_queued_at is None
            or old_transcode_job_finished_at is None
            or stitched_s3_key is None
            or voice_note_started_at == b"not_yet"
            or transcode_job_queued_at == b"not_yet"
            or stitched_s3_key == b"not_yet"
        ):
            await handle_warning(
                f"{__name__}:missing_data",
                f"Voice note `{voice_note_uid}` is either not in the processing set or missing data:\n"
                f"• user sub: `{user_sub}`\n"
                f"• started at: `{voice_note_started_at}`\n"
                f"• transcode job queued at: `{transcode_job_queued_at}`\n"
                f"• old transcode job finished at: `{old_transcode_job_finished_at}`\n"
                f"• stitched s3 key: `{stitched_s3_key}`",
            )
            raise CustomFailureReasonException("missing data or not in processing set")

        if old_transcode_job_finished_at != b"not_yet":
            await handle_warning(
                f"{__name__}:already_finished",
                f"Voice note `{voice_note_uid}` has already been transcoded",
            )
            raise CustomFailureReasonException("already transcoded")

        cleanup_tasks: List[asyncio.Task] = []
        try:
            transcode_started_at = time.time()
            slack = await itgs.slack()
            cleanup_tasks.append(
                asyncio.create_task(
                    slack.send_ops_message(
                        f"{socket.gethostname()} transcoding voice note `{voice_note_uid}` for `{user_sub}` {transcode_started_at - float(voice_note_started_at):.3f}s since voice note upload started",
                    )
                )
            )
            files = await itgs.files()
            with temp_file() as stitched_file_path:
                async with aiofiles.open(
                    stitched_file_path, "wb"
                ) as raw_file, AsyncProgressTrackingWritableBytesIO(
                    itgs,
                    job_progress_uid=job_progress_uid,
                    expected_file_size=stitched_file_size_bytes,
                    delegate=raw_file,
                    message="downloading stitched file",
                ):
                    await files.download(
                        raw_file,
                        bucket=files.default_bucket,
                        key=stitched_s3_key.decode("utf-8"),
                        sync=False,
                    )

                if gd.received_term_signal:
                    logging.info(f"transcode job for {voice_note_uid} bouncing")
                    await bounce()
                    raise BouncedException()

                transcoding_started_at = time.time()
                try:
                    content = await process_audio(
                        stitched_file_path,
                        itgs=itgs,
                        gd=gd,
                        max_file_size=stitched_file_size_bytes,
                        name_hint=f"{voice_note_uid}/{user_sub}",
                        job_progress_uid=job_progress_uid,
                        audio_bitrates=(192, 1411),
                    )
                except ProcessAudioAbortedException:
                    logging.info(f"transcode job for {voice_note_uid} bouncing")
                    await bounce()
                    raise BouncedException()
                time_spent_transcoding = time.time() - transcoding_started_at

                logging.info(
                    f"transcoded {voice_note_uid} to {content.uid} in {time_spent_transcoding:.3f} seconds"
                )
                time_since_voice_note_upload = time.time() - float(
                    voice_note_started_at
                )
                cleanup_tasks.append(
                    asyncio.create_task(
                        slack.send_ops_message(
                            f"{socket.gethostname()} transcoded voice note `{voice_note_uid}` for `{user_sub}`\n"
                            f"• Time spent transcoding: {time_spent_transcoding:.3f}s\n"
                            f"• Total time since upload started: {time_since_voice_note_upload:.3f}s",
                        )
                    )
                )

                finalize_job_progress_uid = f"oseh_jp_{secrets.token_urlsafe(16)}"
                store_at = time.time()
                store_result = await safe_voice_notes_transcoding_finished(
                    itgs,
                    voice_note_uid=voice_note_uid_bytes,
                    transcode_content_file_uid=content.uid.encode("utf-8"),
                    now_str=str(time.time()).encode("utf-8"),
                    finalize_job=json.dumps(
                        {
                            "name": "runners.voice_notes.finalize",
                            "kwargs": {
                                "voice_note_uid": voice_note_uid,
                                "job_progress_uid": finalize_job_progress_uid,
                            },
                            "queued_at": store_at,
                        }
                    ).encode("utf-8"),
                    transcode_job_progress_uid=job_progress_uid_bytes,
                    finalize_job_progress_uid=finalize_job_progress_uid.encode("utf-8"),
                    transcode_progress_finalize_job_spawn_event=json.dumps(
                        JobProgressSpawned(
                            type="spawned",
                            message="Queued finalize job",
                            spawned=JobProgressSpawnedInfo(
                                uid=finalize_job_progress_uid, name="finalize"
                            ),
                            indicator=None,
                            occurred_at=store_at,
                        )
                    ).encode("utf-8"),
                    finalize_job_queued_event=json.dumps(
                        JobProgressSimple(
                            type="queued",
                            message="waiting for an available worker",
                            indicator=JobProgressIndicatorSpinner(type="spinner"),
                            occurred_at=store_at,
                        )
                    ).encode("utf-8"),
                )

                if (
                    store_result.type != "success_pending_finalize"
                    and store_result.type != "success_pending_other"
                ):
                    await handle_warning(
                        f"{__name__}:store_failed:{store_result.type}",
                        f"Failed to store transcoding result for `{voice_note_uid}`: {store_result.type}",
                    )
                    raise CustomFailureReasonException(f"failed to store")

                logging.info(
                    f"Finished transcoding {voice_note_uid} for {user_sub}: {store_result.type}"
                )
        finally:
            await asyncio.wait(cleanup_tasks, return_when=asyncio.ALL_COMPLETED)
            # check for errors
            for task in cleanup_tasks:
                await task
