import asyncio
import json
import logging
import os
import secrets
import shutil
import socket
import subprocess
import time
from typing import List, Optional, cast

import aiofiles
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath

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
from redis_helpers.voice_notes_transcription_finished import (
    safe_voice_notes_transcription_finished,
)
from temp_files import temp_file
import openai
import openai.types.audio.translation
import lib.journals.master_keys

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    voice_note_uid: str,
    stitched_file_size_bytes: int,
    job_progress_uid: str,
):
    """Transcribes the audio file corresponding with the given voice note, reporting
    progress to the given job progress uid.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        voice_note_uid (str): The processing voice note to transcribe
        stitched_file_size_bytes (int): The expected size of the stitched file in bytes
        job_progress_uid (str): For reporting progress or errors
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.voice_notes.transcribe",
            voice_note_uid=voice_note_uid,
            stitched_file_size_bytes=stitched_file_size_bytes,
            job_progress_uid=job_progress_uid,
        )

    async with success_or_failure_reporter(
        itgs, job_progress_uid=job_progress_uid
    ) as helper:
        voice_note_uid_bytes = voice_note_uid.encode("utf-8")
        job_progress_uid_bytes = job_progress_uid.encode("utf-8")

        redis = await itgs.redis()
        (
            user_sub,
            voice_note_started_at,
            transcribe_job_queued_at,
            old_transcribe_job_finished_at,
            stitched_s3_key,
            journal_master_key_uid,
        ) = cast(
            List[Optional[bytes]],
            await redis.hmget(
                b"voice_notes:processing:" + voice_note_uid_bytes,  # type: ignore
                b"user_sub",  # type: ignore
                b"started_at",  # type: ignore
                b"transcribe_job_queued_at",  # type: ignore
                b"transcribe_job_finished_at",  # type: ignore
                b"stitched_s3_key",  # type: ignore
                b"journal_master_key_uid",  # type: ignore
            ),
        )

        if (
            user_sub is None
            or voice_note_started_at is None
            or transcribe_job_queued_at is None
            or old_transcribe_job_finished_at is None
            or stitched_s3_key is None
            or journal_master_key_uid is None
            or voice_note_started_at == b"not_yet"
            or transcribe_job_queued_at == b"not_yet"
            or stitched_s3_key == b"not_yet"
            or journal_master_key_uid == b"not_yet"
        ):
            await handle_warning(
                f"{__name__}:missing_data",
                f"Voice note `{voice_note_uid}` is either not in the processing set or missing data:\n"
                f"- user sub: `{user_sub}`\n"
                f"- started at: `{voice_note_started_at}`\n"
                f"- transcribe job queued at: `{transcribe_job_queued_at}`\n"
                f"- old transcribe job finished at: `{old_transcribe_job_finished_at}`\n"
                f"- stitched s3 key: `{stitched_s3_key}`"
                f"- journal master key uid: `{journal_master_key_uid}`",
            )
            raise CustomFailureReasonException("missing data or not in processing set")

        if old_transcribe_job_finished_at != b"not_yet":
            await handle_warning(
                f"{__name__}:already_finished",
                f"Voice note `{voice_note_uid}` has already been transcribed",
            )
            raise CustomFailureReasonException("already transcribed")

        cleanup_tasks: List[asyncio.Task] = []
        try:
            transcribe_started_at = time.time()
            slack = await itgs.slack()
            cleanup_tasks.append(
                asyncio.create_task(
                    slack.send_ops_message(
                        f"{socket.gethostname()} transcribing voice note `{voice_note_uid}` for `{user_sub}` {transcribe_started_at - float(voice_note_started_at):.3f}s since voice note upload started",
                    )
                )
            )

            files = await itgs.files()
            with temp_file() as stitched_file_path, temp_file(
                ".mp4"
            ) as prepared_file_path:
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
                    logging.info(f"transcribe job for {voice_note_uid} bouncing")
                    await bounce()
                    raise BouncedException()

                real_stitched_file_size = os.path.getsize(stitched_file_path)
                if real_stitched_file_size != stitched_file_size_bytes:
                    await handle_warning(
                        f"{__name__}:size_mismatch",
                        f"Downloaded stitched file size mismatch: expected {stitched_file_size_bytes} bytes, got {real_stitched_file_size} bytes",
                    )
                    raise CustomFailureReasonException("size mismatch")

                await helper.push_progress(
                    "transcoding to correct format for transcription",
                    indicator={"type": "spinner"},
                )
                ffmpeg = shutil.which("ffmpeg")
                cmd = [
                    ffmpeg,
                    "-hide_banner",
                    "-loglevel",
                    "warning",
                    "-nostats",
                    "-i",
                    stitched_file_path,
                    "-vn",
                    "-acodec",
                    "aac",
                    "-ar",
                    "16000",
                    "-b:a",
                    "128k",
                    "-movflags",
                    "faststart",
                    "-map_metadata",
                    "-1",
                    "-map_chapters",
                    "-1",
                    prepared_file_path,
                ]
                logging.info(f"Running command: {json.dumps(cmd)}")
                transcode_started_at = time.perf_counter()
                result = await asyncio.to_thread(
                    subprocess.run, cmd, capture_output=True
                )
                transcode_time_taken = time.perf_counter() - transcode_started_at
                if result.returncode != 0:
                    await handle_warning(
                        f"{__name__}:transcription_failed",
                        "ffmpeg failed\n\nstdout: ```\n"
                        + result.stdout.decode("utf-8", errors="replace")
                        + "\n```\n\nstderr: ```\n"
                        + result.stderr.decode("utf-8", errors="replace")
                        + "\n```",
                    )

                prepared_file_size = os.path.getsize(prepared_file_path)
                logging.info(
                    f"Transcoded audio for transcription in {transcode_time_taken:.3f}s ({real_stitched_file_size} bytes -> {prepared_file_size} bytes)"
                )

                transcription_started_at = time.perf_counter()
                transcript = await asyncio.to_thread(
                    _transcribe_sync, prepared_file_path
                )
                transcription_time_taken = (
                    time.perf_counter() - transcription_started_at
                )

                logging.info(
                    f"Transcribed audio for {voice_note_uid} in {transcription_time_taken:.3f}s"
                )

                encryption_started_at = time.time()
                master_key = await lib.journals.master_keys.get_journal_master_key_for_decryption(
                    itgs,
                    user_sub=user_sub.decode("utf-8"),
                    journal_master_key_uid=journal_master_key_uid.decode("utf-8"),
                )
                if master_key.type != "success":
                    await handle_warning(
                        f"{__name__}:master_key_failed:{master_key.type}",
                        f"Failed to get master key for user `{user_sub}`",
                    )
                    raise CustomFailureReasonException("failed to setup encryption")

                encrypted_transcription_vtt = master_key.journal_master_key.encrypt(
                    transcript.encode("utf-8")
                )
                encryption_time_taken = time.time() - encryption_started_at

                time_since_voice_note_upload = time.time() - float(
                    voice_note_started_at
                )
                cleanup_tasks.append(
                    asyncio.create_task(
                        slack.send_ops_message(
                            f"{socket.gethostname()} transcribed voice note `{voice_note_uid}` for `{user_sub}`\n"
                            f"• Time spent preparing: {transcode_time_taken:.3f}s\n"
                            f"• Time spent transcribing: {transcription_time_taken:.3f}s\n"
                            f"• Time spent encrypting: {encryption_time_taken:.3f}s\n"
                            f"• Total time since upload started: {time_since_voice_note_upload:.3f}s",
                        )
                    )
                )

                await itgs.ensure_redis_liveliness()
                store_at = time.time()
                finalize_job_progress_uid = f"oseh_jp_{secrets.token_urlsafe(16)}"
                store_transcription_result = await safe_voice_notes_transcription_finished(
                    itgs,
                    voice_note_uid=voice_note_uid_bytes,
                    encrypted_transcription_vtt=encrypted_transcription_vtt,
                    transcription_source=b'{"type": "ai", "model": "whisper-1", "version": "live"}',
                    now_str=str(store_at).encode("ascii"),
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
                    transcribe_job_progress_uid=job_progress_uid_bytes,
                    finalize_job_progress_uid=finalize_job_progress_uid.encode("utf-8"),
                    transcribe_progress_finalize_job_spawn_event=json.dumps(
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
                    store_transcription_result.type != "success_pending_other"
                    and store_transcription_result.type != "success_pending_finalize"
                ):
                    await handle_warning(
                        f"{__name__}:store_failed:{store_transcription_result.type}",
                        f"Failed to store transcription for voice note `{voice_note_uid}",
                    )
                    raise CustomFailureReasonException("store failed")

                logging.info(
                    f"Stored transcription for voice note `{voice_note_uid}`: {store_transcription_result.type}"
                )
        finally:
            await asyncio.wait(cleanup_tasks, return_when=asyncio.ALL_COMPLETED)
            # check for errors
            for task in cleanup_tasks:
                await task


def _transcribe_sync(input_path: str) -> str:
    """Transcribes the audio file at the given path using the OpenAI API. If
    the audio is not in English, the transcription is translated into English
    before being returned.

    Args:
        input_path (str): The path to the audio file to transcribe

    Returns:
        openai.types.audio.transcription.Translation: The transcription result
    """
    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
    client = openai.Client(api_key=openai_api_key)
    with open(input_path, "rb") as f:
        res = client.audio.translations.create(
            model="whisper-1", file=f, response_format="vtt"
        )

    return str(res) if isinstance(res, str) else res.text
