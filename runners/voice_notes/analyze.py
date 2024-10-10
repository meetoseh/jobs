import asyncio
import io
import json
import os
import secrets
import socket
import time
from typing import List, Optional, TypedDict, cast

import aiofiles
from content import hash_filelike
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
    AsyncProgressTrackingReadableBytesIO,
    AsyncProgressTrackingWritableBytesIO,
)
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    CustomFailureReasonException,
    success_or_failure_reporter,
)
from redis_helpers.voice_notes_analyze_finished import safe_voice_notes_analyze_finished
from temp_files import temp_file
import logging
import shareables.journey_audio.p01_crop_and_normalize
import numpy as np
import lib.journals.master_keys

category = JobCategory.HIGH_RESOURCE_COST

NUM_BINS = [64, 56, 48, 40, 32, 24, 16, 8]
"""The bin sizes we emit, in order"""


class _BinInfo(TypedDict):
    start_seconds_incl: float
    end_seconds_excl: float
    start_sample_incl: int
    end_sample_excl: int


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    voice_note_uid: str,
    stitched_file_size_bytes: int,
    job_progress_uid: str,
):
    """Analyzes the audio file to compute a time vs intensity graph. This is the
    most common way to provide a visual representation of an audio file and the
    client will want to fetch it to display it to the user without having to download
    the audio file.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        voice_note_uid (str): the uid of the voice note being analyzed
        stitched_file_size_bytes (int): the size of the stitched file in bytes, allowing
            for a real progress bar when downloading the file
        job_progress_uid (str): the uid of the job progress to update
    """

    async def bounce():
        logging.info(f"analyze job for {voice_note_uid} bouncing")
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.voice_notes.analyze",
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
            analyze_job_queued_at,
            old_analyze_job_finished_at,
            stitched_s3_key,
            journal_master_key_uid,
        ) = cast(
            List[Optional[bytes]],
            await redis.hmget(
                b"voice_notes:processing:" + voice_note_uid_bytes,  # type: ignore
                b"user_sub",  # type: ignore
                b"started_at",  # type: ignore
                b"analyze_job_queued_at",  # type: ignore
                b"analyze_job_finished_at",  # type: ignore
                b"stitched_s3_key",  # type: ignore
                b"journal_master_key_uid",  # type: ignore
            ),
        )

        if (
            user_sub is None
            or voice_note_started_at is None
            or analyze_job_queued_at is None
            or old_analyze_job_finished_at is None
            or stitched_s3_key is None
            or journal_master_key_uid is None
            or voice_note_started_at == b"not_yet"
            or analyze_job_queued_at == b"not_yet"
            or stitched_s3_key == b"not_yet"
            or journal_master_key_uid == b"not_yet"
        ):
            await handle_warning(
                f"{__name__}:missing_data",
                f"Voice note `{voice_note_uid}` is either not in the processing set or missing data:\n"
                f"• user sub: `{user_sub}`\n"
                f"• started at: `{voice_note_started_at}`\n"
                f"• analyze job queued at: `{analyze_job_queued_at}`\n"
                f"• old analyze job finished at: `{old_analyze_job_finished_at}`\n"
                f"• stitched s3 key: `{stitched_s3_key}`"
                f"• journal master key uid: `{journal_master_key_uid}`",
            )
            raise CustomFailureReasonException("missing data or not in processing set")

        if old_analyze_job_finished_at != b"not_yet":
            await handle_warning(
                f"{__name__}:already_finished",
                f"Voice note `{voice_note_uid}` has already been analyzed",
            )
            raise CustomFailureReasonException("already analyzed")

        master_key = (
            await lib.journals.master_keys.get_journal_master_key_for_decryption(
                itgs,
                user_sub=user_sub.decode("utf-8"),
                journal_master_key_uid=journal_master_key_uid.decode("utf-8"),
            )
        )
        if master_key.type != "success":
            raise CustomFailureReasonException("failed to setup encryption")

        analyze_started_at = time.time()
        slack = await itgs.slack()
        report_start_task = asyncio.create_task(
            slack.send_ops_message(
                f"{socket.gethostname()} analyzing voice note `{voice_note_uid}` for `{user_sub}` {analyze_started_at - float(voice_note_started_at):.3f}s since voice note upload started",
            )
        )
        try:
            files = await itgs.files()
            with temp_file() as stitched_file_path, temp_file() as raw_path:
                download_started_at = time.time()
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
                download_finished_at = time.time()
                logging.info(
                    f"Downloading stitched file for {voice_note_uid} took {download_finished_at - download_started_at:.3f}s"
                )

                if gd.received_term_signal:
                    await bounce()
                    raise BouncedException()

                hash_started_at = time.time()
                async with aiofiles.open(
                    stitched_file_path, "rb"
                ) as raw_file, AsyncProgressTrackingReadableBytesIO(
                    itgs,
                    job_progress_uid=job_progress_uid,
                    expected_file_size=stitched_file_size_bytes,
                    delegate=raw_file,
                    message="hashing",
                ) as tracking_file:
                    stitched_sha512 = await hash_filelike(tracking_file)
                hash_finished_at = time.time()
                logging.info(
                    f"Hashing stitched file for {voice_note_uid} took {hash_finished_at - hash_started_at:.3f}s"
                )
                if gd.received_term_signal:
                    await bounce()
                    raise BouncedException()

                await helper.push_progress(
                    "downmixing and normalizing to PCM", indicator={"type": "spinner"}
                )
                extracting_raw_started_at = time.time()
                norm_result = await asyncio.to_thread(
                    shareables.journey_audio.p01_crop_and_normalize.crop_and_normalize,
                    source_path=stitched_file_path,
                    dest_path=raw_path,
                )
                extracting_raw_finished_at = time.time()
                logging.info(
                    f"extracting raw audio for {voice_note_uid} took {extracting_raw_finished_at - extracting_raw_started_at:.3f}s"
                )
                loading_raw_started_at = time.time()
                raw_audio = np.fromfile(raw_path, dtype=norm_result.dtype)
                loading_raw_finished_at = time.time()
                logging.info(
                    f"loading raw audio for {voice_note_uid} took {loading_raw_finished_at - loading_raw_started_at:.3f}s"
                )

                await helper.push_progress(
                    "computing time vs intensity graphs", indicator={"type": "spinner"}
                )

                core_analysis_started_at = time.time()
                dtype_width_bytes = norm_result.dtype.itemsize
                norm_file_size = os.path.getsize(norm_result.path)
                exact_samples = norm_file_size // dtype_width_bytes
                if exact_samples * dtype_width_bytes != norm_file_size:
                    raise Exception(
                        f"exact samples {exact_samples} * dtype width {dtype_width_bytes} != norm file size {norm_file_size}"
                    )

                # we're going to rescale all the audio from 0-1 and switch to float64 while doing so
                raw_audio = raw_audio.astype(np.float64)
                raw_audio -= raw_audio.mean()

                raw_audio_max = np.max(np.abs(raw_audio))
                if raw_audio_max != 0:
                    raw_audio /= float(raw_audio_max)

                time_vs_intensity_raw = io.BytesIO()
                time_vs_intensity_raw.write(b'{"type": "tvi", "version": 1}\n')
                time_vs_intensity_raw.write(
                    json.dumps(
                        {
                            "audio_file_sha512": stitched_sha512,
                            "duration_seconds": exact_samples / norm_result.sample_rate,
                            "duration_samples": exact_samples,
                            "computed_at": time.time(),
                        }
                    ).encode("utf-8")
                )
                time_vs_intensity_raw.write(b"\n")

                for bins in NUM_BINS:
                    arr: List[float] = []
                    bin_infos: List[_BinInfo] = []

                    # bins may not exactly divide the samples; we will make
                    # the last bin smaller to adjust

                    # we use root mean square instead of pure mean to get a better
                    # sense of the intensity of the audio

                    bin_size = (exact_samples + bins - 1) // bins
                    for i in range(bins):
                        start = i * bin_size
                        end = min((i + 1) * bin_size, exact_samples)
                        bin_infos.append(
                            {
                                "start_seconds_incl": start / norm_result.sample_rate,
                                "end_seconds_excl": end / norm_result.sample_rate,
                                "start_sample_incl": start,
                                "end_sample_excl": end,
                            }
                        )
                        arr.append(
                            float(np.sqrt(np.mean(np.square(raw_audio[start:end]))))
                        )

                    highest_bin_value = max(arr)
                    if highest_bin_value != 0:
                        arr = [x / highest_bin_value for x in arr]

                    time_vs_intensity_raw.write(json.dumps(arr).encode("ascii"))
                    time_vs_intensity_raw.write(b"\n")
                    time_vs_intensity_raw.write(json.dumps(bin_infos).encode("ascii"))
                    time_vs_intensity_raw.write(b"\n")

                core_analysis_finished_at = time.time()
                logging.info(
                    f"core analysis for {voice_note_uid} took {core_analysis_finished_at - core_analysis_started_at:.3f}s"
                )

                encryption_started_at = time.time()
                encrypted_time_vs_intensity = master_key.journal_master_key.encrypt(
                    time_vs_intensity_raw.getvalue()
                )
                encryption_finished_at = time.time()
                logging.info(
                    f"encryption for {voice_note_uid} took {encryption_finished_at - encryption_started_at:.3f}s"
                )

                await itgs.ensure_redis_liveliness()
                finalize_job_progress_uid = f"oseh_jp_{secrets.token_urlsafe(16)}"
                store_at = time.time()
                store_result = await safe_voice_notes_analyze_finished(
                    itgs,
                    voice_note_uid=voice_note_uid_bytes,
                    encrypted_time_vs_intensity=encrypted_time_vs_intensity,
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
                    analyze_job_progress_uid=job_progress_uid_bytes,
                    finalize_job_progress_uid=finalize_job_progress_uid.encode("utf-8"),
                    analyze_progress_finalize_job_spawn_event=json.dumps(
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
                        f"Failed to store analysis result for `{voice_note_uid}`: {store_result.type}",
                    )
                    raise CustomFailureReasonException(f"failed to store")

                logging.info(
                    f"Finished analyzing {voice_note_uid} for {user_sub}: {store_result.type}"
                )

                voice_note_started_at_float = float(voice_note_started_at)
                analyze_job_queued_at_float = float(analyze_job_queued_at)
                await report_start_task
                await slack.send_ops_message(
                    f"{socket.gethostname()} analyzed voice note `{voice_note_uid}` for `{user_sub.decode('utf-8')}`:\n```\n"
                    f"voice note upload started at {voice_note_started_at_float:.3f}s\n"
                    f"analyze job queued at        {analyze_job_queued_at_float:.3f}s (+{analyze_job_queued_at_float - voice_note_started_at_float:>8.3f}s)\n"
                    f"analyze started at           {analyze_started_at:.3f}s (+{analyze_started_at - analyze_job_queued_at_float:>8.3f}s) [+{analyze_started_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"downloaded stitched file at  {download_finished_at:.3f}s (+{download_finished_at - analyze_started_at:>8.3f}s) [+{download_finished_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"hashed stitched file at      {hash_finished_at:.3f}s (+{hash_finished_at - download_finished_at:>8.3f}s) [+{hash_finished_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"started extracting audio at  {extracting_raw_started_at:.3f}s (+{extracting_raw_started_at - analyze_started_at:>8.3f}s) [+{extracting_raw_started_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"finished extracting audio at {extracting_raw_finished_at:.3f}s (+{extracting_raw_finished_at - extracting_raw_started_at:>8.3f}s) [+{extracting_raw_finished_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"started loading audio at     {loading_raw_started_at:.3f}s (+{loading_raw_started_at - extracting_raw_finished_at:>8.3f}s) [+{loading_raw_started_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"finished loading audio at    {loading_raw_finished_at:.3f}s (+{loading_raw_finished_at - loading_raw_started_at:>8.3f}s) [+{loading_raw_finished_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"started core analysis at     {core_analysis_started_at:.3f}s (+{core_analysis_started_at - loading_raw_finished_at:>8.3f}s) [+{core_analysis_started_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"finished core analysis at    {core_analysis_finished_at:.3f}s (+{core_analysis_finished_at - core_analysis_started_at:>8.3f}s) [+{core_analysis_finished_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"started encryption at        {encryption_started_at:.3f}s (+{encryption_started_at - core_analysis_finished_at:>8.3f}s) [+{encryption_started_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"finished encryption at       {encryption_finished_at:.3f}s (+{encryption_finished_at - encryption_started_at:>8.3f}s) [+{encryption_finished_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    f"stored analysis result at    {store_at:.3f}s (+{store_at - encryption_finished_at:>8.3f}s) [+{store_at - voice_note_started_at_float:>8.3f}s overall]\n"
                    "```\n"
                )
        finally:
            await report_start_task
