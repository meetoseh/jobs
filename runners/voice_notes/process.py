"""The initial job for processing a voice note called by the file upload system"""

import json
import os
import secrets
import time
import aiofiles
from error_middleware import handle_warning
from file_uploads import StitchFileAbortedException, stitch_file_upload
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
)
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    CustomFailureReasonException,
    success_or_failure_reporter,
)
from redis_helpers.voice_notes_upload_success_started import (
    safe_voice_notes_upload_success_started,
)
from redis_helpers.voice_notes_upload_success_stitched import (
    safe_voice_notes_upload_success_stitched,
)
from temp_files import temp_file

category = JobCategory.LOW_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: str,
    voice_note_uid: str,
):
    """Handles the user finishing uploading a file intended to be used as a voice note. This
    job will stitch the upload together from s3 and check that the file appears to be an acceptable
    audio file. If it is acceptable, uploads the stitched file to s3 and queues jobs to transcribe
    & transcode the audio file.

    Finally, queues a job to delete the original upload from s3.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): For downloading the parts
        uploaded_by_user_sub (str): Who the voice note belong sto
        job_progress_uid (str): For reporting progress or errors
        voice_note_uid (str): Reserved in advance for coordinating the sub-jobs and to
            allow for optimistically assuming processing will succeed to hide the latency
            involved
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.voice_notes.process",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
            voice_note_uid=voice_note_uid,
        )

    async with success_or_failure_reporter(itgs, job_progress_uid=job_progress_uid):
        result = await safe_voice_notes_upload_success_started(
            itgs, voice_note_uid=voice_note_uid.encode("ascii"), now=int(time.time())
        )
        if result.type == "replaced":
            await handle_warning(
                f"{__name__}:start:replaced",
                f"voice notes process job recovering from already started state on voice note `{voice_note_uid}`",
            )
        elif result.type != "success":
            await handle_warning(
                f"{__name__}:start:{result.type}",
                f"failed to start voice notes process job on voice note `{voice_note_uid}`",
            )
            return

        files = await itgs.files()
        stitched_s3_key = (
            f"s3_files/voice_notes/stitched/{voice_note_uid}/{secrets.token_urlsafe(4)}"
        )
        stitched_s3_uid = f"oseh_s3f_{secrets.token_urlsafe(16)}"
        purgatory_key = json.dumps(
            {"key": stitched_s3_key, "bucket": files.default_bucket}, sort_keys=True
        )

        with temp_file() as stitched_path:
            try:
                await stitch_file_upload(
                    file_upload_uid,
                    stitched_path,
                    itgs=itgs,
                    gd=gd,
                    job_progress_uid=job_progress_uid,
                )
            except StitchFileAbortedException:
                await bounce()
                raise BouncedException()

            file_size = os.path.getsize(stitched_path)

            redis = await itgs.redis()
            await redis.zadd(
                "files:purgatory", mapping={purgatory_key: time.time() + 3000}
            )
            async with aiofiles.open(
                stitched_path, "rb"
            ) as raw_file, AsyncProgressTrackingReadableBytesIO(
                itgs,
                job_progress_uid=job_progress_uid,
                expected_file_size=file_size,
                delegate=raw_file,
                message="uploading stitched file",
            ) as tracking_file:
                await files.upload(
                    tracking_file,
                    bucket=files.default_bucket,
                    key=stitched_s3_key,
                    sync=False,
                )

            conn = await itgs.conn()
            cursor = conn.cursor()
            response = await cursor.execute(
                """
INSERT INTO s3_files (
    uid, key, file_size, content_type, created_at
)
VALUES (
    ?, ?, ?, ?, ?
)
                """,
                (
                    stitched_s3_uid,
                    stitched_s3_key,
                    file_size,
                    "audio/unknown",
                    time.time(),
                ),
            )
            assert response.rows_affected == 1, response

        mark_stitched_at = time.time()
        transcribe_job_progress_uid = f"oseh_jp_{secrets.token_urlsafe(16)}"
        transcode_job_progress_uid = f"oseh_jp_{secrets.token_urlsafe(16)}"
        mark_stitched_response = await safe_voice_notes_upload_success_stitched(
            itgs,
            voice_note_uid=voice_note_uid.encode("ascii"),
            stitched_s3_key=stitched_s3_key.encode("ascii"),
            transcribe_job=json.dumps(
                {
                    "name": "runners.voice_notes.transcribe",
                    "kwargs": {
                        "voice_note_uid": voice_note_uid,
                        "stitched_file_size_bytes": file_size,
                        "job_progress_uid": transcribe_job_progress_uid,
                    },
                    "queued_at": mark_stitched_at,
                }
            ).encode("utf-8"),
            transcribe_job_progress_uid=transcribe_job_progress_uid.encode("ascii"),
            transcribe_job_event=json.dumps(
                JobProgressSimple(
                    type="queued",
                    message="waiting for an available worker",
                    indicator=JobProgressIndicatorSpinner(type="spinner"),
                    occurred_at=mark_stitched_at,
                )
            ).encode("utf-8"),
            transcode_job=json.dumps(
                {
                    "name": "runners.voice_notes.transcode",
                    "kwargs": {
                        "voice_note_uid": voice_note_uid,
                        "stitched_file_size_bytes": file_size,
                        "job_progress_uid": transcode_job_progress_uid,
                    },
                    "queued_at": mark_stitched_at,
                }
            ).encode("utf-8"),
            transcode_job_progress_uid=transcode_job_progress_uid.encode("ascii"),
            transcode_job_event=json.dumps(
                JobProgressSimple(
                    type="queued",
                    message="waiting for an available worker",
                    indicator=JobProgressIndicatorSpinner(type="spinner"),
                    occurred_at=mark_stitched_at,
                )
            ).encode("utf-8"),
            primary_job_spawn_transcode_event=json.dumps(
                JobProgressSpawned(
                    type="spawned",
                    message="Queued transcoding job",
                    spawned=JobProgressSpawnedInfo(
                        uid=transcode_job_progress_uid, name="transcode"
                    ),
                    indicator=None,
                    occurred_at=mark_stitched_at,
                )
            ).encode("utf-8"),
            primary_job_spawn_transcribe_event=json.dumps(
                JobProgressSpawned(
                    type="spawned",
                    message="Queued transcription job",
                    spawned=JobProgressSpawnedInfo(
                        uid=transcribe_job_progress_uid, name="transcribe"
                    ),
                    indicator=None,
                    occurred_at=mark_stitched_at,
                )
            ).encode("utf-8"),
            now_str=str(mark_stitched_at).encode("ascii"),
        )

        if mark_stitched_response.type != "success":
            await handle_warning(
                f"{__name__}:mark_stitched:{mark_stitched_response.type}",
                f"failed to mark stitched voice note `{voice_note_uid}`",
            )
            raise CustomFailureReasonException(
                f"failed to mark voice note stitched: {mark_stitched_response.type}"
            )

        await redis.zrem("files:purgatory", purgatory_key)
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )
