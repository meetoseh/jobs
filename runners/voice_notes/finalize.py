import asyncio
import io
import json
import secrets
import socket
import time
from typing import List, Optional, cast

from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath

from jobs import JobCategory
from lib.progressutils.success_or_failure_reporter import (
    CustomFailureReasonException,
    success_or_failure_reporter,
)

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, voice_note_uid: str, job_progress_uid: str
):
    """Finalizes the voice note by creating the corresponding record in the database
    and removing it from the processing pseudo-set in redis.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        voice_note_uid (str): what to finalize
        job_progress_uid (str): where to report progress
    """
    voice_note_uid_bytes = voice_note_uid.encode("utf-8")

    async with success_or_failure_reporter(
        itgs, job_progress_uid=job_progress_uid
    ) as helper:
        redis = await itgs.redis()
        (
            user_sub,
            started_at,
            upload_success_job_at,
            stitched_s3_key,
            journal_master_key_uid,
            transcribe_job_queued_at,
            encrypted_transcription_vtt,
            transcription_source,
            transcribe_job_finished_at,
            transcode_job_queued_at,
            transcode_content_file_uid,
            transcode_job_finished_at,
            analyze_job_queued_at,
            encrypted_time_vs_intensity,
            analyze_job_finished_at,
            finalize_job_queued_at,
        ) = cast(
            List[Optional[bytes]],
            await redis.hmget(
                b"voice_notes:processing:" + voice_note_uid_bytes,  # type: ignore
                b"user_sub",  # type: ignore
                b"started_at",  # type: ignore
                b"upload_success_job_at",  # type: ignore
                b"stitched_s3_key",  # type: ignore
                b"journal_master_key_uid",  # type: ignore
                b"transcribe_job_queued_at",  # type: ignore
                b"encrypted_transcription_vtt",  # type: ignore
                b"transcription_source",  # type: ignore
                b"transcribe_job_finished_at",  # type: ignore
                b"transcode_job_queued_at",  # type: ignore
                b"transcode_content_file_uid",  # type: ignore
                b"transcode_job_finished_at",  # type: ignore
                b"analyze_job_queued_at",  # type: ignore
                b"encrypted_time_vs_intensity",  # type: ignore
                b"analyze_job_finished_at",  # type: ignore
                b"finalize_job_queued_at",  # type: ignore
            ),
        )

        if (
            user_sub is None
            or started_at is None
            or upload_success_job_at is None
            or stitched_s3_key is None
            or journal_master_key_uid is None
            or transcribe_job_queued_at is None
            or encrypted_transcription_vtt is None
            or transcription_source is None
            or transcribe_job_finished_at is None
            or transcode_job_queued_at is None
            or transcode_content_file_uid is None
            or transcode_job_finished_at is None
            or analyze_job_queued_at is None
            or encrypted_time_vs_intensity is None
            or analyze_job_finished_at is None
            or finalize_job_queued_at is None
            or stitched_s3_key == b"not_yet"
            or encrypted_transcription_vtt == b"not_yet"
            or journal_master_key_uid == b"not_yet"
            or transcription_source == b"not_yet"
            or transcode_content_file_uid == b"not_yet"
            or encrypted_time_vs_intensity == b"not_yet"
        ):
            await handle_warning(
                f"{__name__}:not_ready",
                (
                    f"Cannot finalize voice note `{voice_note_uid}` because it is not ready:\n"
                    f"• user sub: `{user_sub}`\n"
                    f"• started at: `{started_at}`\n"
                    f"• upload success job at: `{upload_success_job_at}`\n"
                    f"• stitched s3 key: `{stitched_s3_key}`\n"
                    f"• journal master key uid: `{journal_master_key_uid}`\n"
                    f"• transcribe job queued at: `{transcribe_job_queued_at}`\n"
                    f"• encrypted transcription vtt (first 6): `{None if encrypted_transcription_vtt is None else encrypted_transcription_vtt[:6]}`\n"
                    f"• transcription source: `{transcription_source}`\n"
                    f"• transcribe job finished at: `{transcribe_job_finished_at}`\n"
                    f"• transcode job queued at: `{transcode_job_queued_at}`\n"
                    f"• transcode content file uid: `{transcode_content_file_uid}`\n"
                    f"• transcode job finished at: `{transcode_job_finished_at}`\n"
                    f"• analyze job queued at: `{analyze_job_queued_at}`\n"
                    f"• encrypted time vs intensity (first 6): `{None if encrypted_time_vs_intensity is None else encrypted_time_vs_intensity[:6]}`\n"
                    f"• analyze job finished at: `{analyze_job_finished_at}`\n"
                    f"• finalize job queued at: `{finalize_job_queued_at}`"
                ),
            )
            raise CustomFailureReasonException("not ready")

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_s3_file", key=stitched_s3_key.decode("utf-8")
        )

        files = await itgs.files()
        transcript_s3_file_key = f"s3_files/voice_notes/transcripts/{voice_note_uid}/{secrets.token_urlsafe(4)}.vtt.fernet"
        transcript_s3_file_uid = f"oseh_s3f_{secrets.token_urlsafe(16)}"
        time_vs_intensity_s3_file_key = f"s3_files/voice_notes/time_vs_intensity/{voice_note_uid}/{secrets.token_urlsafe(4)}.jsonlines.fernet"
        time_vs_intensity_s3_file_uid = f"oseh_s3f_{secrets.token_urlsafe(16)}"

        transcript_purgatory_key = json.dumps(
            {"key": transcript_s3_file_key, "bucket": files.default_bucket},
            sort_keys=True,
        )
        time_vs_intensity_purgatory_key = json.dumps(
            {"key": time_vs_intensity_s3_file_key, "bucket": files.default_bucket},
            sort_keys=True,
        )
        await redis.zadd(
            "files:purgatory",
            mapping={
                transcript_purgatory_key: time.time() + 3000,
                time_vs_intensity_purgatory_key: time.time() + 3000,
            },
        )
        await helper.push_progress(
            "moving encrypted transcript and analysis to s3",
            indicator={"type": "spinner"},
        )
        await asyncio.gather(
            files.upload(
                io.BytesIO(encrypted_transcription_vtt),
                bucket=files.default_bucket,
                key=transcript_s3_file_key,
                sync=True,
            ),
            files.upload(
                io.BytesIO(encrypted_time_vs_intensity),
                bucket=files.default_bucket,
                key=time_vs_intensity_s3_file_key,
                sync=True,
            ),
        )

        await helper.push_progress("finishing up", indicator={"type": "spinner"})
        user_sub_str = user_sub.decode("utf-8")
        voice_note_created_at = time.time()
        conn = await itgs.conn()
        cursor = conn.cursor()
        response = await cursor.executeunified3(
            (
                (
                    "SELECT 1 FROM users WHERE sub=?",
                    (user_sub_str,),
                ),
                (
                    "SELECT 1 FROM voice_notes WHERE uid=?",
                    (voice_note_uid,),
                ),
                (
                    "SELECT 1 FROM content_files WHERE uid=?",
                    (transcode_content_file_uid.decode("utf-8"),),
                ),
                (
                    """
SELECT 1 FROM user_journal_master_keys
WHERE
    uid = ?
    AND user_id = (SELECT users.id FROM users WHERE users.sub = ?)
                    """,
                    (
                        journal_master_key_uid.decode("utf-8"),
                        user_sub_str,
                    ),
                ),
                (
                    """
INSERT INTO s3_files (
    uid, key, file_size, content_type, created_at
)
VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)
                    """,
                    (
                        transcript_s3_file_uid,
                        transcript_s3_file_key,
                        len(encrypted_transcription_vtt),
                        f"text/vtt; encryption=fernet+{journal_master_key_uid.decode('utf-8')}",
                        voice_note_created_at,
                        time_vs_intensity_s3_file_uid,
                        time_vs_intensity_s3_file_key,
                        len(encrypted_time_vs_intensity),
                        f"application/jsonlines; encryption=fernet+{journal_master_key_uid.decode('utf-8')}",
                        voice_note_created_at,
                    ),
                ),
                (
                    """
INSERT INTO voice_notes (
    uid,
    user_id,
    user_journal_master_key_id,
    transcript_s3_file_id,
    transcription_source,
    audio_content_file_id,
    time_vs_avg_signal_intensity_s3_file_id,
    created_at
)
SELECT
    ?,
    users.id,
    user_journal_master_keys.id,
    transcript_s3_files.id,
    ?,
    content_files.id,
    time_vs_avg_signal_intensity_s3_files.id,
    ?
FROM
    users,
    user_journal_master_keys,
    s3_files AS transcript_s3_files,
    s3_files AS time_vs_avg_signal_intensity_s3_files,
    content_files
WHERE
    users.sub = ?
    AND user_journal_master_keys.uid = ?
    AND user_journal_master_keys.user_id = users.id
    AND transcript_s3_files.key = ?
    AND time_vs_avg_signal_intensity_s3_files.key = ?
    AND content_files.uid = ?
    AND NOT EXISTS (
        SELECT 1 FROM voice_notes AS vn WHERE vn.uid = ?
    )
                    """,
                    (
                        voice_note_uid,
                        transcription_source.decode("utf-8"),
                        voice_note_created_at,
                        user_sub_str,
                        journal_master_key_uid.decode("utf-8"),
                        transcript_s3_file_key,
                        time_vs_intensity_s3_file_key,
                        transcode_content_file_uid.decode("utf-8"),
                        voice_note_uid,
                    ),
                ),
            )
        )

        users_response = response[0]
        voice_notes_response = response[1]
        content_files_response = response[2]
        user_journal_master_keys_response = response[3]
        s3_files_insert_response = response[4]
        voice_notes_insert_response = response[5]

        did_insert = (
            voice_notes_insert_response.rows_affected is not None
            and voice_notes_insert_response.rows_affected > 0
        )

        if did_insert:
            async with redis.pipeline() as pipe:
                pipe.multi()
                await pipe.zrem(
                    "files:purgatory",
                    transcript_purgatory_key,
                    time_vs_intensity_purgatory_key,
                )
                await pipe.delete(b"voice_notes:processing:" + voice_note_uid_bytes)
                await pipe.zrem(b"voice_notes:processing", voice_note_uid_bytes)
                await pipe.execute()

        if not users_response.results:
            assert not did_insert, "inserted despite user not found"
            await handle_warning(
                f"{__name__}:user_not_found",
                f"Cannot finalize voice note `{voice_note_uid}` because the user `{user_sub_str}` was not found",
            )
            raise CustomFailureReasonException("user not found")

        if voice_notes_response.results:
            assert not did_insert, "inserted despite voice note already existing"
            await handle_warning(
                f"{__name__}:voice_note_exists",
                f"Cannot finalize voice note `{voice_note_uid}` because it already exists",
            )
            raise CustomFailureReasonException("voice note already finalized")

        if not content_files_response.results:
            assert not did_insert, "inserted despite content file not found"
            await handle_warning(
                f"{__name__}:content_file_not_found",
                f"Cannot finalize voice note `{voice_note_uid}` because the content file `{transcode_content_file_uid.decode('utf-8')}` was not found",
            )
            raise CustomFailureReasonException("content file not found")

        if not user_journal_master_keys_response.results:
            assert not did_insert, "inserted despite journal key not found"
            await handle_warning(
                f"{__name__}:journal_key_not_found",
                f"Cannot finalize voice note `{voice_note_uid}` because the journal key `{journal_master_key_uid.decode('utf-8')}` was not found",
            )
            raise CustomFailureReasonException("journal key not found")

        if s3_files_insert_response.rows_affected != 2:
            assert not did_insert, "inserted despite s3 files not inserted"
            await handle_warning(
                f"{__name__}:s3_file_not_inserted",
                f"Cannot finalize voice note `{voice_note_uid}` because the s3 file `{transcript_s3_file_key}` or `{time_vs_intensity_s3_file_key}` was not inserted",
            )
            raise CustomFailureReasonException("failed to move transcript to s3")

        if not did_insert:
            await handle_warning(
                f"{__name__}:insert_failed",
                f"Failed to finalize voice note `{voice_note_uid}` unexpectedly",
            )
            raise CustomFailureReasonException("failed to insert")

        if voice_notes_insert_response.rows_affected != 1:
            await handle_warning(
                f"{__name__}:inserted_many",
                f"Failed to finalize voice note `{voice_note_uid}` - more than 1 row affected? `{voice_notes_insert_response.rows_affected=}`",
            )
            raise CustomFailureReasonException("inserted many")

        time_since_upload_started = voice_note_created_at - float(started_at)
        slack = await itgs.slack()
        await slack.send_ops_message(
            f"{socket.gethostname()} finalized voice note `{voice_note_uid}` for `{user_sub_str}` {time_since_upload_started:.3f}s since voice note upload started",
        )
