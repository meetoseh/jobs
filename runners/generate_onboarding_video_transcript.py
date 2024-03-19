"""Generates a transcript for a content file that was exported to be used as
an onboarding video. Does not necessarily have to be a row in 
`onboarding_video_uploads`, though that is the common case.
"""

import os
from typing import Optional, cast
from content import hash_filelike
from itgs import Itgs
from graceful_death import GracefulDeath
import aiofiles

from jobs import JobCategory
from lib.progressutils.async_progress_tracking_bytes_io import (
    AsyncProgressTrackingReadableBytesIO,
    AsyncProgressTrackingWritableBytesIO,
)
from lib.progressutils.success_or_failure_reporter import (
    CustomFailureReasonException,
    CustomSuccessReasonException,
    success_or_failure_reporter,
)
from lib.transcripts.db import store_transcript_for_content_file
from lib.transcripts.gen import create_transcript
from temp_files import temp_file

category = JobCategory.HIGH_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    content_file_uid: str,
    job_progress_uid: Optional[str] = None,
):
    """Generates a transcript for the content file with the given uid, if it
    exists and doesn't already have one.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        content_file_uid (str): the uid of the content file to generate a transcript for
        job_progress_uid (str): If specified, used to push job progress
    """
    async with success_or_failure_reporter(
        itgs, job_progress_uid=job_progress_uid
    ) as prog:
        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        await prog.push_progress(
            "Locating best available source audio...", indicator={"type": "spinner"}
        )

        response = await cursor.executeunified3(
            (
                (
                    "SELECT 1 FROM content_files WHERE uid = ?",
                    (content_file_uid,),
                ),
                (
                    """
SELECT
    transcripts.uid,
    transcripts.source,
    transcripts.created_at
FROM content_files, content_file_transcripts, transcripts
WHERE
    content_files.uid = ?
    AND content_file_transcripts.content_file_id = content_files.id
    AND content_file_transcripts.transcript_id = transcripts.id
ORDER BY content_file_transcripts.created_at DESC, content_file_transcripts.uid ASC
LIMIT 1
                    """,
                    (content_file_uid,),
                ),
                (
                    """
SELECT
    content_files.original_sha512,
    original_s3_files.key,
    original_s3_files.file_size,
    best_export_s3_files.key,
    best_export_s3_files.file_size,
    best_exports.format_parameters,
    best_exports.quality_parameters
FROM content_files
LEFT OUTER JOIN s3_files AS original_s3_files 
    ON original_s3_files.id = content_files.original_s3_file_id
LEFT OUTER JOIN content_file_exports AS best_exports
    ON (
        best_exports.content_file_id = content_files.id
        AND best_exports.format = 'mp4'
        AND NOT EXISTS (
            SELECT 1 FROM content_file_exports AS cfe
            WHERE
                cfe.content_file_id = best_exports.content_file_id
                AND cfe.format = 'mp4'
                AND (
                    (COALESCE(
                        json_extract(cfe.quality_parameters, '$.target_audio_bitrate_kbps'),
                        0
                    ) > COALESCE(
                        json_extract(best_exports.quality_parameters, '$.target_audio_bitrate_kbps'),
                        0
                    ))
                    OR (
                        (
                            json_extract(cfe.quality_parameters, '$.target_audio_bitrate_kbps') IS NULL
                            OR json_extract(best_exports.quality_parameters, '$.target_audio_bitrate_kbps') IS NULL
                        )
                        AND (
                            cfe.bandwidth > best_exports.bandwidth
                            OR (
                                cfe.bandwidth = best_exports.bandwidth
                                AND cfe.uid < best_exports.uid
                            )
                        )
                    )
                    OR (
                        json_extract(cfe.quality_parameters, '$.target_audio_bitrate_kbps') IS NOT NULL
                        AND json_extract(best_exports.quality_parameters, '$.target_audio_bitrate_kbps') IS NOT NULL
                        AND json_extract(cfe.quality_parameters, '$.target_audio_bitrate_kbps') = json_extract(best_exports.quality_parameters, '$.target_audio_bitrate_kbps')
                        AND (
                            cfe.bandwidth < best_exports.bandwidth
                            OR (
                                cfe.bandwidth = best_exports.bandwidth
                                AND cfe.uid < best_exports.uid
                            )
                        )
                    )
                )
        )
    )
LEFT OUTER JOIN content_file_export_parts AS best_export_parts
    ON (
        best_export_parts.content_file_export_id = best_exports.id
        AND NOT EXISTS (
            SELECT 1 FROM content_file_export_parts AS cfep
            WHERE
                cfep.content_file_export_id = best_exports.id
                AND cfep.position < best_export_parts.position
        )
    )
LEFT OUTER JOIN s3_files AS best_export_s3_files
    ON best_export_s3_files.id = best_export_parts.s3_file_id
WHERE content_files.uid = ?
""",
                    (content_file_uid,),
                ),
            ),
        )

        existence_response = response.items[0]
        transcript_response = response.items[1]
        source_response = response.items[2]

        if not existence_response.results:
            raise CustomFailureReasonException(f"there is no {content_file_uid=}")

        if transcript_response.results:
            transcript_uid = cast(str, transcript_response.results[0][0])
            transcript_source = cast(str, transcript_response.results[0][1])
            transcript_created_at = cast(str, transcript_response.results[0][2])

            raise CustomSuccessReasonException(
                f"there is already a transcript for {content_file_uid=}: {transcript_uid=}, {transcript_source=}, {transcript_created_at=}"
            )

        assert source_response.results, response

        original_sha512 = cast(str, source_response.results[0][0])
        original_key = cast(Optional[str], source_response.results[0][1])
        original_file_size = cast(Optional[int], source_response.results[0][2])
        best_export_key = cast(Optional[str], source_response.results[0][3])
        best_export_file_size = cast(Optional[int], source_response.results[0][4])
        best_export_format_parameters = cast(
            Optional[str], source_response.results[0][5]
        )
        best_export_quality_parameters = cast(
            Optional[str], source_response.results[0][6]
        )

        audio_required_sha512: Optional[str] = None
        if original_key is not None:
            assert original_file_size is not None, response
            audio_name_hint = "original file"
            audio_s3_key = original_key
            audio_required_sha512 = original_sha512
            audio_file_size = original_file_size
        elif best_export_key is not None:
            assert best_export_file_size is not None, response
            audio_name_hint = f"export with {best_export_format_parameters=}, {best_export_quality_parameters=}"
            audio_s3_key = best_export_key
            audio_required_sha512 = None
            audio_file_size = best_export_file_size
        else:
            raise CustomFailureReasonException(
                f"could not locate a suitable source audio for {content_file_uid=}"
            )

        transcript_prompt: Optional[str] = "Oseh"

        files = await itgs.files()
        with temp_file() as source_filepath:
            async with aiofiles.open(
                source_filepath, "wb"
            ) as raw_file, AsyncProgressTrackingWritableBytesIO(
                itgs,
                job_progress_uid=job_progress_uid,
                expected_file_size=audio_file_size,
                delegate=raw_file,
                message=f"downloading {audio_name_hint}",
            ) as tracked_file:
                await files.download(
                    tracked_file,
                    bucket=files.default_bucket,
                    key=audio_s3_key,
                    sync=False,
                )

            if audio_required_sha512 is not None:
                real_file_size = os.path.getsize(source_filepath)
                async with aiofiles.open(
                    source_filepath, "rb"
                ) as raw_file, AsyncProgressTrackingReadableBytesIO(
                    itgs,
                    job_progress_uid=job_progress_uid,
                    expected_file_size=real_file_size,
                    delegate=raw_file,
                    message=f"verifying {audio_name_hint}",
                ) as tracked_file:
                    real_sha512 = await hash_filelike(tracked_file)

                if real_sha512 != audio_required_sha512:
                    raise CustomFailureReasonException(
                        f"the downloaded {audio_name_hint} does not match the expected hash: {real_sha512=}, {audio_required_sha512=}"
                    )

            await prog.push_progress(
                "generating transcript", indicator={"type": "spinner"}
            )
            result = await create_transcript(
                itgs, source_filepath, prompt=transcript_prompt
            )
            await prog.push_progress(
                "storing transcript", indicator={"type": "spinner"}
            )
            await store_transcript_for_content_file(
                itgs,
                content_file_uid=content_file_uid,
                transcript=result.transcript,
                source=result.source,
            )


if __name__ == "__main__":
    import asyncio

    async def main():
        content_file_uid = input("content file uid: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.generate_onboarding_video_transcript",
                content_file_uid=content_file_uid,
            )

    asyncio.run(main())
