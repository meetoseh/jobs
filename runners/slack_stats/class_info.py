import os
import secrets
import time
from typing import Optional, cast

import aioboto3
from itgs import Itgs
from graceful_death import GracefulDeath
import csv

from jobs import JobCategory
from temp_files import temp_file
import pytz
import unix_dates
import socket

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Produces a csv with information about our classes, uploads it to s3, and
    posts a message to slack with the s3 key used.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    last_created_at: Optional[float] = None
    last_uid: Optional[str] = None

    presign_length_seconds = 60 * 60 * 24 * 7 - 1
    files = await itgs.files()

    if os.environ["ENVIRONMENT"] != "dev":
        s3_session = aioboto3.Session()
        s3_client = s3_session.client("s3")
        await s3_client.__aenter__()
    else:
        s3_session = None
        s3_client = None

    tz = pytz.timezone("US/Pacific")
    num_classes = 0
    try:
        with temp_file(".csv") as out_path:
            with open(out_path, "w") as out_file:
                csvw = csv.writer(out_file)
                csvw.writerow(
                    [
                        "title",
                        "instructor",
                        "length",
                        "category_external",
                        "category_internal",
                        "created_at",
                        "description",
                        "uid",
                        "video_s3_key",
                        "video_presigned_url",
                        "audio_s3_key",
                        "audio_presigned_url",
                    ]
                )

                while True:
                    response = await cursor.execute(
                        """
    SELECT
        journeys.title,
        instructors.name,
        audio_content_files.duration_seconds,
        journey_subcategories.external_name,
        journey_subcategories.internal_name,
        journeys.created_at,
        journeys.description,
        journeys.uid,
        s3_files.key,
        audio_s3_files.key
    FROM 
        journeys,
        instructors,
        content_files,
        content_files AS audio_content_files,
        s3_files,
        s3_files AS audio_s3_files,
        journey_subcategories
    WHERE
        journeys.deleted_at IS NULL
        AND journeys.special_category IS NULL
        AND journeys.video_content_file_id = content_files.id
        AND s3_files.id = content_files.original_s3_file_id
        AND journeys.audio_content_file_id = audio_content_files.id
        AND audio_s3_files.id = audio_content_files.original_s3_file_id
        AND instructors.id = journeys.instructor_id
        AND journey_subcategories.id = journeys.journey_subcategory_id
        AND NOT EXISTS (
            SELECT 1 FROM journeys AS alt_journeys, content_files AS alt_content_files
            WHERE
                (
                    alt_journeys.id = journeys.variation_of_journey_id
                    OR alt_journeys.variation_of_journey_id = journeys.id
                )
                AND alt_journeys.deleted_at IS NULL
                AND alt_journeys.special_category IS NULL
                AND alt_journeys.video_content_file_id = alt_content_files.id
                AND (
                    alt_content_files.duration_seconds > content_files.duration_seconds
                    OR (
                        alt_content_files.duration_seconds = content_files.duration_seconds
                        AND alt_journeys.uid < journeys.uid
                    )
                )    
        )
        AND (
            ? IS NULL
            OR journeys.created_at > ?
            OR (
                journeys.created_at = ?
                AND journeys.uid > ?
            )
        )
    ORDER BY journeys.created_at ASC, journeys.uid ASC
    LIMIT 100
                        """,
                        (last_created_at, last_created_at, last_created_at, last_uid),
                    )

                    if not response.results:
                        break

                    for row in response.results:
                        row_title = cast(str, row[0])
                        row_instructor = cast(str, row[1])
                        row_length = cast(float, row[2])
                        row_category_external = cast(str, row[3])
                        row_category_internal = cast(str, row[4])
                        row_created_at = cast(float, row[5])
                        row_description = cast(str, row[6])
                        row_uid = cast(str, row[7])
                        row_video_s3_key = cast(str, row[8])
                        row_audio_s3_key = cast(str, row[9])

                        last_created_at = row_created_at
                        last_uid = row_uid
                        num_classes += 1

                        created_at_pretty = unix_dates.unix_timestamp_to_datetime(
                            row_created_at, tz=tz
                        ).strftime("%Y-%m-%d %H:%M:%S %Z")

                        video_presigned_url = ""
                        if s3_client is not None:
                            video_presigned_url = (
                                await s3_client.generate_presigned_url(
                                    "get_object",
                                    Params={
                                        "Bucket": files.default_bucket,
                                        "Key": row_video_s3_key,
                                    },
                                    ExpiresIn=presign_length_seconds,
                                )
                            )

                        audio_presigned_url = ""
                        if s3_client is not None:
                            audio_presigned_url = (
                                await s3_client.generate_presigned_url(
                                    "get_object",
                                    Params={
                                        "Bucket": files.default_bucket,
                                        "Key": row_audio_s3_key,
                                    },
                                    ExpiresIn=presign_length_seconds,
                                )
                            )

                        csvw.writerow(
                            [
                                row_title,
                                row_instructor,
                                row_length,
                                row_category_external,
                                row_category_internal,
                                created_at_pretty,
                                row_description,
                                row_uid,
                                row_video_s3_key,
                                video_presigned_url,
                                row_audio_s3_key,
                                audio_presigned_url,
                            ]
                        )

            upload_key = (
                f"s3_files/class_info/{int(time.time())}-{secrets.token_urlsafe(4)}.csv"
            )
            with open(out_path, "rb") as f:
                await files.upload(
                    f, bucket=files.default_bucket, key=upload_key, sync=True
                )

            upload_presigned_url = upload_key
            if s3_client is not None:
                upload_presigned_url = await s3_client.generate_presigned_url(
                    "get_object",
                    Params={
                        "Bucket": files.default_bucket,
                        "Key": upload_key,
                    },
                    ExpiresIn=presign_length_seconds,
                )

            slack = await itgs.slack()
            await slack.send_oseh_bot_message(
                f"{socket.gethostname()} produced class info CSV ({num_classes} classes) at {upload_presigned_url}"
            )
    finally:
        if s3_client is not None:
            await s3_client.__aexit__(None, None, None)


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.slack_stats.class_info")

    asyncio.run(main())
