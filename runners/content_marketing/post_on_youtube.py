import json
import secrets
import socket
import time
from typing import List, Optional, Union, cast
from content import hash_content
from error_middleware import handle_contextless_error, handle_error, handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.basic_redis_lock import basic_redis_lock
import os
import apiclient
import google.oauth2.credentials
import google.auth.transport.requests

from temp_files import temp_file


category = JobCategory.LOW_RESOURCE_COST

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, journey_uid: str, dry_run: Optional[bool] = None
):
    """Makes a new post to our YouTube channel using the full video from the given journey.

    If the journey has a youtube video already but it hasn't finished uploading, this will
    resume uploading

    If the journey has a youtube video already and it has finished uploading, this will
    do nothing

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_uid (str): The uid of the journey to post
        dry_run (bool, None): True to not actually post anything, false to post,
            None to decide based on the current environment (i.e., post only in prod)
    """
    if dry_run is None:
        dry_run = os.environ["ENVIRONMENT"] != "production"

    async with basic_redis_lock(itgs, key="youtube:lock", gd=gd, spin=False):
        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        response = await cursor.executeunified3(
            (
                (
                    """
INSERT INTO journey_youtube_videos (
    uid,
    journey_id,
    content_file_id,
    title,
    description,
    tags,
    category,
    youtube_video_id,
    started_at,
    finished_at
)
SELECT
    ?,
    journeys.id,
    journeys.video_content_file_id,
    journeys.title,
    journeys.description,
    json_array(
        'oseh',
        instructors.name,
        journey_subcategories.external_name
    ),
    '24',
    NULL,
    ?,
    NULL
FROM journeys, instructors, journey_subcategories
WHERE
    journeys.uid = ?
    AND instructors.id = journeys.instructor_id
    AND journey_subcategories.id = journeys.journey_subcategory_id
    AND journeys.video_content_file_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM journey_youtube_videos AS jyv
        WHERE
            jyv.journey_id = (SELECT journeys.id FROM journeys AS j WHERE j.uid = ?)
    )
    AND EXISTS (
        SELECT 1 FROM content_files
        WHERE
            content_files.id = journeys.video_content_file_id
            AND content_files.original_s3_file_id IS NOT NULL
    )
                    """,
                    [
                        f"oseh_jyv_{secrets.token_urlsafe(16)}",
                        time.time(),
                        journey_uid,
                        journey_uid,
                    ],
                ),
                (
                    """
SELECT
    journey_youtube_videos.uid,
    journey_youtube_videos.title,
    journey_youtube_videos.description,
    journey_youtube_videos.tags,
    journey_youtube_videos.category,
    journey_youtube_videos.youtube_video_id,
    journey_youtube_videos.started_at,
    journey_youtube_videos.finished_at,
    content_files.uid,
    content_files.original_sha512,
    content_files.duration_seconds,
    s3_files.key
FROM
    journey_youtube_videos,
    content_files,
    s3_files
WHERE
    journey_youtube_videos.journey_id = (
        SELECT journeys.id FROM journeys WHERE journeys.uid = ?
    )
    AND journey_youtube_videos.content_file_id = content_files.id
    AND content_files.original_s3_file_id = s3_files.id
                    """,
                    (journey_uid,),
                ),
            ),
        )

        is_new_record = (
            response[0].rows_affected is not None and response[0].rows_affected > 0
        )
        found_record = not not response[1].results

        if is_new_record and not found_record:
            await handle_contextless_error(
                extra_info=f"Inserted a new journey_youtube_videos record for journey {journey_uid} but couldn't find it"
            )
            return

        if not is_new_record and not found_record:
            await handle_warning(
                f"{__name__}:journey_does_not_exist",
                f"Tried to post journey uid=`{journey_uid}` to YouTube but it doesn't exist or is missing required data",
            )
            return

        assert found_record, response
        assert response[1].results

        row = response[1].results[0]
        record_uid = cast(str, row[0])
        record_title = cast(str, row[1])
        record_description = cast(str, row[2])
        record_tags_raw = cast(str, row[3])
        record_category = cast(str, row[4])
        record_youtube_video_id = cast(Optional[str], row[5])
        record_started_at = cast(float, row[6])
        record_finished_at = cast(Optional[float], row[7])
        content_file_uid = cast(str, row[8])
        content_file_sha512 = cast(str, row[9])
        content_file_duration_seconds = cast(Union[int, float], row[10])
        s3_file_key = cast(str, row[11])

        record_tags = cast(List[str], json.loads(record_tags_raw))
        assert isinstance(record_tags, list), record_tags_raw
        assert all(isinstance(tag, str) for tag in record_tags), record_tags_raw

        if record_finished_at is not None or record_youtube_video_id is not None:
            await handle_warning(
                f"{__name__}:already_posted",
                f"Journey `{journey_uid}` has already been posted to YouTube as `{record_youtube_video_id}` (started: {record_started_at}, finished: {record_finished_at})",
            )
            return

        logging.info(
            f"Preparing to post journey {journey_uid} to Youtube:\n"
            f"  - Title: {record_title}\n"
            f"  - Description: {record_description}\n"
            f"  - Tags: {record_tags_raw}\n"
            f"  - Category: {record_category}\n"
            f"  - Video: {content_file_uid} ({content_file_duration_seconds}s)"
            f"  - S3: {s3_file_key} ({content_file_sha512})"
        )

        files = await itgs.files()
        with temp_file() as source_file:
            logging.debug("Downloading...")
            with open(source_file, "wb") as f:
                await files.download(
                    f, bucket=files.default_bucket, key=s3_file_key, sync=True
                )

            logging.debug("Checking integrity...")
            retrieved_sha512 = await hash_content(source_file)

            if retrieved_sha512 != content_file_sha512:
                await handle_warning(
                    f"{__name__}:original_file_corrupted",
                    f"Content file `{content_file_uid}` for journey `{journey_uid}` to produce `{record_uid}` has SHA512 `{content_file_sha512}` but downloaded file has SHA512 `{retrieved_sha512}`",
                )
                return

            logging.debug("Initializing youtube...")

            redis = await itgs.redis()
            response = cast(
                List[bytes],
                await redis.hmget(
                    b"youtube:authorization",  # type: ignore
                    [b"access_token", b"refresh_token"],
                ),
            )

            if not all(response) or not all(
                isinstance(token, bytes) for token in response
            ):
                await handle_warning(
                    f"{__name__}:youtube_auth_missing",
                    f"Missing one or more YouTube authorization tokens",
                )
                return

            access_token = response[1].decode("utf-8")
            refresh_token = response[2].decode("utf-8")

            creds = google.oauth2.credentials.Credentials(
                access_token, refresh_token=refresh_token
            )
            if creds.expired:
                logging.info("Refreshng credentials...")
                creds.refresh(google.auth.transport.requests.Request())
                logging.info(
                    "Refreshed credentials, storing updated access token and refresh token"
                )
                await itgs.ensure_redis_liveliness()
                redis = await itgs.redis()
                await redis.hset(
                    b"youtube:authorization",  # type: ignore
                    mapping={
                        "access_token": creds.token,
                        "refresh_token": creds.refresh_token,
                    },
                )

            youtube = apiclient.discovery.build("youtube", "v3", credentials=creds)
            body = {
                "snippet": {
                    "title": record_title,
                    "description": record_description,
                    "tags": record_tags,
                    "categoryId": record_category,
                },
                "status": {
                    "privacyStatus": "public",
                },
            }

            logging.debug("Making insert request using:\n" + json.dumps(body, indent=2))

            if dry_run:
                logging.info("DRY RUN: exiting without actually starting upload")
                return

            insert_request = youtube.videos().insert(
                part=",".join(body.keys()),
                body=body,
                media_body=apiclient.http.MediaFileUpload(
                    source_file, chunksize=-1, resumable=True
                ),
            )

            try:
                while True:
                    logging.debug("Uploading file...")
                    status, response = insert_request.next_chunk()
                    if response is None:
                        continue

                    if "id" not in response:
                        await handle_warning(
                            f"{__name__}:upload_failed",
                            f"The upload failed with an unexpected response\n```\n{response}\n```",
                        )

                    new_video_id = cast(str, response["id"])
                    break
            except Exception as e:
                await handle_error(
                    e,
                    extra_info=f"An error occurred while uploading the video",
                )
                return

            logging.info(f"Successfully uploaded video to YouTube as `{new_video_id}`")

            await cursor.execute(
                "UPDATE journey_youtube_videos SET youtube_video_id = ?, finished_at = ? WHERE uid = ?",
                [new_video_id, time.time(), record_uid],
            )

        slack = await itgs.slack()
        await slack.send_ops_message(
            f"{socket.gethostname()} posted {record_title} to YouTube, available at https://www.youtube.com/watch?v={new_video_id}"
        )


if __name__ == "__main__":
    import asyncio
    import argparse

    async def main():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--journey_uid",
            type=str,
            required=True,
            help="The uid of the journey to post",
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--no-dry-run", action="store_true")
        args = parser.parse_args()

        dry_run = None
        if args.dry_run:
            dry_run = True
        if args.no_dry_run:
            dry_run = False

        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.content_marketing.post_on_youtube",
                journey_uid=args.journey_uid,
                dry_run=dry_run,
            )

    asyncio.run(main())
