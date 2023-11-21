"""a very simple job"""
import dataclasses
import json
import os
import secrets
from typing import Optional
import aiohttp
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from shareables.generate_class.main import run_pipeline
from audio import ProcessAudioAbortedException, process_audio
from temp_files import temp_dir
import socket
import time
import urllib.parse
from runners.process_instructor_profile_picture import (
    process_instructor_profile_picture,
)

category = JobCategory.HIGH_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, only_in_env: Optional[str] = None):
    """Creates a new AI journey and saves it to the database.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    if only_in_env is not None and os.environ["ENVIRONMENT"] != only_in_env:
        logging.debug(
            f"Skipping create_ai_journey request for {only_in_env} since we're in {os.environ['ENVIRONMENT']}"
        )
        return

    with temp_dir() as dest_folder:
        logging.info(f"Generating new journey into {dest_folder}...")

        slack = await itgs.slack()
        await slack.send_ops_message(
            f"Generating new journey on {socket.gethostname()}..."
        )
        started_at = time.time()

        generated = await run_pipeline(gd=gd, dest_folder=dest_folder)
        if gd.received_term_signal:
            logging.info("Received term signal, aborting creating class")
            return
        assert generated is not None
        generated_class_at = time.time()
        logging.info(
            f"Finished generating class audio in {generated_class_at - started_at:.3f} seconds, processing..."
        )

        try:
            content = await process_audio(
                generated.path,
                itgs=itgs,
                gd=gd,
                max_file_size=1024 * 1024 * 1024,
                name_hint="ai-journey_audio_content",
            )
        except ProcessAudioAbortedException:
            logging.info("Aborted processing class audio")
            return

        processed_at = time.time()
        logging.info(
            f"Finished processing class audio in {processed_at - generated_class_at:.3f} seconds, selecting background..."
        )

        conn = await itgs.conn()
        cursor = conn.cursor("none")

        response = await cursor.execute(
            "SELECT uid FROM journey_background_images ORDER BY RANDOM() LIMIT 1"
        )
        assert (
            response.results is not None
        ), "no journey background images to choose from"
        background_uid = response.results[0][0]
        background_selected_at = time.time()
        logging.info(
            f"Finished selecting background in {background_selected_at - processed_at:.3f} seconds, creating instructor..."
        )

        cursor.read_consistency = "weak"
        response = await cursor.execute(
            "SELECT uid FROM instructors WHERE name=? ORDER BY uid ASC LIMIT 1",
            (generated.meta.instructor,),
        )
        if response.results:
            instructor_uid: str = response.results[0][0]
            logging.debug(f"Instructor already exists with uid {instructor_uid}")
        else:
            logging.debug("Instructor doesn't exist, creating...")
            picture_url = f"https://avatars.dicebear.com/api/bottts/{urllib.parse.quote(generated.meta.instructor)}.svg"
            picture_file_loc = os.path.join(dest_folder, "picture.svg")
            async with aiohttp.ClientSession() as session:
                async with session.get(picture_url) as resp:
                    if not resp.ok:
                        await handle_warning(
                            f"{__name__}:bad_picture_url",
                            f"{picture_url=} returned {resp.status}",
                        )
                        return

                    with open(picture_file_loc, "wb") as f:
                        while True:
                            chunk = await resp.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)

            instructor_uid = f"oseh_i_{secrets.token_urlsafe(16)}"
            response = await cursor.execute(
                "INSERT INTO instructors (uid, name, created_at) VALUES (?, ?, ?)",
                (instructor_uid, generated.meta.instructor, time.time()),
            )
            await process_instructor_profile_picture(
                itgs,
                gd,
                stitched_path=picture_file_loc,
                uploaded_by_user_sub=None,
                instructor_uid=instructor_uid,
            )

        instructor_created_at = time.time()
        logging.info(
            f"Finished creating instructor in {instructor_created_at - background_selected_at:.3f} seconds, creating journey..."
        )

        interactive_prompt_uid = f"oseh_ip_{secrets.token_urlsafe(16)}"
        journey_uid = f"oseh_j_{secrets.token_urlsafe(16)}"
        now = time.time()
        response = await cursor.executemany3(
            (
                (
                    """
                    INSERT INTO interactive_prompts (
                        uid,
                        prompt,
                        duration_seconds,
                        created_at,
                        deleted_at
                    )
                    VALUES (?, ?, ?, ?, NULL)
                    """,
                    (
                        interactive_prompt_uid,
                        json.dumps(dataclasses.asdict(generated.meta.prompt)),
                        10,
                        now,
                    ),
                ),
                (
                    """
                    INSERT INTO journeys (
                        uid,
                        audio_content_file_id,
                        background_image_file_id,
                        blurred_background_image_file_id,
                        darkened_background_image_file_id,
                        instructor_id,
                        title,
                        description,
                        journey_subcategory_id,
                        interactive_prompt_id,
                        created_at,
                        deleted_at,
                        sample_content_file_id,
                        video_content_file_id,
                        special_category
                    )
                    SELECT
                        ?,
                        content_files.id,
                        journey_background_images.image_file_id,
                        journey_background_images.blurred_image_file_id,
                        journey_background_images.darkened_image_file_id,
                        instructors.id,
                        ?,
                        ?,
                        journey_subcategories.id,
                        interactive_prompts.id,
                        ?,
                        NULL,
                        NULL,
                        NULL,
                        ?
                    FROM content_files, journey_background_images, instructors, journey_subcategories, interactive_prompts
                    WHERE
                        content_files.uid = ?
                        AND journey_background_images.uid = ?
                        AND instructors.uid = ?
                        AND journey_subcategories.internal_name = ?
                        AND interactive_prompts.uid = ?
                    """,
                    (
                        journey_uid,
                        generated.meta.title,
                        generated.meta.description,
                        now,
                        "ai",
                        content.uid,
                        background_uid,
                        instructor_uid,
                        generated.meta.category,
                        interactive_prompt_uid,
                    ),
                ),
            )
        )

        if response[0].rows_affected is None or response[0].rows_affected < 1:
            raise Exception(
                f"Failed to create interactive prompt {interactive_prompt_uid}: {content.uid=}, {generated=}"
            )

        if response[1].rows_affected is None or response[1].rows_affected < 1:
            raise Exception(
                f"Failed to create journey {journey_uid}: {content.uid=}, {generated=}"
            )

        created_journey_at = time.time()
        logging.info(
            f"Finished storing journey in {created_journey_at - instructor_created_at:.3f} seconds, queueing corresponding jobs..."
        )

        jobs = await itgs.jobs()
        await jobs.enqueue("runners.refresh_journey_emotions", journey_uid=journey_uid)
        await jobs.enqueue(
            "runners.process_journey_video_sample", journey_uid=journey_uid
        )
        await jobs.enqueue("runners.process_journey_video", journey_uid=journey_uid)

        finished_at = time.time()
        logging.info(
            f"Finished queueing jobs in {finished_at - created_journey_at:.3f} seconds, total time {finished_at - processed_at:.3f} seconds"
        )
        await slack.send_ops_message(
            f"Generated {generated.meta.title} (a {generated.meta.category} class for {generated.meta.emotion}) ({journey_uid}) in {finished_at - started_at:.3f} seconds on {socket.gethostname()}"
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.create_ai_journey")

    asyncio.run(main())
