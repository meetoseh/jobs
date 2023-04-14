import json
import os
import secrets
import socket
from typing import Dict, List, Optional
from content import hash_content_sync
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
from dataclasses import dataclass

from jobs import JobCategory
from lib.image_files.get_biggest_export import (
    ImageFileExportRef,
    download_export_ref as download_image_export_ref,
    get_biggest_export_ref,
)
import io
import hashlib
from log_helpers import format_bps
from temp_files import temp_dir, temp_file
import textwrap
import zipfile
import logging
import time

category = JobCategory.HIGH_RESOURCE_COST


EXPORTER_VERSION = "1.0.6"


async def execute(itgs: Itgs, gd: GracefulDeath, *, course_uid: str):
    """Produces a hash of the given course as described in
    backend/docs/db/course_exports.md. If the hash differs from
    the latest export of the course (or there is no latest export
    for the course), this will generate a new export.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        course_uid (str): The uid of the course to potentially produce an export for
    """
    course = await get_course(itgs, course_uid)
    if course is None:
        logging.warning(
            f"Cannot create an export for {course_uid=} since it does not exist"
        )
        return

    course_hash = hash_course(course)
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    response = await cursor.execute(
        """
        SELECT
            course_exports.inputs_hash
        FROM course_exports, courses
        WHERE
            course_exports.course_id = courses.id
            AND courses.uid = ?
        ORDER BY course_exports.created_at DESC
        LIMIT 1
        """,
        (course_uid,),
    )
    old_inputs_hash: Optional[str] = (
        None if not response.results else response.results[0][0]
    )
    if old_inputs_hash == course_hash:
        logging.info(f"Course {course_uid=} has not changed since last export")
        return

    logging.info(
        f"Course {course_uid=} has changed since last export, generating export..."
    )
    slack = await itgs.slack()
    await slack.send_ops_message(
        f"{socket.gethostname()} Starting course export for {course.title} ({course_uid=})"
    )
    with temp_file() as zip_path:
        started_at = time.time()
        await generate_export(itgs, course, zip_path)
        export_file_size = os.path.getsize(zip_path)
        export_sha512 = hash_content_sync(zip_path)
        export_generated_at = time.time()
        logging.info(
            f"Finished generating {export_file_size} byte export in {export_generated_at - started_at:.1f} seconds ({format_bps(export_generated_at - started_at, export_file_size)})"
        )

        course_export_uid = f"oseh_ce_{secrets.token_urlsafe(16)}"
        s3_file_uid = f"oseh_s3f_{secrets.token_urlsafe(16)}"
        s3_file_key = f"s3_files/courses/{course_uid}/{course_export_uid}/{secrets.token_urlsafe(8)}.zip"

        files = await itgs.files()
        redis = await itgs.redis()
        protect_for = 60 * 60
        purgatory_key = json.dumps(
            {"key": s3_file_key, "bucket": files.default_bucket}, sort_keys=True
        )
        await redis.zadd(
            "files:purgatory", mapping={purgatory_key: time.time() + protect_for}
        )
        with open(zip_path, "rb") as f:
            await files.upload(
                f, bucket=files.default_bucket, key=s3_file_key, sync=True
            )

        upload_completed_at = time.time()
        logging.info(
            f"Finished uploading export in {upload_completed_at - export_generated_at:.1f} seconds"
        )
        response = await cursor.executemany3(
            (
                (
                    """
                    INSERT INTO s3_files (
                        uid, key, file_size, content_type, created_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        s3_file_uid,
                        s3_file_key,
                        export_file_size,
                        "application/zip",
                        upload_completed_at,
                    ),
                ),
                (
                    """
                    INSERT INTO course_exports (
                        uid, course_id, inputs_hash, s3_file_id, output_sha512, created_at
                    )
                    SELECT
                        ?, courses.id, ?, s3_files.id, ?, ?
                    FROM courses, s3_files
                    WHERE
                        courses.uid = ? AND s3_files.uid = ?
                    """,
                    (
                        course_export_uid,
                        course_hash,
                        export_sha512,
                        upload_completed_at,
                        course_uid,
                        s3_file_uid,
                    ),
                ),
            )
        )
        if response[0].rows_affected is None or response[0].rows_affected < 1:
            await handle_contextless_error(
                f"failed to insert s3_file {s3_file_uid=}, {s3_file_key=}, {export_file_size=}, {upload_completed_at=}"
            )
            return
        if response[1].rows_affected is None or response[1].rows_affected < 1:
            await handle_contextless_error(
                f"failed to insert course_export {course_export_uid=}, {course_hash=}, {export_sha512=}, {upload_completed_at=}, {course_uid=}, {s3_file_uid=}"
            )
            return
        await redis.zrem("files:purgatory", purgatory_key)
        finished_at = time.time()

        await slack.send_ops_message(
            f"{socket.gethostname()} Completed course export for {course.title} ({course_uid}) in {finished_at - started_at:.1f} seconds: {course_export_uid}"
        )


@dataclass
class InstructorForCourseExport:
    name: str


@dataclass
class ContentFileExportRef:
    content_file_uid: str
    content_file_export_uid: str
    content_file_export_part_uid: str


@dataclass
class JourneyForCourseExport:
    uid: str
    instructor: InstructorForCourseExport
    title: str
    description: str
    subcategory_external_name: str
    video_content_file: ContentFileExportRef


@dataclass
class CourseForExport:
    title: str
    title_short: str
    description: str
    background: ImageFileExportRef
    journeys: List[JourneyForCourseExport]
    """Must be sorted in the same order as the journeys in the course"""


def hash_course(course: CourseForExport) -> str:
    """Produces a hash of the given course as described in
    backend/docs/db/course_exports.md.

    Args:
        course (CourseForExport): The course to hash

    Returns:
        str: The hash of the given course
    """
    f = io.StringIO()
    write_stable_course_string(course, f)
    return hashlib.sha512(f.getvalue().encode("utf-8")).hexdigest()


def write_stable_course_string(course: CourseForExport, f: io.StringIO) -> None:
    """Writes a stable string representation of the given course to the given string.
    This representation depends on the current exporter version, and is hashed to
    produce the inputs hash.
    """
    print(EXPORTER_VERSION, file=f)
    print(course.title, file=f)
    print(course.title_short, file=f)
    print(course.description, file=f)
    print(course.background.image_file_uid, file=f)
    print(course.background.base_url, file=f)
    for journey in course.journeys:
        print(journey.uid, file=f)
        print(journey.instructor.name, file=f)
        print(journey.title, file=f)
        print(journey.description, file=f)
        print(journey.subcategory_external_name, file=f)
        print(journey.video_content_file.content_file_uid, file=f)
        print(journey.video_content_file.content_file_export_uid, file=f)
        print(journey.video_content_file.content_file_export_part_uid, file=f)


@dataclass
class JourneyRelpaths:
    """Implementation detail of generate_export"""

    video: str
    """The relative path to where the video is stored"""
    description: str
    """The relative path to where we stored the txt file with the title, instructor, and description"""


async def download_video(
    itgs: Itgs, video: ContentFileExportRef, f: io.BytesIO
) -> None:
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            s3_files.key
        FROM s3_files, content_file_export_parts
        WHERE
            s3_files.id = content_file_export_parts.s3_file_id
            AND content_file_export_parts.uid = ?
        """,
        (video.content_file_export_part_uid,),
    )

    if not response.results:
        raise ValueError(
            f"Could not find video with uid {video.content_file_export_part_uid}"
        )

    key = response.results[0][0]
    files = await itgs.files()
    await files.download(f, bucket=files.default_bucket, key=key, sync=True)


async def generate_export(itgs: Itgs, course: CourseForExport, out_file: str) -> None:
    """Generates a new export for the given course and writes it to the given file."""
    # Paths are always relative to the temp folder
    used_relpaths = set()

    def create_relpath(relfolder: str, desired_name: str, extension: str) -> str:
        """Creates a relative path from the given base url and desired name.
        This will ensure that the path does not conflict with any other paths
        created with this function.

        Args:
            relfolder (str): The folder the relpath will be inside, e.g., 'images'
            desired_name (str): The desired name of the file, without the extension, e.g.,
                the course title. Spaces and special characters will be replaced with
                underscores.
            extension (str): The extension of the file, e.g., '.png'
        """
        cleaned_name = "".join([a if a.isalnum() else "_" for a in desired_name])
        relpath = os.path.join(relfolder, cleaned_name + extension)
        if relpath in used_relpaths:
            i = 2
            while True:
                relpath = os.path.join(relfolder, f"{cleaned_name}_{i}" + extension)
                if relpath not in used_relpaths:
                    break
                i += 1
        used_relpaths.add(relpath)
        return relpath

    with temp_dir() as folder:
        os.makedirs(os.path.join(folder, "images"))
        os.makedirs(os.path.join(folder, "videos"))
        os.makedirs(os.path.join(folder, "classes"))

        course_background_relpath = create_relpath(
            "images", course.title, course.background.format
        )
        with open(os.path.join(folder, course_background_relpath), "wb") as f:
            await download_image_export_ref(itgs, course.background, f, sync=True)

        journey_uid_to_relpaths: Dict[str, JourneyRelpaths] = dict()
        for journey in course.journeys:
            video_relpath = create_relpath("videos", journey.title, ".mp4")
            with open(os.path.join(folder, video_relpath), "wb") as f:
                await download_video(itgs, journey.video_content_file, f)

            description_relpath = create_relpath("classes", journey.title, ".txt")
            with open(os.path.join(folder, description_relpath), "w") as f:
                print(journey.title, file=f)
                print(f"by {journey.instructor.name}", file=f)
                print(file=f)
                print("\n".join(textwrap.wrap(journey.description, width=80)), file=f)

            journey_uid_to_relpaths[journey.uid] = JourneyRelpaths(
                video=video_relpath,
                description=description_relpath,
            )

        meta = create_relpath("", "meta", ".txt")
        with open(os.path.join(folder, meta), "w") as f:
            print(f"exporter version: {EXPORTER_VERSION}", file=f)

        index_file = create_relpath("", "index", ".html")
        with open(os.path.join(folder, index_file), "w") as f:
            print("<!doctype html>", file=f)
            print('<html lang="en">', file=f)
            print("<head>", file=f)
            print('<meta charset="utf-8">', file=f)
            print(f"<title>{course.title}</title>", file=f)
            print(
                '<meta name="viewport" content="width=device-width, initial-scale=1" />',
                file=f,
            )
            print("<style>", file=f)
            with open(os.path.join("assets", "css", "course_export.css"), "r") as css:
                print(css.read(), file=f)
            print("</style>", file=f)
            print("</head>", file=f)
            print("<body>", file=f)
            print(f"<h1>{course.title}</h1>", file=f)
            print(f"<p>{course.description}</p>", file=f)
            print(
                f'<div class="background-container"><img src="{course_background_relpath}" /></div>',
                file=f,
            )
            print("<h2>Classes</h2>", file=f)
            for journey in course.journeys:
                print(f'<div class="course-class">', file=f)
                print(f'<div class="course-class-title">{journey.title}</div>', file=f)
                print(
                    f'<div class="course-class-instructor">by {journey.instructor.name}</div>',
                    file=f,
                )
                print(
                    f'<video controls src="{journey_uid_to_relpaths[journey.uid].video}"></video>',
                    file=f,
                )
                print("</div>", file=f)
            print("</body>", file=f)
            print("</html>", file=f)

        with zipfile.ZipFile(
            out_file, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zipf:
            for root, _, files in os.walk(folder):
                for file in files:
                    zipf.write(
                        os.path.join(root, file),
                        os.path.join(root, file)[len(folder) + 1 :],
                    )


async def get_course(itgs: Itgs, uid: str) -> Optional[CourseForExport]:
    """Fetches the course with the given uid from the database.

    Args:
        itgs (Itgs): The integrations to (re)use
        uid (str): The uid of the course to fetch

    Returns:
        CourseForExport or None: The course, or None if it does not exist
    """

    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
        SELECT
            courses.title,
            courses.title_short,
            courses.description,
            background_images.uid
        FROM courses
        JOIN image_files AS background_images ON background_images.id = courses.background_image_file_id
        WHERE courses.uid = ?
        """,
        (uid,),
    )
    if not response.results:
        return None

    course = CourseForExport(
        title=response.results[0][0],
        title_short=response.results[0][1],
        description=response.results[0][2],
        background=await get_biggest_export_ref(
            itgs, uid=response.results[0][3], max_width=1440, max_height=720
        ),
        journeys=[],
    )

    response = await cursor.execute(
        """
        SELECT 
            COUNT(*) 
        FROM courses, course_journeys, journeys
        WHERE
            courses.uid = ?
            AND course_journeys.course_id = courses.id
            AND journeys.id = course_journeys.journey_id
            AND journeys.deleted_at IS NULL
        """,
        (uid,),
    )
    num_journeys = response.results[0][0]
    if num_journeys <= 0:
        await handle_contextless_error(f"course {uid=} has no journeys")
        return None

    response = await cursor.execute(
        """
        SELECT
            journeys.uid,
            instructors.name,
            journeys.title,
            journeys.description,
            journey_subcategories.external_name,
            videos.uid,
            video_exports.uid,
            video_export_parts.uid
        FROM 
            courses, 
            course_journeys, 
            journeys, 
            journey_subcategories, 
            instructors, 
            content_files AS videos, 
            content_file_exports AS video_exports, 
            content_file_export_parts AS video_export_parts
        WHERE
            courses.uid = ?
            AND course_journeys.course_id = courses.id
            AND course_journeys.journey_id = journeys.id
            AND journeys.deleted_at IS NULL
            AND journey_subcategories.id = journeys.journey_subcategory_id
            AND instructors.id = journeys.instructor_id
            AND videos.id = journeys.video_content_file_id
            AND video_exports.content_file_id = videos.id
            AND video_export_parts.content_file_export_id = video_exports.id
            AND video_exports.format = 'mp4'
            AND NOT EXISTS (
                SELECT 1 FROM content_file_exports AS other_exports
                WHERE
                    other_exports.content_file_id = videos.id
                    AND other_exports.format = 'mp4'
                    AND (
                        other_exports.bandwidth > video_exports.bandwidth
                        OR (
                            other_exports.bandwidth = video_exports.bandwidth
                            AND other_exports.uid > video_exports.uid
                        )
                    )
            )
        ORDER BY course_journeys.priority ASC
        """,
        (uid,),
    )
    if len(response.results or []) != num_journeys:
        await handle_contextless_error(
            f"course {uid=} has {len(response.results or [])} exportable journeys, but {num_journeys} were expected"
        )
        return None

    for row in response.results or []:
        journey = JourneyForCourseExport(
            uid=row[0],
            instructor=InstructorForCourseExport(name=row[1]),
            title=row[2],
            description=row[3],
            subcategory_external_name=row[4],
            video_content_file=ContentFileExportRef(
                content_file_uid=row[5],
                content_file_export_uid=row[6],
                content_file_export_part_uid=row[7],
            ),
        )
        course.journeys.append(journey)

    return course


if __name__ == "__main__":
    import asyncio

    async def main():
        course_uid = input("Course UID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.ensure_course_export", course_uid=course_uid)
        print("Job enqueued")

    asyncio.run(main())
