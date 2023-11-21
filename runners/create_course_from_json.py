"""Creates a course from a JSON file, which we're doing while there's no admin to 
do it manually.
"""
from typing import List
from images import process_image
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from pydantic import BaseModel, Field
from temp_files import temp_file
from runners.process_journey_background_image import (
    RESOLUTIONS as FULLSCREEN_RESOLUTIONS,
    make_standard_targets,
)
import secrets
import time

category = JobCategory.HIGH_RESOURCE_COST


class CourseData(BaseModel):
    slug: str = Field(description="the slug for the course")
    revenue_cat_entitlement: str = Field(
        description="the entitlement identifier on revenue cat"
    )
    title: str = Field(description="the title of the course")
    title_short: str = Field(description="the short title of the course")
    description: str = Field(description="the description of the course")
    background_image_file_s3_key: str = Field(
        description="the s3 key where the background image can be found"
    )
    circle_image_file_s3_key: str = Field(
        description="the s3 key where the circle image can be found"
    )
    journeys: List[str] = Field(
        description="the uids of the journeys in the course, in order"
    )


async def execute(itgs: Itgs, gd: GracefulDeath, *, course_data: str):
    """Creates a course from the given course data, which must be a json-serialized
    representation of CourseData

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        course_data (str): the course to create
    """
    course = CourseData.model_validate_json(course_data)

    files = await itgs.files()
    with temp_file() as temp_path:
        with open(temp_path, "wb") as f:
            await files.download(
                f,
                bucket=files.default_bucket,
                key=course.background_image_file_s3_key,
                sync=True,
            )

        resolutions = FULLSCREEN_RESOLUTIONS
        if (1440, 720) not in resolutions:
            resolutions = resolutions + [(1440, 720)]

        targets = make_standard_targets(resolutions)
        background = await process_image(
            temp_path,
            targets,
            itgs=itgs,
            gd=gd,
            max_width=16384,
            max_height=16384,
            max_area=8192 * 8192,
            max_file_size=1024 * 1024 * 512,
            name_hint="course_background_image",
        )

    with temp_file() as temp_path:
        with open(temp_path, "wb") as f:
            await files.download(
                f,
                bucket=files.default_bucket,
                key=course.circle_image_file_s3_key,
                sync=True,
            )

        resolutions = [(189, 189), (378, 378), (567, 567)]
        targets = make_standard_targets(resolutions)
        circle = await process_image(
            temp_path,
            targets,
            itgs=itgs,
            gd=gd,
            max_width=16384,
            max_height=16384,
            max_area=8192 * 8192,
            max_file_size=1024 * 1024 * 512,
            name_hint="course_circle_image",
        )

    uid = f"oseh_c_{secrets.token_urlsafe(16)}"
    now = time.time()
    conn = await itgs.conn()
    cursor = conn.cursor()

    response = await cursor.execute(
        """
        INSERT INTO courses (
            uid, slug, revenue_cat_entitlement, title, title_short, description,
            background_image_file_id, circle_image_file_id, created_at
        )
        SELECT
            ?, ?, ?, ?, ?, ?, background_image_files.id, circle_image_files.id, ?
        FROM image_files AS background_image_files, image_files AS circle_image_files
        WHERE
            background_image_files.uid = ?
            AND circle_image_files.uid = ?
        """,
        (
            uid,
            course.slug,
            course.revenue_cat_entitlement,
            course.title,
            course.title_short,
            course.description,
            now,
            background.uid,
            circle.uid,
        ),
    )
    if response.rows_affected is None or response.rows_affected != 1:
        raise Exception("failed to insert course")

    for i, journey_uid in enumerate(course.journeys):
        course_journey_uid = f"oseh_cj_{secrets.token_urlsafe(16)}"
        response = await cursor.execute(
            """
            INSERT INTO course_journeys (
                uid, course_id, journey_id, priority
            )
            SELECT
                ?, courses.id, journeys.id, ?
            FROM courses, journeys
            WHERE
                courses.uid = ?
                AND journeys.uid = ?
            """,
            (course_journey_uid, i, uid, journey_uid),
        )
        if response.rows_affected is None or response.rows_affected != 1:
            raise Exception(f"failed to insert course journey {i=}, {journey_uid=}")

        response = await cursor.execute(
            "UPDATE journeys SET deleted_at = NULL WHERE uid = ? AND deleted_at IS NOT NULL",
            (journey_uid,),
        )
        if response.rows_affected is not None and response.rows_affected > 0:
            logging.info(
                f"restored journey {journey_uid=} as it is now used in the course"
            )


if __name__ == "__main__":
    import asyncio

    async def main():
        course_data: str = input("course_data: ")
        CourseData.model_validate_json(course_data)
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.create_course_from_json", course_data=course_data
            )
        print("enqueued job")

    asyncio.run(main())
