"""Generates the course share image for a course."""

import io
import os
from typing import Any, Optional, cast

import aiofiles
from PIL import Image, ImageFont
import numpy as np
from content import hash_filelike
from error_middleware import handle_warning
from images import (
    ImageFile,
    SvgAspectRatio,
    _crops_to_pil_box,
    determine_crop,
    get_svg_natural_aspect_ratio,
)
from itgs import Itgs
from graceful_death import GracefulDeath

from jobs import JobCategory
from lib.progressutils.async_progress_tracking_bytes_io import (
    AsyncProgressTrackingReadableBytesIO,
    AsyncProgressTrackingWritableBytesIO,
)
from lib.progressutils.progress_helper import ProgressHelper
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    success_or_failure_reporter,
)
from runners.process_journey_share_image import (
    _estimate_share_image_settings,
    _try_make_share_image_with_fonts,
)
from shareables.generate_share_image.exceptions import ShareImageBounceError
from shareables.generate_share_image.main import run_pipeline
from temp_files import temp_dir, temp_file
import asyncio
import logging.config
from multiprocessing.synchronize import Event as MPEvent
import cairosvg

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    course_uid: str,
    job_progress_uid: Optional[str] = None,
):
    """
    Generates the course share image for a given course. If this results in a
    different share image than the current one for that course, the new one is
    attached and the old one is deleted if no longer in use. If this results in
    the same image but some of the targets are missing, only the missing targets
    are generated. Further, if this results in the same image and all the
    targets already exist, this exits quickly.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        course_uid (str): the uid of the course to generate the share image for
        job_progress_uid (str, None): If specified, used to send progress updates
    """

    async with success_or_failure_reporter(itgs, job_progress_uid=job_progress_uid):
        try:
            with temp_dir() as dirpath:
                await run_pipeline(
                    itgs,
                    gd,
                    CourseShareImageGenerator(course_uid=course_uid, tmp_dir=dirpath),
                    name_hint="course_share_image",
                    job_progress_uid=job_progress_uid,
                )
        except ShareImageBounceError:
            logging.info(f"{__name__} bouncing")
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.process_course_share_image",
                course_uid=course_uid,
                job_progress_uid=job_progress_uid,
            )
            raise BouncedException()


class CourseShareImageGenerator:
    """Implements ShareImageGenerator"""

    def __init__(self, course_uid: str, tmp_dir: str) -> None:
        self.course_uid = course_uid
        """The UID of the course whose share image is being generated"""

        self.tmp_dir = tmp_dir
        """The path to the directory used to store temporary files"""

        self.background_path = os.path.join(self.tmp_dir, "darkened_image")
        """Where we are storing the darkened background image"""

        self.hero_image_file_original_sha512: Optional[str] = None
        """The sha512 hash of the background image we are using"""

        self.hero_image_file_key: Optional[str] = None
        """The key of the background image in the S3 bucket"""

        self.hero_image_file_size: Optional[int] = None
        """The size of the background image in bytes"""

        self.course_title: Optional[str] = None
        """The title of the course"""

        self.instructor_name: Optional[str] = None
        """The name of the instructor for the course"""

        self.brandmark_natural_aspect_ratio: Optional[SvgAspectRatio] = None
        """The natural aspect ratio of the oseh brandmark"""

        self.background_image: Optional[Image.Image] = None
        """The background image, loaded into memory in this process"""

        self.open_sans_regular_raw: Optional[bytes] = None
        """The font file for Open Sans, 400 font weight, loaded into memory in this process"""

    async def get_configuration(
        self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper
    ) -> Any:
        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        response = await cursor.execute(
            """
SELECT
    courses.title,
    instructors.name,
    image_files.original_sha512,
    s3_files.key,
    s3_files.file_size
FROM courses, instructors, image_files, s3_files
WHERE
    courses.uid = ?
    AND instructors.id = courses.instructor_id
    AND image_files.id = courses.hero_image_file_id
    AND s3_files.id = image_files.original_s3_file_id
            """,
            (self.course_uid,),
        )

        if not response.results:
            await handle_warning(
                f"{__name__}:course_not_found",
                f"There is no course with `{self.course_uid=}` or it is missing the original "
                "hero image file required for producing the share image",
            )
            raise Exception("course not found")

        self.course_title = cast(str, response.results[0][0])
        self.instructor_name = cast(str, response.results[0][1])
        self.hero_image_file_original_sha512 = cast(str, response.results[0][2])
        self.hero_image_file_key = cast(str, response.results[0][3])
        self.hero_image_file_size = cast(int, response.results[0][4])

        return {
            "name": f"{__name__}.{self.__class__.__name__}",
            "version": "1.0.0",
            "title": self.course_title,
            "instructor_name": self.instructor_name,
            "hero_image_file_original_sha512": self.hero_image_file_original_sha512,
        }

    async def prepare(self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper):
        assert self.hero_image_file_key is not None

        files = await itgs.files()
        async with aiofiles.open(
            self.background_path, "wb"
        ) as raw_f, AsyncProgressTrackingWritableBytesIO(
            itgs,
            job_progress_uid=prog.job_progress_uid,
            expected_file_size=self.hero_image_file_size,
            delegate=raw_f,
            message="downloading background image",
        ) as f:
            await files.download(
                f,
                bucket=files.default_bucket,
                key=self.hero_image_file_key,
                sync=False,
            )

        if gd.received_term_signal:
            return

        async with aiofiles.open(
            self.background_path, "rb"
        ) as raw_f, AsyncProgressTrackingReadableBytesIO(
            itgs,
            job_progress_uid=prog.job_progress_uid,
            expected_file_size=self.hero_image_file_size,
            delegate=raw_f,
            message="verifying background image",
        ) as f:
            actual_sha512 = await hash_filelike(f)

        assert (
            actual_sha512 == self.hero_image_file_original_sha512
        ), f"{actual_sha512=} {self.hero_image_file_original_sha512=}"

        self.brandmark_natural_aspect_ratio = await get_svg_natural_aspect_ratio(
            "assets/Oseh_Brandmark_White.svg"
        )
        assert self.brandmark_natural_aspect_ratio is not None

    def prepare_process(self, exit_requested: MPEvent):
        self.background_image = Image.open(self.background_path)
        self.background_image.load()

        if exit_requested.is_set():
            return

        with open("assets/OpenSans-Regular.ttf", "rb") as f:
            self.open_sans_regular_raw = f.read()

    def generate(self, width: int, height: int, exit_requested: MPEvent) -> Image.Image:
        assert self.open_sans_regular_raw is not None
        assert self.background_image is not None
        assert self.brandmark_natural_aspect_ratio is not None
        assert self.course_title is not None
        assert self.instructor_name is not None

        bknd = self.background_image
        crops = determine_crop((bknd.width, bknd.height), (width, height), (0.5, 0.5))

        if any(crop > 0 for crop in crops):
            bknd = bknd.crop(_crops_to_pil_box(*crops, bknd.width, bknd.height))
            if exit_requested.is_set():
                raise ShareImageBounceError()

        if bknd.width != width or bknd.height != height:
            bknd = bknd.resize((width, height), Image.LANCZOS)
            if exit_requested.is_set():
                raise ShareImageBounceError()

        if bknd.mode != "RGB":
            bknd = bknd.convert("RGB")

        arr = np.asarray(bknd)
        if not arr.flags["C_CONTIGUOUS"]:
            arr = np.ascontiguousarray(arr)

        height = arr.shape[0]
        width = arr.shape[1]

        bknd_arr = np.copy(arr, order="C")

        for y in range(int(height * 0.67), height):
            y_perc = y / height
            darken_strength = max((y_perc - 0.67) / 0.33, 0)
            np.multiply(
                arr[y, :, :],
                1 - darken_strength,
                out=bknd_arr[y, :, :],
                casting="unsafe",
            )

        new_bknd = Image.fromarray(bknd_arr)
        bknd.close()
        bknd = new_bknd
        del bknd_arr
        del new_bknd
        del arr

        settings = _estimate_share_image_settings(width, height)
        while True:
            if exit_requested.is_set():
                raise ShareImageBounceError()

            title_font = ImageFont.truetype(
                font=io.BytesIO(self.open_sans_regular_raw),
                size=settings.title_fontsize,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.Layout.BASIC,
            )
            instructor_font = ImageFont.truetype(
                font=io.BytesIO(self.open_sans_regular_raw),
                size=settings.instructor_fontsize,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.Layout.BASIC,
            )
            brandmark: Optional[Image.Image] = None
            if settings.brandmark_size is not None:
                with temp_file(".png") as brandmark_filepath:
                    cairosvg.svg2png(
                        url="assets/Oseh_Brandmark_White.svg",
                        write_to=brandmark_filepath,
                        output_width=settings.brandmark_size,
                        output_height=round(
                            (
                                settings.brandmark_size
                                / self.brandmark_natural_aspect_ratio.ratio
                            )
                        ),
                        background_color="transparent",
                    )
                    brandmark = Image.open(brandmark_filepath, formats=["png"])
                    brandmark.load()

            settings, result = _try_make_share_image_with_fonts(
                self.course_title,
                self.instructor_name,
                bknd,
                title_font,
                instructor_font,
                brandmark,
                settings,
            )
            if result is not None:
                return result

    async def finish(
        self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper, img: ImageFile
    ):
        conn = await itgs.conn()
        cursor = conn.cursor("strong")

        response = await cursor.executeunified3(
            (
                (
                    """
                    SELECT
                        image_files.uid
                    FROM courses, image_files
                    WHERE
                        courses.uid = ?
                        AND courses.share_image_file_id = image_files.id
                    """,
                    (self.course_uid,),
                ),
                (
                    """
                    UPDATE courses
                    SET share_image_file_id = image_files.id
                    FROM image_files
                    WHERE
                        courses.uid = ?
                        AND image_files.uid = ?
                    """,
                    (self.course_uid, img.uid),
                ),
            )
        )

        old_image_file_uid = (
            None if not response[0].results else cast(str, response[0].results[0][0])
        )
        updated = (
            response[1].rows_affected is not None and response[1].rows_affected > 0
        )

        if updated:
            assert response[1].rows_affected == 1, response

        if not updated:
            await handle_warning(
                f"{__name__}:raced",
                f"course {self.course_uid} was deleted while share image was generated",
            )
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.delete_image_file",
                uid=img.uid,
            )
            return

        if old_image_file_uid is not None and old_image_file_uid != img.uid:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.delete_image_file",
                uid=old_image_file_uid,
            )

        if old_image_file_uid != img.uid:
            logging.info(
                f"{__name__} assigned new share image {img.uid} to course {self.course_uid}"
            )


if __name__ == "__main__":

    async def main():
        course_uid = input("Course UID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.process_course_share_image", course_uid=course_uid
            )

    asyncio.run(main())
