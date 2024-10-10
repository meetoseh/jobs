"""Ensures that the share image for the given journey is up-to-date"""

from multiprocessing.synchronize import Event as MPEvent
import os
import cairosvg
from typing import Any, Optional, Tuple, cast
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
import logging
from jobs import JobCategory
from lib.progressutils.async_progress_tracking_bytes_io import (
    AsyncProgressTrackingReadableBytesIO,
    AsyncProgressTrackingWritableBytesIO,
)
from lib.progressutils.progress_helper import ProgressHelper
from shareables.generate_share_image.exceptions import ShareImageBounceError
from shareables.generate_share_image.main import run_pipeline
from temp_files import temp_dir, temp_file
from dataclasses import dataclass
import aiofiles
from PIL import Image, ImageFont, ImageDraw
import io
import asyncio

category = JobCategory.HIGH_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath, *, journey_uid: str):
    """Ensures that the share image for the given journey is up-to-date by
    creating it fresh from the journeys current state, updating the journeys
    image, and deleting the old image (if its now unused).

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_uid (str): the uid of the journey to update the share image for
    """

    try:
        with temp_dir() as dirpath:
            await run_pipeline(
                itgs,
                gd,
                JourneyShareImageGenerator(journey_uid=journey_uid, tmp_dir=dirpath),
                name_hint="journey_share_image",
            )
    except ShareImageBounceError:
        logging.info(f"{__name__} bouncing")
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_journey_share_image", journey_uid=journey_uid
        )


class JourneyShareImageGenerator:
    """Implements ShareImageGenerator"""

    def __init__(self, journey_uid: str, tmp_dir: str) -> None:
        self.journey_uid = journey_uid
        """The UID of the journey whose share image is being generated"""

        self.tmp_dir = tmp_dir
        """The path to the directory used to store temporary files"""

        self.background_path = os.path.join(self.tmp_dir, "darkened_image")
        """Where we are storing the darkened background image"""

        self.darkened_image_file_original_sha512: Optional[str] = None
        """The sha512 hash of the background image we are using"""

        self.darkened_image_file_key: Optional[str] = None
        """The key of the background image in the S3 bucket"""

        self.darkened_image_file_size: Optional[int] = None
        """The size of the background image in bytes"""

        self.journey_title: Optional[str] = None
        """The title of the journey"""

        self.instructor_name: Optional[str] = None
        """The name of the instructor for the journey"""

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
    darkened_image_files.original_sha512,
    darkened_s3_files.key,
    darkened_s3_files.file_size,
    journeys.title,
    instructors.name
FROM 
    journeys, 
    image_files AS darkened_image_files,
    s3_files AS darkened_s3_files,
    instructors
WHERE
    journeys.uid = ?
    AND journeys.darkened_background_image_file_id = darkened_image_files.id
    AND darkened_image_files.original_s3_file_id = darkened_s3_files.id
    AND journeys.instructor_id = instructors.id
            """,
            (self.journey_uid,),
        )

        if not response.results:
            await handle_warning(
                f"{__name__}:journey_not_found",
                f"There is no journey with `{self.journey_uid=}` or it is missing the original "
                "image files required for producing the share image",
            )
            raise Exception("journey not found")

        self.darkened_image_file_original_sha512 = cast(str, response.results[0][0])
        self.darkened_image_file_key = cast(str, response.results[0][1])
        self.darkened_image_file_size = cast(int, response.results[0][2])
        self.journey_title = cast(str, response.results[0][3])
        self.instructor_name = cast(str, response.results[0][4])

        return {
            "name": f"{__name__}.{self.__class__.__name__}",
            "version": "1.0.2",
            "title": self.journey_title,
            "instructor_name": self.instructor_name,
            "darkened_image_file_original_sha512": self.darkened_image_file_original_sha512,
        }

    async def prepare(self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper):
        assert self.darkened_image_file_key is not None

        files = await itgs.files()
        async with aiofiles.open(
            self.background_path, "wb"
        ) as raw_f, AsyncProgressTrackingWritableBytesIO(
            itgs,
            job_progress_uid=prog.job_progress_uid,
            expected_file_size=self.darkened_image_file_size,
            delegate=raw_f,
            message="downloading background image",
        ) as f:
            await files.download(
                f,
                bucket=files.default_bucket,
                key=self.darkened_image_file_key,
                sync=False,
            )

        if gd.received_term_signal:
            return

        async with aiofiles.open(
            self.background_path, "rb"
        ) as raw_f, AsyncProgressTrackingReadableBytesIO(
            itgs,
            job_progress_uid=prog.job_progress_uid,
            expected_file_size=self.darkened_image_file_size,
            delegate=raw_f,
            message="verifying background image",
        ) as f:
            actual_sha512 = await hash_filelike(f)

        assert (
            actual_sha512 == self.darkened_image_file_original_sha512
        ), f"{actual_sha512=} {self.darkened_image_file_original_sha512=}"

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
        assert self.journey_title is not None
        assert self.instructor_name is not None

        bknd = self.background_image
        crops = determine_crop((bknd.width, bknd.height), (width, height), (0.5, 0.5))

        if any(crop > 0 for crop in crops):
            bknd = bknd.crop(_crops_to_pil_box(*crops, bknd.width, bknd.height))
            if exit_requested.is_set():
                raise ShareImageBounceError()

        if bknd.width != width or bknd.height != height:
            bknd = bknd.resize((width, height), Image.Resampling.LANCZOS)
            if exit_requested.is_set():
                raise ShareImageBounceError()

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
                self.journey_title,
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
                    FROM journeys, image_files
                    WHERE
                        journeys.uid = ?
                        AND journeys.share_image_file_id = image_files.id
                    """,
                    (self.journey_uid,),
                ),
                (
                    """
                    UPDATE journeys
                    SET share_image_file_id = image_files.id
                    FROM image_files
                    WHERE
                        journeys.uid = ?
                        AND image_files.uid = ?
                    """,
                    (self.journey_uid, img.uid),
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
                f"journey {self.journey_uid} was deleted while share image was generated",
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
                f"{__name__} assigned new share image {img.uid} to journey {self.journey_uid}"
            )


@dataclass
class _ShareImageSettings:
    title_fontsize: int
    instructor_fontsize: int
    brandmark_size: Optional[int]


def _estimate_share_image_settings(width: int, height: int) -> _ShareImageSettings:
    if width == 1200 and height == 630:
        return _ShareImageSettings(
            title_fontsize=60,
            instructor_fontsize=32,
            brandmark_size=60,
        )

    if width == 600 and height == 315:
        return _ShareImageSettings(
            title_fontsize=36,
            instructor_fontsize=22,
            brandmark_size=32,
        )

    if width == 600 and height == 600:
        return _ShareImageSettings(
            title_fontsize=36,
            instructor_fontsize=22,
            brandmark_size=32,
        )

    if width == 400 and height == 300:
        return _ShareImageSettings(
            title_fontsize=24,
            instructor_fontsize=12,
            brandmark_size=32,
        )

    if width == 80 and height == 315:
        return _ShareImageSettings(
            title_fontsize=9, instructor_fontsize=5, brandmark_size=None
        )

    return _ShareImageSettings(
        title_fontsize=round((60 / 630) * height),
        instructor_fontsize=round((32 / 630) * height),
        brandmark_size=(round((60 / 630) * height) if width > 300 else None),
    )


def _try_make_share_image_with_fonts(
    title: str,
    instructor_name: str,
    bknd: Image.Image,
    title_font: ImageFont.FreeTypeFont,
    instructor_font: ImageFont.FreeTypeFont,
    brandmark: Optional[Image.Image],
    settings: _ShareImageSettings,
) -> Tuple[_ShareImageSettings, Optional[Image.Image]]:
    img = Image.new("RGBA", bknd.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    brandmark_width = brandmark.width if brandmark is not None else 0

    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_required_width = (title_bbox[2] - title_bbox[0]) + brandmark_width

    if title_required_width > bknd.width * 0.9:
        if brandmark is not None:
            return (
                _ShareImageSettings(
                    title_fontsize=settings.title_fontsize,
                    instructor_fontsize=settings.instructor_fontsize,
                    brandmark_size=None,
                ),
                None,
            )

        desired_width = bknd.width * 0.9
        scale_factor = desired_width / title_required_width
        return (
            _ShareImageSettings(
                title_fontsize=int(settings.title_fontsize * scale_factor),
                instructor_fontsize=settings.instructor_fontsize,
                brandmark_size=None,
            ),
            None,
        )

    instructor_name_bbox = draw.textbbox((0, 0), instructor_name, font=instructor_font)

    instructor_line_width = instructor_name_bbox[2] - instructor_name_bbox[0]
    instructor_line_required_width = instructor_line_width + brandmark_width

    if instructor_line_required_width > bknd.width * 0.9:
        if brandmark is not None:
            return (
                _ShareImageSettings(
                    title_fontsize=settings.title_fontsize,
                    instructor_fontsize=settings.instructor_fontsize,
                    brandmark_size=None,
                ),
                None,
            )

        actual_width = instructor_line_width
        desired_width = bknd.width * 0.9
        scale_factor = desired_width / actual_width
        new_instructor_fontsize = int(settings.instructor_fontsize * scale_factor)

        return (
            _ShareImageSettings(
                title_fontsize=settings.title_fontsize,
                instructor_fontsize=new_instructor_fontsize,
                brandmark_size=None,
            ),
            None,
        )

    title_height = title_bbox[3] - title_bbox[1]
    spacer = round(settings.title_fontsize * (24 / 60))

    total_height = (
        title_height + spacer + (instructor_name_bbox[3] - instructor_name_bbox[1])
    )

    if total_height >= bknd.height * 0.9:
        actual_height = total_height
        desired_height = bknd.height * 0.9
        scale_factor = desired_height / actual_height
        return (
            _ShareImageSettings(
                title_fontsize=int(settings.title_fontsize * scale_factor),
                instructor_fontsize=int(settings.instructor_fontsize * scale_factor),
                brandmark_size=(
                    int(settings.brandmark_size * scale_factor)
                    if settings.brandmark_size is not None
                    else None
                ),
            ),
            None,
        )

    title_real_y = round((bknd.height - total_height) - (20 / 315) * bknd.height)
    instructor_row_y = title_real_y + title_height + spacer

    title_real_x = round((25 / 600) * bknd.width)
    instructor_real_x = round((25 / 600) * bknd.width)

    img.paste(bknd, (0, 0))
    draw.text(
        (title_real_x - title_bbox[0], title_real_y - title_bbox[1]),
        title,
        font=title_font,
        fill=(255, 255, 255, 255),
    )
    draw.text(
        (
            instructor_real_x - instructor_name_bbox[0],
            instructor_row_y - instructor_name_bbox[1],
        ),
        instructor_name,
        font=instructor_font,
        fill=(255, 255, 255, 255),
    )

    if brandmark is not None:
        # Keep the bottom at the same spot as the bottom of the instructor text
        instructor_name_bottom_y = instructor_row_y + (
            instructor_name_bbox[3] - instructor_name_bbox[1]
        )
        brandmark_real_y = instructor_name_bottom_y - brandmark.height
        brandmark_real_x = bknd.width - brandmark.width - round((25 / 600) * bknd.width)
        img.paste(brandmark, (brandmark_real_x, brandmark_real_y), brandmark)

    img = img.convert("RGB")
    return settings, img


if __name__ == "__main__":

    async def main():
        journey_uid = input("Journey UID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.process_journey_share_image", journey_uid=journey_uid
            )

    asyncio.run(main())
