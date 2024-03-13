"""Ensures that the share image for the given journey is up-to-date"""

import base64
import json
import multiprocessing
import multiprocessing.pool
import os
import secrets
import time
import cairosvg
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, cast
from content import hash_content
from error_middleware import handle_warning
from images import (
    ImageTarget,
    LocalImageFileExport,
    _crops_to_pil_box,
    determine_crop,
    get_svg_natural_aspect_ratio,
    upload_many_image_targets,
)
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.thumbhash import image_to_thumb_hash
from temp_files import temp_dir, temp_file
from dataclasses import dataclass
import aiofiles
from PIL import Image, ImageFont, ImageDraw
import io
import asyncio

category = JobCategory.HIGH_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""

RESOLUTIONS = list(
    dict.fromkeys(
        [
            # 1x
            (600, 315),
            # 2x
            (1200, 630),
            # legacy sharers
            (400, 300),
            # linkedin and other thumbnails
            (80, 150),
            # imessage
            (600, 600),
        ]
    )
)


def _share_image_jpeg_targets(w: int, h: int) -> List[ImageTarget]:
    if w * h < 300 * 300:
        return []

    return [
        ImageTarget(
            required=True,
            width=w,
            height=h,
            format="jpeg",
            quality_settings={"quality": 95, "optimize": True, "progressive": True},
        )
    ]


def _share_image_png_targets(w: int, h: int) -> List[ImageTarget]:
    if w * h >= 300 * 300:
        return []

    return [
        ImageTarget(
            required=True,
            width=w,
            height=h,
            format="png",
            quality_settings={"optimize": True},
        )
    ]


def make_share_image_targets(
    resolutions: Sequence[Tuple[int, int]]
) -> List[ImageTarget]:
    result = []
    for w, h in resolutions:
        result.extend(_share_image_jpeg_targets(w, h))
        result.extend(_share_image_png_targets(w, h))
    return result


TARGETS = make_share_image_targets(RESOLUTIONS)


async def execute(itgs: Itgs, gd: GracefulDeath, *, journey_uid: str):
    """Ensures that the share image for the given journey is up-to-date by
    creating it fresh from the journeys current state, updating the journeys
    image, and deleting the old image (if its now unused).

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        journey_uid (str): the uid of the journey to update the share image for
    """

    async def bounce():
        logging.info(f"{__name__} bouncing")
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.process_journey_share_image", journey_uid=journey_uid
        )

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            darkened_image_files.original_sha512,
            darkened_s3_files.key,
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
        (journey_uid,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}:journey_not_found",
            f"There is no journey with `{journey_uid=}` or it is missing the original "
            "image files required for producing the share image",
        )
        return

    darkened_image_file_original_sha512 = cast(str, response.results[0][0])
    darkened_image_file_key = cast(str, response.results[0][1])
    journey_title = cast(str, response.results[0][2])
    instructor_name = cast(str, response.results[0][3])

    journey = _Journey(title=journey_title, instructor_name=instructor_name)
    brandmark_natural_aspect_ratio = await get_svg_natural_aspect_ratio(
        "assets/Oseh_Brandmark_White.svg"
    )
    assert brandmark_natural_aspect_ratio is not None

    with temp_dir() as dirpath:
        files = await itgs.files()

        async def download_file(filename: str, key: str, sha512: str) -> bool:
            filepath = os.path.join(dirpath, filename)
            async with aiofiles.open(filepath, "wb") as f:
                await files.download(
                    f, bucket=files.default_bucket, key=key, sync=False
                )

            real_hash = await hash_content(filepath)
            if real_hash != sha512:
                await handle_warning(
                    f"{__name__}:hash_mismatch",
                    f"Hash mismatch for {key} (expected {sha512}, got {real_hash})",
                )
                return False
            return True

        background_filename = "darkened_image"

        integrity_matched = await download_file(
            background_filename,
            darkened_image_file_key,
            darkened_image_file_original_sha512,
        )
        if not integrity_matched:
            return

        remaining: List[ImageTarget] = list(TARGETS)
        running_targets: Dict[int, _PartialLocalImageFileExport] = dict()
        running: Set[asyncio.Future] = set()
        finished: List[LocalImageFileExport] = []

        nprocesses = min(multiprocessing.cpu_count() // 2, len(TARGETS))
        max_queued_jobs = nprocesses * 2
        logging.debug(
            f"{__name__} - all files have been downloaded; producing {len(remaining)} targets "
            f"in a pool of {nprocesses} processes with {max_queued_jobs} max queued jobs"
        )
        with multiprocessing.Pool(processes=nprocesses) as pool:
            while remaining or running:
                if gd.received_term_signal:
                    logging.warning(
                        "interrupted while producing share image targets; bouncing"
                    )
                    for task in running:
                        task.cancel()
                    return await bounce()

                while remaining and len(running) < max_queued_jobs:
                    target = remaining.pop()
                    result = len(TARGETS) - len(remaining)

                    output_filepath = os.path.join(
                        dirpath, f"image_{result}.{target.format}"
                    )
                    running_targets[result] = _PartialLocalImageFileExport(
                        uid=f"oseh_ife_{secrets.token_urlsafe(16)}",
                        width=target.width,
                        height=target.height,
                        filepath=output_filepath,
                        crop=(0, 0, 0, 0),
                        format=target.format,
                        quality_settings=target.quality_settings,
                    )
                    running.add(
                        make_share_image_from_pool(
                            pool,
                            journey,
                            os.path.join(dirpath, background_filename),
                            running_targets[result].filepath,
                            target,
                            result,
                            brandmark_natural_aspect_ratio.ratio,
                        )
                    )
                done, running = await asyncio.wait(
                    running, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    result = cast(_MakeShareImageResult, await task)
                    task_was_working_on = running_targets.pop(result.iden)
                    logging.debug(f"{__name__} finished {task_was_working_on.filepath}")
                    finished.append(
                        LocalImageFileExport(
                            uid=task_was_working_on.uid,
                            width=task_was_working_on.width,
                            height=task_was_working_on.height,
                            filepath=task_was_working_on.filepath,
                            crop=task_was_working_on.crop,
                            format=task_was_working_on.format,
                            quality_settings=task_was_working_on.quality_settings,
                            thumbhash=result.thumbhash,
                            file_size=result.file_size,
                        )
                    )

        logging.debug(f"{__name__} - all targets have been produced; uploading")

        if gd.received_term_signal:
            logging.info(f"{__name__} not uploading (term signal received, bouncing)")
            return await bounce()

        uploaded = await upload_many_image_targets(
            finished, itgs=itgs, gd=gd, name_hint="journey_share_image"
        )
        logging.debug(f"{__name__} - all targets have been uploaded; inserting")

        exports_sql_writer = io.StringIO()
        exports_sql_writer.write(
            "WITH batch(uid, s3_file_uid, width, height, format, quality_settings, thumbhash) AS ("
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        for _ in range(1, len(uploaded)):
            exports_sql_writer.write(", (?, ?, ?, ?, ?, ?, ?)")
        exports_sql_writer.write(
            ") INSERT INTO image_file_exports ("
            " uid,"
            " image_file_id,"
            " s3_file_id,"
            " width,"
            " height,"
            " left_cut_px,"
            " right_cut_px,"
            " top_cut_px,"
            " bottom_cut_px,"
            " format,"
            " quality_settings,"
            " thumbhash,"
            " created_at"
            ") SELECT"
            " batch.uid,"
            " image_files.id,"
            " s3_files.id,"
            " batch.width,"
            " batch.height,"
            " 0, 0, 0, 0,"
            " batch.format,"
            " batch.quality_settings,"
            " batch.thumbhash,"
            " ? "
            "FROM batch, image_files, s3_files "
            "WHERE"
            " image_files.uid = ?"
            " AND s3_files.uid = batch.s3_file_uid"
        )
        exports_sql = exports_sql_writer.getvalue()

        now = time.time()
        image_file_uid = f"oseh_if_{secrets.token_urlsafe(16)}"

        exports_qargs = []
        for export in uploaded:
            exports_qargs.extend(
                [
                    export.local_image_file_export.uid,
                    export.s3_file.uid,
                    export.local_image_file_export.width,
                    export.local_image_file_export.height,
                    export.local_image_file_export.format,
                    json.dumps(export.local_image_file_export.quality_settings),
                    export.local_image_file_export.thumbhash,
                ]
            )
        exports_qargs.extend([now, image_file_uid])

        response = await cursor.executeunified3(
            (
                (
                    """
                    INSERT INTO image_files (
                        uid, name, original_sha512, original_width, original_height, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        image_file_uid,
                        f"share_image_for_{journey_uid}",
                        f"garbage--{secrets.token_urlsafe(64)}",
                        1200,
                        630,
                        now,
                    ),
                ),
                (exports_sql, exports_qargs),
                (
                    """
                    SELECT
                        image_files.uid
                    FROM journeys, image_files
                    WHERE
                        journeys.uid = ?
                        AND journeys.share_image_file_id = image_files.id
                    """,
                    (journey_uid,),
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
                    (journey_uid, image_file_uid),
                ),
            )
        )

        if response[2].results:
            old_share_image_file_uid = cast(Optional[str], response[2].results[0][0])
            if old_share_image_file_uid is not None:
                jobs = await itgs.jobs()
                await jobs.enqueue(
                    "runners.delete_image_file", uid=old_share_image_file_uid
                )

        if response[3].rows_affected != 1:
            await handle_warning(
                f"{__name__}:failed_to_set_image",
                f"Failed to set share image for `{journey_uid=}`; `{response[3].rows_affected=}`",
            )

            jobs = await itgs.jobs()
            await jobs.enqueue("runners.delete_image_file", uid=image_file_uid)


@dataclass
class _Journey:
    title: str
    instructor_name: str


@dataclass
class _ShareImageSettings:
    title_fontsize: int
    instructor_fontsize: int
    brandmark_size: Optional[int]


@dataclass
class _MakeShareImageResult:
    file_size: int
    thumbhash: str
    iden: int


@dataclass
class _PartialLocalImageFileExport:
    # omits filesize / thumbhash which aren't available until the export is ready
    uid: str
    width: int
    height: int
    filepath: str
    crop: Tuple[int, int, int, int]  # top, right, bottom, left
    format: str
    quality_settings: Dict[str, Any]


def _estimate_share_image_settings(target: ImageTarget) -> _ShareImageSettings:
    if target.width == 1200 and target.height == 630:
        return _ShareImageSettings(
            title_fontsize=60,
            instructor_fontsize=32,
            brandmark_size=60,
        )

    if target.width == 600 and target.height == 315:
        return _ShareImageSettings(
            title_fontsize=36,
            instructor_fontsize=22,
            brandmark_size=32,
        )

    if target.width == 600 and target.height == 600:
        return _ShareImageSettings(
            title_fontsize=36,
            instructor_fontsize=22,
            brandmark_size=32,
        )

    if target.width == 400 and target.height == 300:
        return _ShareImageSettings(
            title_fontsize=24,
            instructor_fontsize=12,
            brandmark_size=32,
        )

    if target.width == 80 and target.height == 315:
        return _ShareImageSettings(
            title_fontsize=9, instructor_fontsize=5, brandmark_size=None
        )

    return _ShareImageSettings(
        title_fontsize=round((60 / 630) * target.height),
        instructor_fontsize=round((32 / 630) * target.height),
        brandmark_size=(
            round((60 / 630) * target.height) if target.width > 300 else None
        ),
    )


def make_share_image(
    journey: _Journey,
    background_filepath: str,
    output_filepath: str,
    target: ImageTarget,
    iden: int,
    brandmark_natural_aspect_ratio: float,
) -> _MakeShareImageResult:
    """Unlike with most images where we simply rescale to size, it looks
    better if we scale the font size and then allow for subpixel rendering,
    rather than rendering the text larger and then scaling it down.

    Returns the result parameter; useful when working with pools
    """
    img = Image.open(background_filepath)

    crops = determine_crop(
        (img.width, img.height), (target.width, target.height), (0.5, 0.5)
    )

    if any(crop > 0 for crop in crops):
        img = img.crop(_crops_to_pil_box(*crops, img.width, img.height))

    if img.width != target.width or img.height != target.height:
        img = img.resize((target.width, target.height), Image.LANCZOS)

    img = img.convert("RGBA")

    with open("assets/OpenSans-Regular.ttf", "rb") as f:
        open_sans_medium_raw = f.read()

    settings = _estimate_share_image_settings(target)
    success = False
    while not success:
        title_font = ImageFont.truetype(
            font=io.BytesIO(open_sans_medium_raw),
            size=settings.title_fontsize,
            index=0,
            encoding="unic",
            layout_engine=ImageFont.Layout.BASIC,
        )
        instructor_font = ImageFont.truetype(
            font=io.BytesIO(open_sans_medium_raw),
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
                        (settings.brandmark_size / brandmark_natural_aspect_ratio)
                    ),
                    background_color="transparent",
                )
                brandmark = Image.open(brandmark_filepath, formats=["png"])
                brandmark.load()

        settings, success = _try_make_share_image_with_fonts(
            journey,
            img,
            title_font,
            instructor_font,
            brandmark,
            output_filepath,
            target,
            settings,
        )

    filesize = os.path.getsize(output_filepath)
    thumbhash_bytes_as_list = image_to_thumb_hash(output_filepath)
    thumbhash_bytes = bytes(thumbhash_bytes_as_list)
    thumbhash_b64url = base64.urlsafe_b64encode(thumbhash_bytes).decode("ascii")

    return _MakeShareImageResult(
        file_size=filesize,
        thumbhash=thumbhash_b64url,
        iden=iden,
    )


def make_share_image_from_pool(
    pool: multiprocessing.pool.Pool,
    journey: _Journey,
    background_filepath: str,
    output_filepath: str,
    target: ImageTarget,
    iden: int,
    brandmark_natural_aspect_ratio: float,
) -> asyncio.Future:
    """Same as `make_share_image`, except executes in the given pool rather than
    synchronously.

    This utilizes that `make_share_image` returns the iden parameter, which
    means its easier to determine which results are done when many targets are
    being processed in parallel.
    """
    loop = asyncio.get_event_loop()
    future = loop.create_future()

    def _on_done(result):
        loop.call_soon_threadsafe(future.set_result, result)

    def _on_error(error):
        loop.call_soon_threadsafe(future.set_exception, error)

    pool.apply_async(
        make_share_image,
        args=(
            journey,
            background_filepath,
            output_filepath,
            target,
            iden,
            brandmark_natural_aspect_ratio,
        ),
        callback=_on_done,
        error_callback=_on_error,
    )
    return future


def _try_make_share_image_with_fonts(
    journey: _Journey,
    bknd: Image.Image,
    title_font: ImageFont.FreeTypeFont,
    instructor_font: ImageFont.FreeTypeFont,
    brandmark: Optional[Image.Image],
    output_filepath: str,
    target: ImageTarget,
    settings: _ShareImageSettings,
) -> Tuple[_ShareImageSettings, bool]:
    img = Image.new("RGBA", bknd.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    brandmark_width = brandmark.width if brandmark is not None else 0

    title_bbox = draw.textbbox((0, 0), journey.title, font=title_font)
    title_required_width = (title_bbox[2] - title_bbox[0]) + brandmark_width

    if title_required_width > bknd.width * 0.9:
        if brandmark is not None:
            return (
                _ShareImageSettings(
                    title_fontsize=settings.title_fontsize,
                    instructor_fontsize=settings.instructor_fontsize,
                    brandmark_size=None,
                ),
                False,
            )

        desired_width = bknd.width * 0.9
        scale_factor = desired_width / title_required_width
        return (
            _ShareImageSettings(
                title_fontsize=int(settings.title_fontsize * scale_factor),
                instructor_fontsize=settings.instructor_fontsize,
                brandmark_size=None,
            ),
            False,
        )

    instructor_name_bbox = draw.textbbox(
        (0, 0), journey.instructor_name, font=instructor_font
    )

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
                False,
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
            False,
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
            False,
        )

    title_real_y = round((bknd.height - total_height) - (20 / 315) * bknd.height)
    instructor_row_y = title_real_y + title_height + spacer

    title_real_x = round((25 / 600) * bknd.width)
    instructor_real_x = round((25 / 600) * bknd.width)

    img.paste(bknd, (0, 0))
    draw.text(
        (title_real_x - title_bbox[0], title_real_y - title_bbox[1]),
        journey.title,
        font=title_font,
        fill=(255, 255, 255, 255),
    )
    draw.text(
        (
            instructor_real_x - instructor_name_bbox[0],
            instructor_row_y - instructor_name_bbox[1],
        ),
        journey.instructor_name,
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
    img.save(output_filepath, format=target.format, **target.quality_settings)
    return settings, True


if __name__ == "__main__":

    async def main():
        journey_uid = input("Journey UID: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.process_journey_share_image", journey_uid=journey_uid
            )

    asyncio.run(main())