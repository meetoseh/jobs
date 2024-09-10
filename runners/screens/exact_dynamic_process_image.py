"""Processes a raw image intended to be used for an image interstitial screen"""

import asyncio
import base64
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import io
import json
import math
import multiprocessing
import multiprocessing.pool
import os
import secrets
import time
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypedDict,
    Union,
    cast,
)
from content import hash_content
from file_uploads import StitchFileAbortedException, stitch_file_upload
from itgs import Itgs
from graceful_death import GracefulDeath
from images import (
    ImageFile,
    LocalImageFileExport,
    ProcessImageSanity,
    delete_s3_files,
    get_image_file,
    get_svg_natural_aspect_ratio,
    ProcessImageAbortedException,
    name_from_name_hint,
    upload_many_image_targets,
    upload_original,
)
from lib.progressutils.progress_helper import ProgressHelper
from lib.progressutils.success_or_failure_reporter import (
    BouncedException,
    CustomFailureReasonException,
    success_or_failure_reporter,
)
from lib.thumbhash import image_to_thumb_hash
from temp_files import temp_dir, temp_file
from jobs import JobCategory
from dataclasses import dataclass
from PIL import Image
import cairosvg

category = JobCategory.HIGH_RESOURCE_COST

SVG_UNITS = ["px", "pt", "pc", "cm", "mm", "in"]


class DynamicSize(TypedDict):
    width: int
    """The 1x logical width of the image in pixels"""

    height: int
    """The 1x logical height of the image in pixels"""


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: str,
    dynamic_size: DynamicSize,
):
    """Processes the s3 file upload with the given uid for the image which has
    a dynamic size. This will enforce that the uploaded image is either an SVG
    which is exactly the right aspect ratio, or a raster image which is exactly
    an integer multiple of the specified size and has an even width and height.

    This produces the image at 1x, 1.5x, 2x, 2.5x, 3x and the smallest of:
    - 1x
    - 200w, the natural height
    - 200h, the natural width

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        job_progress_uid (str): The uid of the job progress to update
        dynamic_size (DynamicSize): The dynamic size of the image
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.screens.exact_dynamic_process_image",
            file_upload_uid=file_upload_uid,
            uploaded_by_user_sub=uploaded_by_user_sub,
            job_progress_uid=job_progress_uid,
            dynamic_size=dynamic_size,
        )

    async with success_or_failure_reporter(itgs, job_progress_uid=job_progress_uid):
        with temp_file() as stitched_path:
            try:
                await stitch_file_upload(
                    file_upload_uid,
                    stitched_path,
                    itgs=itgs,
                    gd=gd,
                    job_progress_uid=job_progress_uid,
                )
            except StitchFileAbortedException:
                await bounce()
                raise BouncedException()

            try:
                await process_exact_dynamic_image(
                    itgs,
                    gd,
                    stitched_path=stitched_path,
                    uploaded_by_user_sub=uploaded_by_user_sub,
                    job_progress_uid=job_progress_uid,
                    dynamic_size=dynamic_size,
                    pedantic=True,
                )
            except ProcessImageAbortedException:
                await bounce()
                raise BouncedException()

        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.delete_file_upload", file_upload_uid=file_upload_uid
        )


async def process_exact_dynamic_image(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    stitched_path: str,
    uploaded_by_user_sub: Optional[str],
    job_progress_uid: Optional[str],
    dynamic_size: DynamicSize,
    pedantic: bool = False,
) -> None:
    """Processes the image at the given filepath via a dynamic input size. This
    never needs to crop, and the resizing always exactly maintains the aspect
    ratio of the original image (which produces restrictions on the original image).

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): used for signalling when to stop early
        stitched_path (str): the local path to the stitched file
        uploaded_by_user_sub (str, None): the sub of the user who uploaded the file
        job_progress_uid (str): the uid of the job progress to update
        dynamic_size (DynamicSize): The dynamic size of the image
    """
    sanity: ProcessImageSanity = {
        "max_width": 8192,
        "max_height": 8192,
        "max_area": 4096 * 8192,
        "max_file_size": 1024 * 1024 * 512,
    }

    targets = await get_targets(
        stitched_path,
        itgs=itgs,
        gd=gd,
        job_progress_uid=job_progress_uid,
        **sanity,
        dynamic_size=dynamic_size,
        pedantic=pedantic,
    )
    if targets is None:
        return

    uploaded_at = time.time()

    if targets.type == "raster_resize":
        image = await process_raster_image(
            targets,
            itgs=itgs,
            gd=gd,
            job_progress_uid=job_progress_uid,
            uploaded_at=uploaded_at,
        )
    else:
        image = await process_svg_image(
            targets,
            itgs=itgs,
            gd=gd,
            job_progress_uid=job_progress_uid,
            uploaded_at=uploaded_at,
        )

    conn = await itgs.conn()
    cursor = conn.cursor()

    cfi_uid = f"oseh_cfi_{secrets.token_urlsafe(16)}"
    list_slug = f"exact_dynamic@{dynamic_size['width']}x{dynamic_size['height']}"
    response = await cursor.executemany3(
        (
            (
                """
                INSERT INTO client_flow_images (
                    uid,
                    list_slug,
                    image_file_id,
                    original_s3_file_id,
                    original_sha512,
                    uploaded_by_user_id,
                    last_uploaded_at
                )
                SELECT
                    ?,
                    ?,
                    image_files.id,
                    image_files.original_s3_file_id,
                    image_files.original_sha512,
                    users.id,
                    ?
                FROM image_files
                LEFT OUTER JOIN users ON users.sub = ?
                WHERE
                    image_files.uid = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM client_flow_images
                        WHERE 
                            client_flow_images.image_file_id = image_files.id
                            AND client_flow_images.list_slug = ?
                    )
                """,
                (
                    cfi_uid,
                    list_slug,
                    uploaded_at,
                    uploaded_by_user_sub,
                    image.uid,
                    list_slug,
                ),
            ),
            (
                """
                UPDATE client_flow_images
                SET
                    last_uploaded_at = ?
                WHERE
                    image_file_id = (
                        SELECT image_files.id FROM image_files
                        WHERE image_files.uid = ?
                    )
                    AND list_slug = ?
                    AND uid <> ?
                """,
                (
                    uploaded_at,
                    image.uid,
                    list_slug,
                    cfi_uid,
                ),
            ),
        )
    )

    did_insert = response[0].rows_affected is not None and response[0].rows_affected > 0
    did_update = response[1].rows_affected is not None and response[1].rows_affected > 0

    assert did_insert or did_update, f"no insert or update? {response=}"
    assert not (did_insert and did_update), f"both insert and update? {response=}"


@dataclass
class Size:
    width: int
    height: int


@dataclass
class SvgRenderStrategy:
    type: Literal["svg"]

    svg_path: str
    """The local file path to the SVG file"""

    targets: List[Size]
    """The image targets to produce"""


@dataclass
class RasterRenderStrategy:
    type: Literal["raster_resize"]

    source_size: Size
    """The size of the source raster image"""

    raster_path: str
    """The local file path to the SVG file"""

    alpha: bool
    """True if alpha is required, False otherwise. The targets will have already
    been adjusted to only include options that support alpha if this is True."""

    targets: List[Size]
    """The image targets to produce"""


async def get_targets(
    local_filepath: str,
    /,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    job_progress_uid: Optional[str],
    dynamic_size: DynamicSize,
    max_width: int,
    max_height: int,
    max_area: int,
    max_file_size: int,
    pedantic: bool = False,
) -> Union[SvgRenderStrategy, RasterRenderStrategy]:
    """Determines the appropriate targets for the image at the given path being
    processed at the given size.

    Raises CustomFailureReasonException if the image doesn't match the sanity
    checks, or the image isn't appropriate for the dynamic size.
    """
    file_size = os.path.getsize(local_filepath)
    if file_size > max_file_size:
        raise CustomFailureReasonException(
            f"File too large ({file_size} > {max_file_size})"
        )

    svg_natural_aspect_ratio = await get_svg_natural_aspect_ratio(local_filepath)
    if svg_natural_aspect_ratio is not None:

        try:
            width_exact_str = svg_natural_aspect_ratio.width_exact
            for unit in SVG_UNITS:
                if width_exact_str.endswith(unit):
                    width_exact_str = width_exact_str[: -len(unit)]
            height_exact_str = svg_natural_aspect_ratio.height_exact
            for unit in SVG_UNITS:
                if height_exact_str.endswith(unit):
                    height_exact_str = height_exact_str[: -len(unit)]
            svg_width = Decimal(width_exact_str)
            svg_height = Decimal(height_exact_str)
            exact_svg_integer_width = int(svg_width)
            exact_svg_integer_height = int(svg_height)
            if svg_width != exact_svg_integer_width:
                raise CustomFailureReasonException(
                    f"SVG lists viewbox width as {svg_natural_aspect_ratio.width_exact}, which cannot be interpreted exactly as an integer"
                )
            if svg_height != exact_svg_integer_height:
                raise CustomFailureReasonException(
                    f"SVG lists viewbox height as {svg_natural_aspect_ratio.height_exact}, which cannot be interpreted exactly as an integer"
                )
        except (ValueError, InvalidOperation):
            raise CustomFailureReasonException(
                f"SVG lists viewbox width or height as {svg_natural_aspect_ratio.width_exact} or {svg_natural_aspect_ratio.height_exact}, which cannot be interpreted as a number"
            )

        expected_ratio = Fraction(dynamic_size["width"], dynamic_size["height"])
        actual_ratio = Fraction(exact_svg_integer_width, exact_svg_integer_height)
        if expected_ratio != actual_ratio:
            raise CustomFailureReasonException(
                f"SVG aspect ratio (width / height) is {actual_ratio}, but the dynamic size requires {expected_ratio}"
            )

        targets = get_svg_sizes(dynamic_size)
        return SvgRenderStrategy(
            type="svg",
            svg_path=local_filepath,
            targets=targets,
        )

    with Image.open(local_filepath) as img:
        width, height = img.size
        mode = img.mode

    if width > max_width:
        raise CustomFailureReasonException(
            f"Image too large ({width}px width > {max_width}px max width)"
        )

    if height > max_height:
        raise CustomFailureReasonException(
            f"Image too large ({height}px height > {max_height}px max height)"
        )

    if width * height > max_area:
        raise CustomFailureReasonException(
            f"Image too large ({width * height}px area > {max_area}px max area)"
        )

    if mode not in ("RGB", "RGBA"):
        raise CustomFailureReasonException(
            f"Image must be in RGB or RGBA mode, not {mode}"
        )

    requires_alpha = mode == "RGBA"
    targets = get_raster_sizes(
        dynamic_size, Size(width=width, height=height), pedantic=pedantic
    )
    return RasterRenderStrategy(
        type="raster_resize",
        raster_path=local_filepath,
        alpha=requires_alpha,
        targets=targets,
        source_size=Size(width=width, height=height),
    )


def get_raster_sizes(
    dynamic_size: DynamicSize, source_size: Size, *, pedantic: bool = False
) -> List[Size]:
    """Raises CustomFailureReasonException if the dynamic size can not be produced
    from a source image at the given size
    """
    # This could use get_svg_sizes, but the error messages would be worse

    result: List[Size] = []

    target_width = dynamic_size["width"]
    target_height = dynamic_size["height"]

    pixel_ratio = Fraction(1, 1)
    while pixel_ratio <= Fraction(3, 1):
        output_width = target_width * pixel_ratio
        output_height = target_height * pixel_ratio

        if pedantic and (
            output_width.denominator != 1 or output_height.denominator != 1
        ):
            raise CustomFailureReasonException(
                f"Cannot produce a target width of {target_width}px width by {target_height}px height, as at "
                f"a pixel ratio of {pixel_ratio}, this would be a fractional size ({output_width}px width by {output_height}px height)"
            )

        output_int_width = math.ceil(output_width)
        output_int_height = math.ceil(output_height)

        if output_int_width > source_size.width:
            raise CustomFailureReasonException(
                f"A source image of {source_size.width}px width cannot be used to produce a target dynamic "
                f"width of {target_width}px, because at a pixel ratio of {pixel_ratio}, we need a width of {output_width}px"
            )

        if output_int_height > source_size.height:
            raise CustomFailureReasonException(
                f"A source image of {source_size.height}px height cannot be used to produce a target dynamic "
                f"height of {target_height}px, because at a pixel ratio of {pixel_ratio}, we need a height of {output_height}px"
            )

        if pedantic:
            ratio_input_to_output_width = Fraction(source_size.width, output_int_width)
            ratio_input_to_output_height = Fraction(
                source_size.height, output_int_height
            )
            if ratio_input_to_output_width != ratio_input_to_output_height:
                raise CustomFailureReasonException(
                    f"Cannot produce a target width of {target_width}px width by {target_height}px height using "
                    f"a source image of {source_size.width}px width by {source_size.height}px height, as at "
                    f"a pixel ratio of {pixel_ratio}, the desired size {output_int_width}px width by {output_int_height}px height "
                    f"does not produce the same ratio for the width and height: {ratio_input_to_output_width} vs {ratio_input_to_output_height}"
                )

        result.append(
            Size(
                width=output_int_width,
                height=output_int_height,
            )
        )
        pixel_ratio += Fraction(1, 2)

    if preview_size := get_preview_size(dynamic_size):
        result.append(preview_size)

    return result


def get_svg_sizes(dynamic_size: DynamicSize) -> List[Size]:
    """Determines the appropriate sizes for the SVG image to be rendered at
    the given dynamic size. Assumes the appropriate aspect ratio for the SVG.

    Raises CustomFailureReasonException if the dynamic size cannot be used
    to exactly produce target sizes.
    """
    target_width = dynamic_size["width"]
    target_height = dynamic_size["height"]

    result: List[Size] = []
    pixel_ratio = Fraction(1, 1)
    while pixel_ratio <= Fraction(3, 1):
        output_width = target_width * pixel_ratio
        output_height = target_height * pixel_ratio

        if output_width.denominator != 1 or output_height.denominator != 1:
            raise CustomFailureReasonException(
                f"Cannot produce a target width of {target_width}px width by {target_height}px height, as at "
                f"a pixel ratio of {pixel_ratio}, this would be a fractional size ({output_width}px width by {output_height}px height)"
            )

        result.append(
            Size(
                width=int(output_width),
                height=int(output_height),
            )
        )
        pixel_ratio += Fraction(1, 2)

    if preview_size := get_preview_size(dynamic_size):
        result.append(preview_size)

    return result


def get_preview_size(dynamic_size: DynamicSize) -> Optional[Size]:
    """Gets the special preview size, if we need one"""
    target_width = dynamic_size["width"]
    target_height = dynamic_size["height"]

    if target_width <= 200 and target_height <= 200:
        return None

    if target_width > target_height:
        preview_width = 200
        preview_height = int(preview_width * Fraction(target_height, target_width))
    else:
        preview_height = 200
        preview_width = int(preview_height * Fraction(target_width, target_height))

    return Size(width=preview_width, height=preview_height)


@dataclass
class ExportInfo:
    width: int
    height: int
    format: str
    quality_parameters: Dict[str, Any]


def get_webp_export(source: Size) -> ExportInfo:
    if source.width * source.height < 500 * 500:
        return ExportInfo(
            width=source.width,
            height=source.height,
            format="webp",
            quality_parameters={"lossless": True, "quality": 100, "method": 6},
        )

    if source.width * source.height < 750 * 750:
        return ExportInfo(
            width=source.width,
            height=source.height,
            format="webp",
            quality_parameters={"lossless": False, "quality": 95, "method": 4},
        )

    return ExportInfo(
        width=source.width,
        height=source.height,
        format="webp",
        quality_parameters={"lossless": False, "quality": 90, "method": 4},
    )


def get_jpeg_export(source: Size) -> ExportInfo:
    if source.width * source.height < 500 * 500:
        return ExportInfo(
            width=source.width,
            height=source.height,
            format="jpeg",
            quality_parameters={"quality": 95, "optimize": True, "progressive": True},
        )

    if source.width * source.height < 750 * 750:
        return ExportInfo(
            width=source.width,
            height=source.height,
            format="jpeg",
            quality_parameters={"quality": 90, "optimize": True, "progressive": True},
        )

    return ExportInfo(
        width=source.width,
        height=source.height,
        format="jpeg",
        quality_parameters={"quality": 85, "optimize": True, "progressive": True},
    )


def get_png_export(source: Size) -> ExportInfo:
    return ExportInfo(
        width=source.width,
        height=source.height,
        format="png",
        quality_parameters={"optimize": True},
    )


def get_preferred_export(source: Size, /, *, alpha: bool) -> ExportInfo:
    return get_webp_export(source)


def get_fallback_export(source: Size, /, *, alpha: bool) -> ExportInfo:
    if alpha:
        return get_png_export(source)
    return get_jpeg_export(source)


@dataclass
class ExportJob:
    source: str
    output: str
    info: ExportInfo


async def process_image_with_processor(
    *,
    make_target_async: Callable[
        [ExportJob, int, multiprocessing.pool.Pool, asyncio.AbstractEventLoop],
        asyncio.Future,
    ],
    source_path: str,
    source_size: Size,
    alpha: bool,
    sizes: List[Size],
    itgs: Itgs,
    gd: GracefulDeath,
    job_progress_uid: Optional[str],
    uploaded_at: float,
) -> ImageFile:
    """Processes the raster image at the given path to produce the given targets.
    This exclusively resizes the input image
    """
    helper = ProgressHelper(itgs, job_progress_uid, log=True)

    simple_progress = asyncio.create_task(
        helper.push_progress("Hashing source image", indicator={"type": "spinner"})
    )
    sha512 = await hash_content(source_path)
    await simple_progress

    simple_progress = asyncio.create_task(
        helper.push_progress(
            "Determining missing exports", indicator={"type": "spinner"}
        )
    )
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    response = await cursor.execute(
        "SELECT uid FROM image_files WHERE original_sha512 = ?", (sha512,)
    )
    existing_uid = cast(str, response.results[0][0]) if response.results else None

    exports = [
        *(get_preferred_export(size, alpha=alpha) for size in sizes),
        *(get_fallback_export(size, alpha=alpha) for size in sizes),
    ]

    uid = cast(Optional[str], None)
    if existing_uid is not None:
        existing_image = await get_image_file(itgs, existing_uid)
        if existing_image is not None:
            export_keys = [
                f"{info.format}@{info.width}x{info.height}" for info in exports
            ]
            missing_keys = set(export_keys)
            for img in existing_image.exports:
                missing_keys.discard(f"{img.format}@{img.width}x{img.height}")

            exports = [
                exp for (exp, key) in zip(exports, export_keys) if key in missing_keys
            ]
            if not exports:
                await helper.push_progress("All exports already exist")
                return existing_image
            uid = existing_uid

    if uid is None:
        uid = f"oseh_if_{secrets.token_urlsafe(16)}"

    num_logical_cores = multiprocessing.cpu_count()
    num_physical_cores = max(
        (num_logical_cores if num_logical_cores is not None else 1) // 2, 1
    )
    num_workers = min(num_physical_cores, len(exports))
    total_images = len(exports)
    total_pixels = sum(size.width * size.height * 2 for size in sizes)

    done_images = 0
    done_pixels = 0

    loop = asyncio.get_running_loop()

    with multiprocessing.Pool(
        num_workers, maxtasksperchild=1
    ) as pool, temp_dir() as output_dir:
        jobs = [
            ExportJob(
                source=source_path,
                output=os.path.join(output_dir, f"{info_idx}.{info.format}"),
                info=info,
            )
            for info_idx, info in enumerate(exports)
        ]
        thumbhashes = ["" for _ in jobs]

        running: Set[asyncio.Future] = set()
        next_to_start_idx = 0
        last_progress_images = done_images
        progress_job = asyncio.create_task(
            inform_progress(
                helper,
                num_workers,
                done_images,
                total_images,
                done_pixels,
                total_pixels,
            )
        )

        while done_images < total_images:
            while next_to_start_idx < total_images and len(running) < num_workers:
                running.add(
                    make_target_async(
                        jobs[next_to_start_idx],
                        next_to_start_idx,
                        pool,
                        loop,
                    )
                )
                next_to_start_idx += 1

            done, pending = await asyncio.wait(
                running,
                return_when=asyncio.FIRST_COMPLETED,
            )

            try:
                for tsk in done:
                    finished_idx, finished_thumbhash = cast(Tuple[int, str], await tsk)
                    done_images += 1
                    done_pixels += (
                        exports[finished_idx].width * exports[finished_idx].height
                    )
                    thumbhashes[finished_idx] = finished_thumbhash
            except:
                for tsk in pending:
                    tsk.cancel()
                    try:
                        await asyncio.wait_for(tsk, timeout=5)
                    except:
                        pass

                raise

            if gd.received_term_signal:
                progress_job.cancel()
                for tsk in pending:
                    tsk.cancel()
                    try:
                        await asyncio.wait_for(tsk, timeout=5)
                    except:
                        pass
                raise ProcessImageAbortedException("Received term signal")

            running = pending

            if last_progress_images < done_images and progress_job.done():
                try:
                    await progress_job
                except:
                    pass
                progress_job = asyncio.create_task(
                    inform_progress(
                        helper,
                        num_workers,
                        done_images,
                        total_images,
                        done_pixels,
                        total_pixels,
                    )
                )

        await progress_job
        if last_progress_images < done_images:
            last_progress_images = done_images
            await inform_progress(
                helper,
                num_workers,
                done_images,
                total_images,
                done_pixels,
                total_pixels,
            )

        local_image_file_exports = [
            LocalImageFileExport(
                uid=f"oseh_ife_{secrets.token_urlsafe(16)}",
                width=job.info.width,
                height=job.info.height,
                filepath=job.output,
                crop=(0, 0, 0, 0),
                format=job.info.format,
                quality_settings=job.info.quality_parameters,
                thumbhash=thumbhash,
                file_size=os.path.getsize(job.output),
            )
            for thumbhash, job in zip(thumbhashes, jobs)
        ]

        uploaded = await upload_many_image_targets(
            local_image_file_exports,
            itgs=itgs,
            gd=gd,
            job_progress_uid=job_progress_uid,
            name_hint="exact_dynamic_process_image",
        )
        if gd.received_term_signal:
            await delete_s3_files([export.s3_file for export in uploaded], itgs=itgs)
            raise ProcessImageAbortedException("Received term signal")
        original = await upload_original(
            source_path, image_file_uid=uid, now=uploaded_at, itgs=itgs
        )
        if gd.received_term_signal:
            await delete_s3_files(
                [original, *[export.s3_file for export in uploaded]], itgs=itgs
            )
            raise ProcessImageAbortedException("Received term signal")

        exports_sql = io.StringIO()
        exports_sql.write(
            "WITH batch(uid, s3_file_uid, width, height, format, quality_settings, thumbhash, file_size) AS (VALUES "
        )
        exports_qargs = []

        for idx, export in enumerate(uploaded):
            if idx > 0:
                exports_sql.write(",")
            exports_sql.write("(?,?,?,?,?,?,?,?)")
            exports_qargs.extend(
                [
                    export.local_image_file_export.uid,
                    export.s3_file.uid,
                    export.local_image_file_export.width,
                    export.local_image_file_export.height,
                    export.local_image_file_export.format,
                    json.dumps(
                        export.local_image_file_export.quality_settings, sort_keys=True
                    ),
                    export.local_image_file_export.thumbhash,
                    export.local_image_file_export.file_size,
                ]
            )

        exports_sql.write(
            ") INSERT INTO image_file_exports ("
            " uid, image_file_id, s3_file_id, width, height, left_cut_px, right_cut_px,"
            " top_cut_px, bottom_cut_px, format, quality_settings, thumbhash, created_at"
            ") "
            "SELECT"
            " batch.uid,"
            " (SELECT id FROM image_files WHERE image_files.original_sha512=?),"
            " s3_files.id,"
            " batch.width,"
            " batch.height,"
            " 0, 0, 0, 0,"
            " batch.format,"
            " batch.quality_settings,"
            " batch.thumbhash,"
            " ? "
            "FROM batch, s3_files "
            "WHERE"
            " s3_files.uid = batch.s3_file_uid"
        )
        exports_qargs.extend([sha512, uploaded_at])

        await helper.push_progress(
            "Inserting image file exports",
            indicator={"type": "spinner"},
        )
        response = await cursor.executemany3(
            (
                (
                    """
INSERT INTO image_files (
    uid,
    name,
    original_s3_file_id,
    original_sha512,
    original_width,
    original_height,
    created_at
)
SELECT
    ?,
    ?,
    (SELECT s3_files.id FROM s3_files WHERE s3_files.uid = ?),
    ?,
    ?,
    ?,
    ?
WHERE
    NOT EXISTS (
        SELECT 1 FROM image_files AS if
        WHERE if.original_sha512 = ?
    )
                    """,
                    [
                        uid,
                        name_from_name_hint("exact_dynamic_process_image"),
                        original.uid,
                        sha512,
                        source_size.width,
                        source_size.height,
                        uploaded_at,
                        sha512,
                    ],
                ),
                (
                    exports_sql.getvalue(),
                    exports_qargs,
                ),
            ),
            raise_on_error=False,
        )

        if any(item.error for item in response.items):
            await delete_s3_files(
                [original, *[export.s3_file for export in uploaded]], itgs=itgs
            )
            raise CustomFailureReasonException(
                f"SQL error while inserting image file exports: {response.items}"
            )

        if response[1].rows_affected != len(uploaded):
            await delete_s3_files(
                [original, *[export.s3_file for export in uploaded]], itgs=itgs
            )
            raise CustomFailureReasonException(
                "Failed to insert all image file exports, so deleted all s3 artifacts"
            )

        if not response[0].rows_affected:
            await delete_s3_files([original], itgs=itgs)
            original = None

        return ImageFile(
            uid=uid,
            name=name_from_name_hint("exact_dynamic_process_image"),
            original_sha512=sha512,
            original_s3_file=original,
            original_width=source_size.width,
            original_height=source_size.height,
            created_at=uploaded_at,
            exports=[],
        )


def _make_raster_target_async(
    job: ExportJob,
    idx: int,
    pool: multiprocessing.pool.Pool,
    loop: asyncio.AbstractEventLoop,
    /,
):

    future = loop.create_future()

    def _on_done(thumbhash):
        loop.call_soon_threadsafe(future.set_result, (idx, thumbhash))

    def _on_error(error):
        loop.call_soon_threadsafe(future.set_exception, error)

    pool.apply_async(
        _make_raster_target_sync,
        args=(job,),
        callback=_on_done,
        error_callback=_on_error,
    )

    return future


def _make_raster_target_sync(
    job: ExportJob,
    /,
):
    with Image.open(job.source) as source:
        if source.width != job.info.width or source.height != job.info.height:
            source = source.resize((job.info.width, job.info.height), Image.LANCZOS)
        source.save(job.output, job.info.format, **job.info.quality_parameters)
    thumbhash_bytes_as_list = image_to_thumb_hash(job.output)
    thumbhash_bytes = bytes(thumbhash_bytes_as_list)
    thumbhash_b64url = base64.urlsafe_b64encode(thumbhash_bytes).decode("ascii")
    return thumbhash_b64url


def _make_svg_target_async(
    job: ExportJob,
    idx: int,
    pool: multiprocessing.pool.Pool,
    loop: asyncio.AbstractEventLoop,
    /,
):

    future = loop.create_future()

    def _on_done(thumbhash):
        loop.call_soon_threadsafe(future.set_result, (idx, thumbhash))

    def _on_error(error):
        loop.call_soon_threadsafe(future.set_exception, error)

    pool.apply_async(
        _make_svg_target_sync,
        args=(job,),
        callback=_on_done,
        error_callback=_on_error,
    )

    return future


def _make_svg_target_sync(
    job: ExportJob,
    /,
):
    with temp_file(".png") as temp_rasterized_path:
        cairosvg.svg2png(
            url=job.source,
            write_to=temp_rasterized_path,
            output_width=job.info.width,
            output_height=job.info.height,
        )

        with Image.open(temp_rasterized_path) as source:
            source.save(job.output, job.info.format, **job.info.quality_parameters)
    thumbhash_bytes_as_list = image_to_thumb_hash(job.output)
    thumbhash_bytes = bytes(thumbhash_bytes_as_list)
    thumbhash_b64url = base64.urlsafe_b64encode(thumbhash_bytes).decode("ascii")
    return thumbhash_b64url


async def inform_progress(
    helper: ProgressHelper,
    workers: int,
    done_images: int,
    total_images: int,
    done_pixels: int,
    total_pixels: int,
):
    await helper.push_progress(
        f"Processing images ({done_images}/{total_images} images done, {(done_pixels/total_pixels) * 100:.1f}% of pixels done, {workers} workers)",
        indicator={
            "type": "bar",
            "at": done_pixels,
            "of": total_pixels,
        },
    )


async def process_raster_image(
    strategy: RasterRenderStrategy,
    /,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    job_progress_uid: Optional[str],
    uploaded_at: float,
) -> ImageFile:
    return await process_image_with_processor(
        make_target_async=_make_raster_target_async,
        source_path=strategy.raster_path,
        source_size=strategy.source_size,
        alpha=strategy.alpha,
        sizes=strategy.targets,
        itgs=itgs,
        gd=gd,
        job_progress_uid=job_progress_uid,
        uploaded_at=uploaded_at,
    )


async def process_svg_image(
    strategy: SvgRenderStrategy,
    /,
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    job_progress_uid: Optional[str],
    uploaded_at: float,
) -> ImageFile:
    """Processes the SVG image at the given path to produce the given targets.
    This always uses the raw SVG to render each size.
    """
    return await process_image_with_processor(
        make_target_async=_make_svg_target_async,
        source_path=strategy.svg_path,
        source_size=Size(width=0, height=0),
        alpha=True,
        sizes=strategy.targets,
        itgs=itgs,
        gd=gd,
        job_progress_uid=job_progress_uid,
        uploaded_at=uploaded_at,
    )
