"""Called by frontend-web in order to generate server images which don't require
authentication
"""

from typing import List, Tuple
from images import process_image, ImageTarget, ProcessImageAbortedException
from itgs import Itgs
from graceful_death import GracefulDeath
from temp_files import temp_file
import aiofiles
import logging
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


def get_jpg_settings(width: int, height: int) -> dict:
    if width * height < 500 * 500:
        return {"quality": 95, "optimize": True, "progressive": True}

    if width * height < 750 * 750:
        return {"quality": 90, "optimize": True, "progressive": True}

    return {"quality": 85, "optimize": True, "progressive": True}


def get_png_settings(width: int, height: int):
    return {"optimize": True}


def get_webp_settings(width: int, height: int) -> dict:
    if width * height < 500 * 500:
        return {"lossless": True, "quality": 100, "method": 6}

    if width * height < 750 * 750:
        return {"lossless": False, "quality": 95, "method": 4}

    return {"lossless": False, "quality": 90, "method": 4}


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_uid: str,
    file_name: str,
    file_s3_key: str,
    file_resolutions: List[Tuple[int, int]],
    job_uid: str,
    transparency: bool = False,
    focal_point: Tuple[float, float] = (0.5, 0.5),
):
    """Generates an image file with the given uid using the original file located at the
    given key in s3, marking the resulting image as public. Sends a message to
    ps:job:{job_uid} when done. Picks reasonably high quality settings for each
    resolution, reducing quality for larger resolutions. Exports webp and jpeg.

    Note that this deletes the s3 file if it succeeds as it's reuploaded to the
    standard location.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_uid (str): The uid to use for the image file
        file_name (str): The name of the original file
        file_s3_key (str): The s3 key of the original file
        file_resolutions (List[Tuple[int, int]]): The resolutions to generate as a list
            of (width, height)
        job_uid (str): The uid to use to send a message to ps:job:{job_uid} when done
        transparency (bool, optional): Whether or not the image needs to support transparency.
        focal_point (Tuple[float, float], optional): The focal point of the image as a
            percentage of the width and height. Defaults to (0.5, 0.5). When cropping, we
            will attempt to keep this point in the center of the cropped image.
    """

    async def bounce():
        jobs = await itgs.jobs()
        await jobs.enqueue(
            "runners.generate_server_image",
            file_uid=file_uid,
            file_name=file_name,
            file_s3_key=file_s3_key,
            file_resolutions=file_resolutions,
            job_uid=job_uid,
        )

    targets: List[ImageTarget] = [
        *(
            (
                ImageTarget(
                    required=True,
                    width=w,
                    height=h,
                    format="jpeg",
                    quality_settings=get_jpg_settings(w, h),
                )
                for w, h in file_resolutions
            )
            if not transparency
            else (
                ImageTarget(
                    required=True,
                    width=w,
                    height=h,
                    format="png",
                    quality_settings=get_png_settings(w, h),
                )
                for w, h in file_resolutions
            )
        ),
        *(
            ImageTarget(
                required=False,
                width=w,
                height=h,
                format="webp",
                quality_settings=get_webp_settings(w, h),
            )
            for w, h in file_resolutions
        ),
    ]

    files = await itgs.files()
    with temp_file() as local_path:
        async with aiofiles.open(local_path, "wb") as f:
            await files.download(
                f, bucket=files.default_bucket, key=file_s3_key, sync=False
            )

        if gd.received_term_signal:
            await bounce()
            return

        try:
            image = await process_image(
                local_path,
                targets,
                itgs=itgs,
                gd=gd,
                max_width=16384,
                max_height=16384,
                max_area=8192 * 8192,
                max_file_size=1024 * 1024 * 512,
                name_hint=file_name,
                force_uid=file_uid,
                focal_point=focal_point,
            )
        except ProcessImageAbortedException:
            return await bounce()

    if image.uid != file_uid:
        slack = await itgs.slack()
        await slack.send_web_error_message(
            f"server image uid mismatch: {image.uid=} != {file_uid=}"
        )

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    await cursor.execute(
        "INSERT INTO static_public_images (image_file_id) "
        "SELECT id FROM image_files WHERE uid=? "
        "ON CONFLICT(image_file_id) DO NOTHING",
        (file_uid,),
    )

    await files.delete(bucket=files.default_bucket, key=file_s3_key)
    await cursor.execute(
        "DELETE FROM s3_files WHERE key=?",
        (file_s3_key,),
    )

    redis = await itgs.redis()
    await redis.publish(f"ps:job:{job_uid}", "done")

    logging.info(f"Generated server image {file_uid=} with {len(targets)} targets")
