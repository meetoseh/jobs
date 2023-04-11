"""Updates the vip_chat_request_image_uid in redis to match a local file"""
from images import ImageTarget
from itgs import Itgs
from graceful_death import GracefulDeath
from .process_journey_background_image import get_jpg_settings, get_webp_settings
from images import process_image, ImageTarget, ProcessImageAbortedException
import logging
import os
from jobs import JobCategory


category = JobCategory.HIGH_RESOURCE_COST

RESOLUTIONS = [(189, 189), (378, 378), (567, 567)]

TARGETS = [
    *(
        ImageTarget(
            required=True,
            width=w,
            height=h,
            format="jpeg",
            quality_settings=get_jpg_settings(w, h),
        )
        for w, h in RESOLUTIONS
    ),
    *(
        ImageTarget(
            required=False,
            width=w,
            height=h,
            format="webp",
            quality_settings=get_webp_settings(w, h),
        )
        for w, h in RESOLUTIONS
    ),
]


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Updates the vip_chat_request_image_uid in redis to match the file located
    at tmp/vip_chat_request_image.jpg

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    path = os.path.join("tmp", "vip_chat_request_image.jpg")
    try:
        image = await process_image(
            path,
            TARGETS,
            itgs=itgs,
            gd=gd,
            max_width=16384,
            max_height=16384,
            max_area=8192 * 8192,
            max_file_size=1024 * 1024 * 512,
            name_hint="journey_background_image",
        )
    except ProcessImageAbortedException:
        logging.warning("vip_chat_request_image.png processing was aborted; retry")
        return

    redis = await itgs.redis()
    await redis.set("vip_chat_request_image_uid", image.uid)

    logging.info(
        f"vip_chat_request_image.png processing complete; assigned uid {image.uid}"
    )


if __name__ == "__main__":
    import asyncio
    import yaml
    import logging.config

    async def main():
        if not os.path.exists(os.path.join("tmp", "vip_chat_request_image.jpg")):
            print("vip_chat_request_image.jpg not found; aborting")
            return

        gd = GracefulDeath()
        with open("logging.yaml") as f:
            logging_config = yaml.safe_load(f)

        logging.config.dictConfig(logging_config)
        async with Itgs() as itgs:
            await execute(itgs, gd)

    asyncio.run(main())
