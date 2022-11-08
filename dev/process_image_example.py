import asyncio
from graceful_death import GracefulDeath
import dataclasses
from images import process_image
from runners.check_profile_picture import TARGETS
from itgs import Itgs
import json


async def main():
    gd = GracefulDeath()
    async with Itgs() as itgs:
        res = await process_image(
            "tmp/cropped.jpg",
            TARGETS,
            itgs=itgs,
            gd=gd,
            max_width=4096,
            max_height=4096,
            max_area=2048 * 2048,
            max_file_size=1024 * 1024 * 50,
            name_hint="profile_picture",
        )
        with open("tmp/res.json", "w") as f:
            json.dump(dataclasses.asdict(res), f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
