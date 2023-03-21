from typing import Optional
from itgs import Itgs
import aiohttp
import os
import lib.image_files.auth
from urllib.parse import urlencode


async def get_biggest_export_url(
    itgs: Itgs,
    *,
    uid: str,
    duration: int,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
) -> str:
    """Fetches the biggest export in the most modern format for the image file
    with the given uid.

    Args:
        itgs (Itgs): The integrations to (re)use
        uid (str): The uid of the image file to fetch the biggest export for
        duration (int): How long the link should be good for in seconds
        max_width (int or None): The maximum width of the export to select
        max_height (int or None): The maximum height of the export to select

    Returns:
        str: A presigned url to the biggest export of the image file
    """
    jwt = await lib.image_files.auth.create_jwt(
        itgs, image_file_uid=uid, duration=duration
    )
    root_backend_url = os.environ["ROOT_BACKEND_URL"]

    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{root_backend_url}/api/1/image_files/playlist/{uid}?presign=0",
            headers={"Authorization": f"bearer {jwt}"},
        ) as response:
            response.raise_for_status()

            data = await response.json()
            items: dict = data["items"]
            for format in ["webp", "jpeg", "png"]:
                if format in items:
                    for candidate in reversed(items[format]):
                        url = candidate["url"]
                        width = candidate["width"]
                        height = candidate["height"]

                        if max_width is not None and width > max_width:
                            continue
                        if max_height is not None and height > max_height:
                            continue

                        return url + "?" + urlencode({"jwt": jwt})

            raise ValueError(
                f"playlist does not contain any exports in a known format!: {max_width=}, {max_height=}, {uid=}, {data=}"
            )
