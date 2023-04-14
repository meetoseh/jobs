from typing import Optional, Union
from file_service import AsyncWritableBytesIO
from itgs import Itgs
import aiohttp
import os
import lib.image_files.auth
from urllib.parse import urlencode
from dataclasses import dataclass
import io


@dataclass
class ImageFileExportRef:
    image_file_uid: str
    """The uid of the image file the export is for"""
    base_url: str
    """The url, without presigning, where the export can be downloaded from"""
    width: int
    """Width of the image export in pixels"""
    height: int
    """Height of the image export in pixels"""
    format: str
    """The format of the image export, e.g., webp"""


async def get_biggest_export_ref(
    itgs: Itgs,
    *,
    uid: str,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None,
) -> ImageFileExportRef:
    """Fetches the biggest export in the most modern format for the image file
    with the given uid.

    Args:
        itgs (Itgs): The integrations to (re)use
        uid (str): The uid of the image file to fetch the biggest export for
        max_width (int or None): The maximum width of the export to select
        max_height (int or None): The maximum height of the export to select

    Returns:
        ImageFileExportRef: A ref to the biggest export of the image file
            which matches the given constraints
    """
    jwt = await lib.image_files.auth.create_jwt(itgs, image_file_uid=uid, duration=30)
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

                        return ImageFileExportRef(
                            image_file_uid=uid,
                            base_url=url,
                            width=width,
                            height=height,
                            format=format,
                        )

            raise ValueError(
                f"playlist does not contain any exports in a known format!: {max_width=}, {max_height=}, {uid=}, {data=}"
            )


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
    ref = await get_biggest_export_ref(
        itgs,
        uid=uid,
        max_width=max_width,
        max_height=max_height,
    )
    return f"{ref.base_url}?{urlencode({'jwt': jwt})}"


async def download_export_ref(
    itgs: Itgs,
    ref: ImageFileExportRef,
    f: Union[io.BytesIO, AsyncWritableBytesIO],
    *,
    sync: bool,
) -> None:
    """Downloads the given image file export to the given file, generating
    a short-lived jwt to use for the request

    Args:
        itgs (Itgs): The integrations to (re)use
        ref (ImageFileExportRef): The ref to the export to download
        f (io.BytesIO or AsyncReadableBytesIO): The file to write the export to. Should
            be synchronous if sync is True, and asynchronous if sync is False
        sync (bool): Whether the file is written to synchronously or asynchronously
    """
    jwt = await lib.image_files.auth.create_jwt(
        itgs, image_file_uid=ref.image_file_uid, duration=30
    )

    async with aiohttp.ClientSession() as session:
        async with session.get(
            ref.base_url,
            headers={"Authorization": f"bearer {jwt}"},
        ) as response:
            response.raise_for_status()

            if sync:
                async for chunk in response.content.iter_any():
                    f.write(chunk)
            else:
                async for chunk in response.content.iter_any():
                    await f.write(chunk)
