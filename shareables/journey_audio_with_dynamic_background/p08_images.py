import base64
from dataclasses import dataclass
from fractions import Fraction
import json
import random
import secrets
from typing import Dict, List, Literal, Optional, Set, Tuple, Type
from abc import ABCMeta, abstractmethod
from lib.redis_api_limiter import ratelimit_using_redis
from shareables.journey_audio_with_dynamic_background.p07_image_descriptions import (
    ImageDescriptions,
)
from shareables.shareable_pipeline_exception import ShareablePipelineException

from PIL import Image, ImageEnhance, ImageFilter, UnidentifiedImageError
import openai
import os
import io
import logging
import time
import requests
from images import ImageTarget, _make_target
from temp_files import temp_dir, temp_file
from urllib.parse import urlencode, urlparse, quote
from itgs import Itgs
import numpy as np
import shutil
import subprocess
import asyncio
import yaml
import logging.config

from videos import VideoGenericInfo, get_video_generic_info


class ImagesError(ShareablePipelineException):
    def __init__(self, message: str):
        super().__init__(message, 8, "images")


MODEL_CAPABLE_IMAGE_SIZES: Dict[str, List[Tuple[int, int]]] = {
    "dall-e": ((256, 256), (512, 512), (1024, 1024)),
}


@dataclass
class Frame:
    """An image within images. May either be an individual image or a video
    which can be processed to get individual images.
    """

    path: str
    """The path to where the file is stored"""

    style: Literal["image", "video"]
    """The type of image this is. Either an image or a video"""

    video_info: Optional[VideoGenericInfo]
    """Information about the video, if this is a video, otherwise None"""

    def get_image_at(self, timestamp: float) -> Image.Image:
        """Gets a PIL image representing this frame at the given timestamp in seconds
        relative to the start of this frame.
        """
        if self.style == "image":
            res = Image.open(self.path)
            res.load()
            return res

        assert self.style == "video"
        ffmpeg = shutil.which("ffmpeg")

        wrapped_timestamp = timestamp % self.video_info.duration

        cmd = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-nostats",
            "-accurate_seek",
            "-ss",
            f"{wrapped_timestamp:.3f}",
            "-i",
            self.path,
            "-frames:v",
            "1",
            "-c:v",
            "bmp",
            "-an",
            "-f",
            "rawvideo",
            "pipe:1",
        ]

        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:
            raise ImagesError(
                f"ffmpeg exited with code {result.returncode} when trying to get frame at {timestamp} "
                f"from {self.path} using {json.dumps(cmd)}: \n\n```\n{result.stderr.decode()}\n```\n\n```\n{result.stdout}\n```\n\n"
            )

        try:
            return Image.open(io.BytesIO(result.stdout), formats=["bmp"])
        except UnidentifiedImageError:
            logging.warning(
                f"Failed to identify image at {timestamp=} from {self.path=} ({self.video_info=}) using {wrapped_timestamp=}"
            )

            # probably we want the last frame; let's grab that.
            cmd = [
                ffmpeg,
                "-hide_banner",
                "-loglevel",
                "warning",
                "-nostats",
                "-i",
                self.path,
                "-vf",
                f"select='eq(n,{self.video_info.n_frames-1})'",
                "-frames:v",
                "1",
                "-c:v",
                "bmp",
                "-an",
                "-f",
                "rawvideo",
                "pipe:1",
            ]
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode != 0:
                raise ImagesError(
                    f"ffmpeg exited with code {result.returncode} when trying to get frame at {timestamp} (last frame, {self.video_info.n_frames - 1=}) "
                    f"from {self.path} using {json.dumps(cmd)}: \n\n```\n{result.stderr.decode()}\n```\n\n```\n{result.stdout}\n```\n\n"
                )
            return Image.open(io.BytesIO(result.stdout), formats=["bmp"])


@dataclass
class Images:
    """A sequence of images produced for a video, with one image approximately
    every 2 seconds.
    """

    image_descriptions: ImageDescriptions
    """The image descriptions, used for a better string representation of the object."""

    prompts: List[str]
    """The prompts that were used for each image, in order. Used to better understand
    how the images were generated.
    """

    images: List[Tuple[float, Frame]]
    """The images to show in order, where the timestamp is when the image should
    be shown, and the string is the path to the image. 
    """

    def __str__(self) -> str:
        """Produces a string representation of these images and where they can from,
        formatted like in the following:


        Phrase 1. (00:00:00.000 --> 00:00:15.000) "Hello, my name is Anna"
           00:00:00.000 - "A calm and peaceful background setting" -> /path/to/image1.jpg
           00:00:07.500 - "A person's chest rising and falling as they breathe." -> /path/to/image2.jpg
        Phrase 2. (00:00:15.000 --> 00:01:00.000) "Today we are going to talk about the sun"
           00:00:15.000 - "A bright and sunny day." -> /path/to/image3.jpg
           00:00:22.500 - "A smiling person on a street." -> /path/to/image4.jpg
        """

        visible_timerange_index: Optional[int] = None
        current_timerange_index = 0
        for idx, (visible_at, frame) in enumerate(self.images):
            while (
                current_timerange_index + 1
                < len(self.image_descriptions.transcript.phrases)
                and self.image_descriptions.transcript.phrases[
                    current_timerange_index + 1
                ][0].start
                <= visible_at
            ):
                current_timerange_index += 1

            if visible_timerange_index != current_timerange_index:
                timerange = self.image_descriptions.transcript.phrases[
                    current_timerange_index
                ][0]
                print(
                    f'Phrase {current_timerange_index + 1}. ({timerange}) "{self.image_descriptions.transcript.phrases[current_timerange_index][1]}"'
                )
                visible_timerange_index = current_timerange_index

            print(f'   {visible_at} - "{self.prompts[idx]}" -> {frame.path}')


class ImageGenerator(metaclass=ABCMeta):
    width: int
    """The width of the images to generate. Readonly"""

    height: int
    """The height of the images to generate. Readonly"""

    @abstractmethod
    async def generate(self, itgs: Itgs, prompt: str, folder: str) -> Frame:
        """Generates a new image using the given prompt, at the width and
        height that this generator is capable of. The frame is written
        to a file within the given folder, and the path to the file is
        returned.
        """
        ...


class DalleImageGenerator(ImageGenerator):
    """Uses OpenAI's dall-e generator to create images. Not thread safe if an api
    delay is specified, but does not require any local hardware beyond enough
    memory to process the images.
    """

    def __init__(
        self,
        width: int,
        height: int,
        *,
        api_key: Optional[str] = None,
    ):
        assert (width, height) in MODEL_CAPABLE_IMAGE_SIZES["dall-e"]
        self.width = width
        self.height = height

        self.openai_api_key = (
            api_key if api_key is not None else os.environ["OSEH_OPENAI_API_KEY"]
        )
        """The api key used to connect to openai"""

    async def generate(self, itgs: Itgs, prompt: str, folder: str) -> Frame:
        await ratelimit_using_redis(
            itgs, key="external_apis:api_limiter:dall-e", time_between_requests=5
        )

        response = openai.Image.create(
            api_key=self.openai_api_key,
            prompt=prompt,
            n=1,
            size=f"{self.width}x{self.height}",
            response_format="b64_json",
        )
        img = Image.open(
            io.BytesIO(base64.b64decode(bytes(response.data[0].b64_json, "utf-8")))
        )

        filename = f"{secrets.token_urlsafe(8)}.webp"
        filepath = os.path.join(folder, filename)
        img.save(filepath, "webp", optimize=True, quality=95, method=4)
        return Frame(path=filepath, style="image", video_info=None)


class PexelsImageGenerator(ImageGenerator):
    def __init__(
        self,
        width: int,
        height: int,
        *,
        api_key: Optional[str] = None,
    ):
        self.width = width
        self.height = height

        self.pexels_api_key = (
            os.environ["OSEH_PEXELS_API_KEY"] if api_key is None else api_key
        )
        """The api key used to connect to pexels"""

    async def generate(self, itgs: Itgs, prompt: str, folder: str) -> Frame:
        await ratelimit_using_redis(
            itgs, key="external_apis:api_limiter:pexels", time_between_requests=40
        )

        img = self._generate_img(prompt)
        filename = f"{secrets.token_urlsafe(8)}.webp"
        filepath = os.path.join(folder, filename)
        img.save(filepath, "webp", optimize=True, quality=95, method=4)
        return Frame(path=filepath, style="image", video_info=None)

    def _generate_img(self, prompt: str) -> Image.Image:
        response = requests.get(
            "https://api.pexels.com/v1/search?"
            + urlencode(
                {
                    "query": prompt,
                    "per_page": 1,
                    "page": random.randint(1, 20),
                    "orientation": (
                        "landscape"
                        if self.width > self.height
                        else ("portrait" if self.height > self.width else "square")
                    ),
                    "size": "medium",
                    "locale": "en-US",
                },
                quote_via=quote,
            ),
            headers={"Authorization": self.pexels_api_key},
        )
        if not response.ok:
            raise Exception(
                f"Pexels API returned {response.status_code}: {response.text}"
            )

        data = response.json()
        url = data["photos"][0]["src"]["original"]

        response = requests.get(
            url, headers={"Authorization": self.pexels_api_key}, stream=True
        )
        if not response.ok:
            raise Exception(
                f"Pexels Image GET returned {response.status_code}: {response.text}"
            )

        with temp_file() as tmp_path, temp_file() as out_path:
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=16 * 1024):
                    f.write(chunk)

            peeked = Image.open(tmp_path)
            if peeked.width == self.width and peeked.height == self.height:
                peeked.load()
                return peeked

            peeked.close()

            target = _make_target(
                tmp_path,
                ImageTarget(
                    required=True,
                    width=self.width,
                    height=self.height,
                    format="webp",
                    quality_settings={"lossless": False, "quality": 95, "method": 4},
                ),
                out_path,
            )

            if target is None:
                raise Exception("Failed to produce image target")

            result = Image.open(out_path, formats=["webp"])
            if result.width != self.width or result.height != self.height:
                raise Exception(
                    f"Expected {self.width}x{self.height}, got {result.width}x{result.height}"
                )

            result.load()
            return result


@dataclass
class PexelVideo:
    url: str
    width: int
    height: int
    duration: float


class PexelsVideosImageGenerator(ImageGenerator):
    def __init__(
        self,
        width: int,
        height: int,
        *,
        api_key: Optional[str] = None,
    ):
        self.width = width
        self.height = height

        self.pexels_api_key = (
            os.environ["OSEH_PEXELS_API_KEY"] if api_key is None else api_key
        )
        """The api key used to connect to pexels"""

    async def generate(self, itgs: Itgs, prompt: str, folder: str) -> Frame:
        await ratelimit_using_redis(
            itgs, key="external_apis:api_limiter:pexels", time_between_requests=40
        )

        raw_video_infos = self._generate_raw_video(prompt)

        for raw_video_info in raw_video_infos:
            iden = secrets.token_urlsafe(8)
            raw_path = os.path.join(folder, f"{iden}-raw.mp4")
            await ratelimit_using_redis(
                itgs,
                key="external_apis:api_limiter:pexels",
                time_between_requests=40,
            )
            with open(raw_path, "wb") as f:
                response = requests.get(raw_video_info.url, stream=True)
                if not response.ok:
                    if response.status_code in (404, 410):
                        logging.warning(
                            f"Retrying video at {raw_video_info.url} ({response.status_code=}) in 60s"
                        )
                        time.sleep(60)
                        await ratelimit_using_redis(
                            itgs,
                            key="external_apis:api_limiter:pexels",
                            time_between_requests=40,
                        )
                        response = requests.get(raw_video_info.url, stream=True)
                        if not response.ok:
                            if response.status_code in (404, 410):
                                logging.warning(
                                    f"Skipping video at {raw_video_info.url} ({response.status_code=}), after 180s"
                                )
                                time.sleep(180)
                                continue
                            raise Exception(
                                f"Pexels Video GET returned {response.status_code}: {response.text}"
                            )
                        continue

                    raise Exception(
                        f"Pexels Video GET returned {response.status_code}: {response.text}"
                    )

                for chunk in response.iter_content(chunk_size=16 * 1024):
                    f.write(chunk)

            info = get_video_generic_info(raw_path)

            if (
                raw_video_info.width == self.width
                and raw_video_info.height == self.height
            ):
                return Frame(path=raw_path, style="video", video_info=info)

            out_path = os.path.join(folder, f"{iden}-transformed.mp4")
            ffmpeg = shutil.which("ffmpeg")
            cmd = [
                ffmpeg,
                "-hide_banner",
                "-loglevel",
                "warning",
                "-nostats",
                "-i",
                raw_path,
                "-codec:v",
                "h264",
                "-codec:a",
                "aac",
                "-b:v",
                f"10486k",
                "-b:a",
                f"384k",
                "-vf",
                f"crop={self.width}:{self.height}",
                "-map_metadata",
                "-1",
                "-map_chapters",
                "-1",
                "-movflags",
                "+faststart",
                out_path,
            ]

            logging.info(f"Running command: {json.dumps(cmd)}")
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode != 0:
                raise Exception(
                    "ffmpeg failed\n\nstdout: ```\n"
                    + result.stdout.decode("utf-8")
                    + "\n```\n\nstderr: ```\n"
                    + result.stderr.decode("utf-8")
                    + "\n```"
                )

            info = get_video_generic_info(out_path)
            return Frame(path=out_path, style="video", video_info=info)

        raise Exception(f"Could not find usable videos for prompt {prompt=}")

    def _generate_raw_video(self, prompt: str) -> List[PexelVideo]:
        """Finds a few videos on pexels that matches the prompt and meets the size requirements."""
        response = requests.get(
            "https://api.pexels.com/videos/search?"
            + urlencode(
                {
                    "query": prompt,
                    "per_page": 2,
                    "page": random.randint(1, 10),
                    "size": "medium",
                    "locale": "en-US",
                    "orientation": (
                        "landscape"
                        if self.width > self.height
                        else ("portrait" if self.height > self.width else "square")
                    ),
                }
            ),
            headers={"Authorization": self.pexels_api_key},
        )
        if not response.ok:
            raise Exception(
                f"Pexels API returned {response.status_code}: {response.text}"
            )

        res: List[PexelVideo] = []
        data = response.json()
        logging.debug(f"pexels response for {prompt=}: {json.dumps(data)}")
        for raw_video in data["videos"]:
            if raw_video["width"] < self.width or raw_video["height"] < self.height:
                logging.debug(
                    f"Skipping video {raw_video['id']} because it is too small ({raw_video['width']}x{raw_video['height']} vs {self.width}x{self.height}"
                )
                continue

            for output in raw_video["video_files"]:
                if (
                    output["width"] < self.width
                    or output["height"] < self.height
                    or output["file_type"] != "video/mp4"
                ):
                    continue

                parsed_link = urlparse(str(output["link"]))
                filename = os.path.basename(parsed_link.path)
                if not filename.endswith(".mp4"):
                    continue

                video = PexelVideo(
                    url=output["link"],
                    width=output["width"],
                    height=output["height"],
                    duration=raw_video["duration"],
                )
                res.append(video)
                break

        if not res:
            raise Exception(
                f"No suitable videos found for {prompt=}, {self.width=}, {self.height=}"
            )
        return res


class StabilityAIImageGenerator(ImageGenerator):
    def __init__(
        self,
        width: int,
        height: int,
        *,
        initial_width: int = 384,
        initial_height: int = 640,
        engine_id: str = "stable-diffusion-xl-beta-v2-2-2",
        upscaler_id: str = "esrgan-v1-x2plus",
        style_preset: str = "digital-art",
        api_key: Optional[str] = None,
    ) -> None:
        self.width: int = width
        self.height: int = height
        self.initial_width: int = initial_width
        self.initial_height: int = initial_height
        self.engine_id: str = engine_id
        self.upscaler_id: str = upscaler_id
        self.style_preset: str = style_preset
        self.api_key: str = (
            api_key if api_key is not None else os.environ["OSEH_STABILITY_AI_KEY"]
        )

    async def generate(self, itgs: Itgs, prompt: str, folder: str) -> Frame:
        logging.info(
            f"Requesting {self.initial_width}x{self.initial_height} image using stability ai "
            f'engine {self.engine_id}, style preset {self.style_preset}, and prompt "{prompt}"'
        )
        started_at = time.time()
        response = requests.post(
            f"https://api.stability.ai/v1/generation/{self.engine_id}/text-to-image",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            json={
                "text_prompts": [
                    {
                        "text": prompt,
                    }
                ],
                "width": self.initial_width,
                "height": self.initial_height,
                "style_preset": self.style_preset,
                "samples": 1,
            },
        )
        if not response.ok:
            logging.warn(
                f"Stability AI returned {response.status_code}: {response.text}"
            )
            response.raise_for_status()

        data = response.json()
        base_image_bas64 = data["artifacts"][0]["base64"]
        image_path = os.path.join(folder, f"{secrets.token_urlsafe(16)}.png")
        with open(image_path, "wb") as f:
            f.write(base64.b64decode(base_image_bas64))

        img = Image.open(image_path)
        assert (
            img.width == self.initial_width and img.height == self.initial_height
        ), f"expected {self.initial_width}x{self.initial_height} image, got {img.width}x{img.height}"
        assert img.mode == "RGB", f"expected RGB image, got {img.mode}"
        assert img.format == "PNG", f"expected PNG image, got {img.format}"
        img.close()

        img_width = self.initial_width
        img_height = self.initial_height

        while img_width < self.width or img_height < self.height:
            logging.info(
                f"Upscaling image from {img_width}x{img_height} to {img_width*2}x{img_height*2} "
                f"using stability-ai upscaler engine {self.upscaler_id}"
            )
            with open(image_path, "rb") as f:
                response = requests.post(
                    f"https://api.stability.ai/v1/generation/{self.upscaler_id}/image-to-image/upscale",
                    headers={
                        "Accept": "image/png",
                        "Authorization": f"Bearer {self.api_key}",
                    },
                    files={
                        "image": f,
                    },
                )

            response.raise_for_status()
            img = Image.open(io.BytesIO(response.content))
            assert (
                img.width == img_width * 2 and img.height == img_height * 2
            ), f"expected {img_width*2}x{img_height*2} image, got {img.width}x{img.height}"
            assert img.mode == "RGB", f"expected RGB image, got {img.mode}"
            assert img.format == "PNG", f"expected PNG image, got {img.format}"
            img.save(image_path)
            img.close()

            img_width *= 2
            img_height *= 2

        if img_width != self.width or img_height != self.height:
            current_aspect_ratio = Fraction(img_width, img_height)
            target_aspect_ratio = Fraction(self.width, self.height)
            if current_aspect_ratio != target_aspect_ratio:
                if current_aspect_ratio < target_aspect_ratio:
                    # the image is too tall; crop from top and bottom
                    crop_to_height = int(img_width / target_aspect_ratio)
                    crop_to = (
                        0,
                        (img_height - crop_to_height) // 2,
                        img_width,
                        (img_height + crop_to_height) // 2,
                    )
                else:
                    # the image is too wide; crop from left and right
                    crop_to_width = int(img_height * target_aspect_ratio)
                    crop_to = (
                        (img_width - crop_to_width) // 2,
                        0,
                        (img_width + crop_to_width) // 2,
                        img_height,
                    )

                logging.info(
                    f"Cropping image from {img_width}x{img_height} to {crop_to[2]-crop_to[0]}x{crop_to[3]-crop_to[1]} "
                    f"starting at ({crop_to[0]},{crop_to[1]})"
                )
                img = Image.open(image_path)
                img.load()
                img = img.crop(crop_to)
                img.save(image_path)
                img.close()
                img_width = crop_to[2] - crop_to[0]
                img_height = crop_to[3] - crop_to[1]

        if img_width != self.width or img_height != self.height:
            logging.info(
                f"Resizing image from {img_width}x{img_height} to {self.width}x{self.height}"
            )
            img = Image.open(image_path)
            img.load()
            img = img.resize((self.width, self.height), Image.LANCZOS)
            img.save(image_path)
            img.close()
            img_width = self.width
            img_height = self.height

        time_taken = time.time() - started_at
        logging.info(
            f"Finished generating {img_width}x{img_height} image using stability ai in {time_taken:.3f}s"
        )
        return Frame(path=image_path, style="image", video_info=None)


class DummyGenerator(ImageGenerator):
    """A dummy generator that just returns the same frame every time, for
    testing purposes.
    """

    def __init__(self, width: int, height: int, frame: Frame) -> None:
        self.width = width
        self.height = height
        self.frame = frame

    async def generate(self, prompt: str, folder: str) -> Frame:
        return self.frame


class TransformationImageGenerator(ImageGenerator):
    """Uses an abstract method "transform_image" on a wrapped generator
    to produce images. This adds support for both image and video to
    what is implemented as an image-only generator.
    """

    def __init__(self, wrapped: ImageGenerator) -> None:
        self.width = wrapped.width
        self.height = wrapped.height
        self.wrapped = wrapped
        """The model that is actually generating the images"""

    async def generate(self, itgs: Itgs, prompt: str, folder: str) -> Frame:
        inner_frame = await self.wrapped.generate(itgs, prompt, folder)

        if inner_frame.style == "image":
            img = Image.open(inner_frame.path)
            img.load()
            img = self.transform_image(img)
            img.save(inner_frame.path, "webp", optimize=True, quality=95, method=4)
            return inner_frame

        assert inner_frame.style == "video"

        outpath = os.path.join(folder, f"{secrets.token_urlsafe(8)}.mp4")
        ffmpeg = shutil.which("ffmpeg")
        reader_cmd = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-nostats",
            "-i",
            inner_frame.path,
            "-c:v",
            "bmp",
            "-an",
            "-f",
            "rawvideo",
            "pipe:1",
        ]
        reader = subprocess.Popen(reader_cmd, stdout=subprocess.PIPE)

        writer_cmd = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "quiet",
            "-nostats",
            "-f",
            "rawvideo",
            "-codec:v",
            "rawvideo",
            "-pix_fmt",
            "rgb24",
            "-s",
            f"{self.width}x{self.height}",
            "-r",
            str(inner_frame.video_info.framerate),
            "-i",
            "pipe:0",
            "-pix_fmt",
            "yuv420p",
            "-codec:v",
            "h264",
            "-b:v",
            "10486k",
            "-an",
            "-movflags",
            "+faststart",
            outpath,
        ]
        writer = subprocess.Popen(
            writer_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE
        )

        while True:
            bmp = b""
            next_frame_header = reader.stdout.read(14)
            if not next_frame_header:
                break
            assert next_frame_header[:2] == b"BM"
            bmp_size = int.from_bytes(next_frame_header[2:6], "little", signed=False)
            bmp += next_frame_header
            bmp += reader.stdout.read(bmp_size - 14)

            frame = Image.open(io.BytesIO(bmp), formats=["bmp"])
            frame.load()
            frame = self.transform_image(frame)
            writer.stdin.write(frame.tobytes())

        reader.stdout.close()
        writer.stdin.close()

        reader_code = reader.wait()
        if reader_code != 0:
            raise Exception(f"ffmpeg exited with code {reader_code}")

        writer_code = writer.wait()
        if writer_code != 0:
            raise Exception(f"ffmpeg exited with code {writer_code}")

        info = get_video_generic_info(outpath)
        return Frame(path=outpath, style="video", video_info=info)

    @abstractmethod
    def transform_image(self, res: Image.Image) -> Image.Image:
        """Transforms the given PIL image and returns the result."""
        ...


class DarkenTopAndBottomImageGenerator(TransformationImageGenerator):
    """Darkens the top and bottom third of the image using a simple linear
    gradient.
    """

    def transform_image(self, res: Image.Image) -> Image.Image:
        res_as_arr = np.asarray(res).copy()

        third_height = res_as_arr.shape[0] // 3
        color_multiplier = np.repeat(
            np.repeat(
                np.linspace(0.0, 1.0, third_height, dtype=np.float64)[:, np.newaxis],
                res_as_arr.shape[1],
                axis=1,
            )[:, :, np.newaxis],
            3,
            axis=2,
        )

        res_as_arr[:third_height] = (
            res_as_arr[:third_height].astype(np.float64) * color_multiplier
        ).astype(np.uint8)
        res_as_arr[-third_height:] = (
            res_as_arr[-third_height:].astype(np.float64) * color_multiplier[::-1, :, :]
        ).astype(np.uint8)

        return Image.fromarray(res_as_arr)


class DarkenImageGenerator(TransformationImageGenerator):
    def transform_image(self, res: Image.Image) -> Image.Image:
        return ImageEnhance.Brightness(res).enhance(0.6)


GENERATOR_CLASSES_BY_NAME: Dict[str, Type[ImageGenerator]] = {
    "dall-e": DalleImageGenerator,
}


class BlurredBackgroundImageResizer(TransformationImageGenerator):
    """Generates a larger image than the one generated by the underlying
    model by using a darkened blurred background.
    """

    def __init__(self, wrapped: ImageGenerator, width: int, height: int) -> None:
        assert wrapped.width <= width
        assert wrapped.height <= height
        self.wrapped = wrapped
        """The model that is actually generating the images"""

        self.width = width
        """The width of the images to generate"""

        self.height = height
        """The height of the images to generate"""

    def transform_image(self, core: Image.Image) -> Image.Image:
        if core.width == self.width and core.height == self.height:
            return core

        out = core.resize((self.width, self.height), Image.NEAREST)
        out = out.filter(ImageFilter.GaussianBlur(15))

        out.paste(
            core,
            (
                (self.width - core.width) // 2,
                (self.height - core.height) // 2,
            ),
        )
        return out


class CropImageGenerator(TransformationImageGenerator):
    """Generates a smaller image by cropping the center of the image generated
    by the underlying model.
    """

    def __init__(self, wrapped: ImageGenerator, width: int, height: int) -> None:
        assert wrapped.width >= width
        assert wrapped.height >= height
        self.wrapped = wrapped
        """The model that is actually generating the images"""

        self.width = width
        """The width of the images to generate"""

        self.height = height
        """The height of the images to generate"""

    def transform_image(self, core: Image.Image) -> Image.Image:
        if core.width == self.width and core.height == self.height:
            return core

        return core.crop(
            (
                (core.width - self.width) // 2,
                (core.height - self.height) // 2,
                (core.width + self.width) // 2,
                (core.height + self.height) // 2,
            )
        )


def create_image_generator(
    width: int,
    height: int,
    *,
    model: Optional[Literal["dall-e", "pexels", "pexels-video", "stability-ai"]] = None,
) -> ImageGenerator:
    """Produces the standard image generator for generating images of the given
    width and height, based on what is actually installed.
    """
    if model is None:
        model = "dall-e"

    if model == "pexels":
        return DarkenImageGenerator(PexelsImageGenerator(width, height))

    if model == "pexels-video":
        return DarkenImageGenerator(PexelsVideosImageGenerator(width, height))

    if model == "stability-ai":
        return DarkenImageGenerator(StabilityAIImageGenerator(width, height))

    model_sizes = MODEL_CAPABLE_IMAGE_SIZES[model]
    if width < model_sizes[0][0] or height < model_sizes[0][1]:
        raise ValueError(
            f"Cannot generate an image smaller than {model_sizes[0][0]}x{model_sizes[0][1]}"
        )

    model_size = next(
        (size for size in model_sizes if size[0] >= width and size[1] >= height),
        model_sizes[-1],
    )

    generator = GENERATOR_CLASSES_BY_NAME[model](
        model_size[0],
        model_size[1],
    )
    if width != model_size[0] or height != model_size[1]:
        if width >= model_size[0] and height >= model_size[1]:
            generator = BlurredBackgroundImageResizer(generator, width, height)
        elif width <= model_size[0] and height <= model_size[1]:
            generator = CropImageGenerator(generator, width, height)
        else:
            # crop one dimension, blur the other
            if width >= model_size[0]:
                generator = CropImageGenerator(generator, width, model_size[1])
            else:
                generator = CropImageGenerator(generator, model_size[0], height)

            generator = BlurredBackgroundImageResizer(generator, width, height)

    return DarkenImageGenerator(generator)


def generate_image_from_prompt(
    prompt: str, width: int, height: int, model: str, out: str
) -> None:
    """Generates an image from the given prompt at the given width. The model that
    we use can only produce images of certain sizes, so in order to produce
    an image which is larger, this will expand with a blurred and darkened background
    based on the image.

    This is primarily for manually testing images in the cli; normally, create the
    generator using create_image_generator and use that instead, in case the model
    benefits from being reused.

    Args:
        prompt (str): The prompt to generate the image from
        width (int): The width of the image to generate
        height (int): The height of the image to generate
        out (str): The file to write the result to. Always writes a webp file, regardless
            of the extension of the file.
    """
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)

    async def _inner():
        with temp_dir() as folder:
            async with Itgs() as itgs:
                frame = await create_image_generator(
                    width, height, model=model
                ).generate(itgs, prompt, folder)
                os.rename(frame.path, out)

    asyncio.run(_inner())


async def create_images(
    image_descriptions: ImageDescriptions,
    width: int,
    height: int,
    *,
    itgs: Itgs,
    folder: str,
    min_image_duration: float = 3.0,
    std_image_duration: float = 4.0,
    max_image_duration: float = 5.0,
    max_retries: int = 5,
    model: Optional[Literal["dall-e", "pexels", "pexels-video", "stability-ai"]] = None,
) -> Images:
    """Produces background images of the given width and height using the given
    image descriptions.

    Args:
        image_descriptions (ImageDescriptions): The prompts to use to generate the
            images, broken down by when the prompts are to be displayed. Note that
            prompts may be reused or selected from arbitrarily, especially if the
            number of prompts does not match the number of images to be generated
            for a given segment.
        width (int): The width of the images to generate
        height (int): The height of the images to generate
        folder (str): The folder within which to store generated images.
        min_image_duration (float, optional): The minimum amount of time to show
            each image for.
        standard_image_duration (float, optional): When nothing else is pressuring
            us to show an image for a certain amount of time, this is the amount
            of time to show each image for.
        max_image_duration (float, optional): The maximum amount of time to show
            each image for. Ignored for videos and near the ends of segments

    Returns:
        Images: The images to display, broken down by when they are to be displayed.

    Raises:
        ImagesError: If there is a problem generating the images.
    """
    generator = create_image_generator(width, height, model=model)

    images: List[Tuple[float, Frame]] = []
    prompts: List[str] = []
    next_image_starts_at: float = 0.0
    video_duration = image_descriptions.image_descriptions[-1][0].end.in_seconds()
    current_segment_index = 0
    prompts_remaining_in_segment: Set[str] = set()

    last_error: Optional[Exception] = None

    while next_image_starts_at < video_duration - min_image_duration:
        image_duration = (
            random.triangular(
                min_image_duration,
                max_image_duration,
                std_image_duration,
            )
            if video_duration - next_image_starts_at > max_image_duration
            else video_duration - next_image_starts_at
        )

        while (
            image_descriptions.image_descriptions[current_segment_index][
                0
            ].end.in_seconds()
            <= next_image_starts_at
            and current_segment_index < len(image_descriptions.image_descriptions) - 1
        ):
            current_segment_index += 1
            prompts_remaining_in_segment = set()

        if not prompts_remaining_in_segment:
            prompts_remaining_in_segment = set(
                image_descriptions.image_descriptions[current_segment_index][1]
            )

        current_segment_end = image_descriptions.image_descriptions[
            current_segment_index
        ][0].end.in_seconds()
        time_until_next_segment = current_segment_end - next_image_starts_at
        image_duration = min(image_duration, time_until_next_segment)

        if time_until_next_segment - image_duration < min_image_duration:
            image_duration = time_until_next_segment

        found_frame = False
        while not found_frame:
            if not prompts_remaining_in_segment:
                err_msg = f"Ran out of prompts to attempt for segment; original prompts: {image_descriptions.image_descriptions[current_segment_index][1]}"
                if last_error is not None:
                    raise ImagesError(err_msg) from last_error
                else:
                    raise ImagesError(err_msg)

            prompt = random.choice(list(prompts_remaining_in_segment))
            prompts_remaining_in_segment.remove(prompt)

            for attempt in range(max_retries):
                try:
                    frame = await generator.generate(itgs, prompt, folder)
                    found_frame = True
                    break
                except Exception as e:
                    last_error = e
                    logging.info(
                        f"Failing to generate image (attempt {attempt+1}/{max_retries})"
                    )

        prompts.append(prompt)
        images.append((next_image_starts_at, frame))

        if frame.style == "video":
            # 1. show at least 20 seconds
            image_duration = max(image_duration, 20.0)

            # 2. If less than 30 seconds until the end of the segment,
            #    go until the end of the segment
            if time_until_next_segment < 30.0:
                image_duration = time_until_next_segment

            # 3. don't loop the video
            image_duration = min(image_duration, frame.video_info.duration)

            # 4. don't go past the end of the segment
            image_duration = min(image_duration, time_until_next_segment)

            # if that means we're using less than 10 seconds, and the video
            # could take us through the next segment, go through the next
            # segment instead
            if (
                image_duration < 10
                and current_segment_index
                < len(image_descriptions.image_descriptions) - 1
                and (
                    image_descriptions.image_descriptions[current_segment_index + 1][
                        0
                    ].end.in_seconds()
                    - next_image_starts_at
                )
                < frame.video_info.duration
            ):
                image_duration = (
                    image_descriptions.image_descriptions[current_segment_index + 1][
                        0
                    ].end.in_seconds()
                    - next_image_starts_at
                )

            # alternatively, don't show videos less than 5s as it's jarring
            if image_duration < 5 and frame.video_info.duration >= 5:
                image_duration = 5

        next_image_starts_at += image_duration

    return Images(image_descriptions=image_descriptions, prompts=prompts, images=images)
