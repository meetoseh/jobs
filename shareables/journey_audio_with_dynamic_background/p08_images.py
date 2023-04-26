import base64
from dataclasses import dataclass
import json
import random
import secrets
from typing import Dict, List, Literal, Optional, Set, Tuple, Type
from abc import ABCMeta, abstractmethod
from shareables.journey_audio_with_dynamic_background.p06_transcript import Timestamp
from shareables.journey_audio_with_dynamic_background.p07_image_descriptions import (
    ImageDescriptions,
)
from shareables.shareable_pipeline_exception import ShareablePipelineException

try:
    from diffusers import StableDiffusionPipeline, DPMSolverMultistepScheduler
    import torch

    HAVE_STABLE_DIFFUSION = True
except ImportError:
    HAVE_STABLE_DIFFUSION = False
from PIL import Image, ImageFilter, UnidentifiedImageError
import openai
import os
import io
import logging
import time
import requests
from images import ImageTarget, _make_target
from temp_files import temp_file
from urllib.parse import urlencode, urlparse, quote
import numpy as np
import shutil
import subprocess


class ImagesError(ShareablePipelineException):
    def __init__(self, message: str):
        super().__init__(message, 8, "images")


MODEL_CAPABLE_IMAGE_SIZES: Dict[str, List[Tuple[int, int]]] = {
    "dall-e": ((256, 256), (512, 512), (1024, 1024)),
    "stable-diffusion": ((768, 768),),
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

    framerate: Optional[float]
    """The framerate, in frames per second. Only applicable for video
    frames.
    """

    duration: Optional[float]
    """The duration of the video, in seconds. None if this is an image.
    If the video is displayed longer than its duration, it should loop.
    """

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

        wrapped_timestamp = timestamp % self.duration

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
                f"Failed to identify image at {timestamp=} from {self.path=} ({self.duration=}) using {wrapped_timestamp=}"
            )

            # probably we want the last frame; let's grab that. But in order
            # to do so we need to know how many frames are in the video
            ffprobe = shutil.which("ffprobe")
            cmd = [
                ffprobe,
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-count_frames",
                "-show_entries",
                "stream=nb_read_frames",
                "-print_format",
                "json",
                self.path,
            ]
            logging.debug(
                f"Running {json.dumps(cmd)} to get last frame of {self.path=}"
            )
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode != 0:
                raise Exception(
                    "ffprobe failed\n\nstdout: ```\n"
                    + result.stdout.decode("utf-8")
                    + "\n```\n\nstderr: ```\n"
                    + result.stderr.decode("utf-8")
                    + "\n```"
                )

            result_json = json.loads(result.stdout.decode("utf-8"))
            n_frames = result_json["streams"][0]["nb_read_frames"]

            cmd = [
                ffmpeg,
                "-hide_banner",
                "-loglevel",
                "warning",
                "-nostats",
                "-i",
                self.path,
                "-vf",
                f"select=eq(n,{n_frames-1})",
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
                    f"ffmpeg exited with code {result.returncode} when trying to get frame at {timestamp} (last frame, {n_frames=}) "
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
    def generate(self, prompt: str, folder: str) -> Frame:
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
        api_delay: Optional[float] = 1.0,
        api_key: Optional[str] = None,
    ):
        assert (width, height) in MODEL_CAPABLE_IMAGE_SIZES["dall-e"]
        self.width = width
        self.height = height

        self.openai_api_key = (
            api_key if api_key is not None else os.environ["OSEH_OPENAI_API_KEY"]
        )
        """The api key used to connect to openai"""

        self.api_delay = api_delay
        """The minimum amount of time to wait between api calls, in seconds"""

        self._last_api_call_time: Optional[float] = None
        """The time.perf_counter() of the last time an api request was made, or
        None
        """

    def generate(self, prompt: str, folder: str) -> Frame:
        if self.api_delay is not None:
            now = time.perf_counter()
            if self._last_api_call_time is not None:
                time_since_last = now - self._last_api_call_time
                if time_since_last < self.api_delay:
                    time.sleep(self.api_delay - time_since_last)
            self._last_api_call_time = now

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
        return Frame(path=filepath, style="image", framerate=None, duration=None)


class StableDiffusionImageGenerator(ImageGenerator):
    def __init__(self, width: int, height: int):
        assert (width, height) in MODEL_CAPABLE_IMAGE_SIZES["stable-diffusion"]
        self.width = width
        self.height = height

        pipe = StableDiffusionPipeline.from_pretrained(
            "stabilityai/stable-diffusion-2-1",
            torch_dtype=torch.float16,
            local_files_only=os.environ["ENVIRONMENT"] != "dev",
        )
        pipe.scheduler = DPMSolverMultistepScheduler.from_config(pipe.scheduler.config)
        pipe = pipe.to("cuda")

        self.pipe = pipe
        """The actual pipeline used to generate images"""

    def generate(self, prompt: str, folder: str) -> Frame:
        img = self.pipe(prompt).images[0]
        filename = f"{secrets.token_urlsafe(8)}.webp"
        filepath = os.path.join(folder, filename)
        img.save(filepath, "webp", optimize=True, quality=95, method=4)
        return Frame(path=filepath, style="image", framerate=None, duration=None)


class PexelsImageGenerator(ImageGenerator):
    def __init__(
        self,
        width: int,
        height: int,
        *,
        api_delay: Optional[float] = 1.0,
        api_key: Optional[str] = None,
    ):
        self.width = width
        self.height = height

        self.pexels_api_key = (
            os.environ["OSEH_PEXELS_API_KEY"] if api_key is None else api_key
        )
        """The api key used to connect to pexels"""

        self.api_delay = api_delay
        """The minimum amount of time to wait between api calls, in seconds"""

        self._last_api_call_time: Optional[float] = None
        """The time.perf_counter() of the last time an api request was made, or
        None
        """

    def generate(self, prompt: str, folder: str) -> Frame:
        if self.api_delay is not None:
            now = time.perf_counter()
            if self._last_api_call_time is not None:
                time_since_last = now - self._last_api_call_time
                if time_since_last < self.api_delay:
                    time.sleep(self.api_delay - time_since_last)
            self._last_api_call_time = now

        img = self._generate_img(prompt)
        filename = f"{secrets.token_urlsafe(8)}.webp"
        filepath = os.path.join(folder, filename)
        img.save(filepath, "webp", optimize=True, quality=95, method=4)
        return Frame(path=filepath, style="image", framerate=None, duration=None)

    def _generate_img(self, prompt: str) -> Image.Image:
        response = requests.get(
            "https://api.pexels.com/v1/search?"
            + urlencode(
                {
                    "query": prompt,
                    "per_page": 1,
                    "orientation": "portrait",
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
        api_delay: Optional[float] = 40.0,
        api_key: Optional[str] = None,
    ):
        self.width = width
        self.height = height

        self.pexels_api_key = (
            os.environ["OSEH_PEXELS_API_KEY"] if api_key is None else api_key
        )
        """The api key used to connect to pexels"""

        self.api_delay = api_delay
        """The minimum amount of time to wait between api calls, in seconds"""

        self._last_api_call_time: Optional[float] = None
        """The time.perf_counter() of the last time an api request was made, or
        None
        """

    def generate(self, prompt: str, folder: str) -> Frame:
        if self.api_delay is not None:
            now = time.perf_counter()
            if self._last_api_call_time is not None:
                time_since_last = now - self._last_api_call_time
                if time_since_last < self.api_delay:
                    time.sleep(self.api_delay - time_since_last)
            self._last_api_call_time = now

        raw_video_infos = self._generate_raw_video(prompt)

        for raw_video_info in raw_video_infos:
            iden = secrets.token_urlsafe(8)
            raw_path = os.path.join(folder, f"{iden}-raw.mp4")
            with open(raw_path, "wb") as f:
                response = requests.get(raw_video_info.url, stream=True)
                if not response.ok:
                    if response.status_code in (404, 410):
                        logging.warning(
                            f"Retrying video at {raw_video_info.url} ({response.status_code=}) in 60s"
                        )
                        time.sleep(60)
                        response = requests.get(raw_video_info.url, stream=True)
                        if not response.ok:
                            if response.status in (404, 410):
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

            ffprobe = shutil.which("ffprobe")
            cmd = [
                ffprobe,
                "-v",
                "warning",
                "-print_format",
                "json",
                "-select_streams",
                "v",
                "-show_entries",
                "stream=r_frame_rate,duration",
                raw_path,
            ]
            logging.info(f"Running command: {json.dumps(cmd)}")
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode != 0:
                raise Exception(
                    "ffprobe failed\n\nstdout: ```\n"
                    + result.stdout.decode("utf-8")
                    + "\n```\n\nstderr: ```\n"
                    + result.stderr.decode("utf-8")
                    + "\n```"
                )

            info = json.loads(result.stdout.decode("utf-8"))
            true_duration = float(info["streams"][0]["duration"])
            framerate_frac: str = info["streams"][0]["r_frame_rate"]

            num, denom = framerate_frac.split("/")
            framerate = float(num) / float(denom)

            if (
                raw_video_info.width == self.width
                and raw_video_info.height == self.height
            ):
                return Frame(
                    path=raw_path,
                    style="video",
                    framerate=framerate,
                    duration=true_duration,
                )

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

            cmd = [
                ffprobe,
                "-v",
                "warning",
                "-print_format",
                "json",
                "-select_streams",
                "v",
                "-show_entries",
                "stream=r_frame_rate,duration",
                out_path,
            ]
            logging.info(f"Running command: {json.dumps(cmd)}")
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode != 0:
                raise Exception(
                    "ffprobe failed\n\nstdout: ```\n"
                    + result.stdout.decode("utf-8")
                    + "\n```\n\nstderr: ```\n"
                    + result.stderr.decode("utf-8")
                    + "\n```"
                )

            info = json.loads(result.stdout.decode("utf-8"))
            true_duration = float(info["streams"][0]["duration"])
            framerate_frac: str = info["streams"][0]["r_frame_rate"]

            num, denom = framerate_frac.split("/")
            framerate = float(num) / float(denom)

            return Frame(
                path=out_path,
                style="video",
                framerate=framerate,
                duration=true_duration,
            )

        raise Exception(f"Could not find usable videos for prompt {prompt=}")

    def _generate_raw_video(self, prompt: str) -> List[PexelVideo]:
        """Finds a few videos on pexels that matches the prompt and meets the size requirements."""
        # TODO: This currently is optimized for the 1080x1920 size
        response = requests.get(
            "https://api.pexels.com/videos/search?"
            + urlencode(
                {
                    "query": prompt,
                    "per_page": 2,
                    "page": random.randint(1, 10),
                    "size": "medium",
                    "locale": "en-US",
                    "orientation": "portrait",
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


class DummyGenerator(ImageGenerator):
    """A dummy generator that just returns the same frame every time, for
    testing purposes.
    """

    def __init__(self, width: int, height: int, frame: Frame) -> None:
        self.width = width
        self.height = height
        self.frame = frame

    def generate(self, prompt: str, folder: str) -> Frame:
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

    def generate(self, prompt: str, folder: str) -> Frame:
        inner_frame = self.wrapped.generate(prompt, folder)

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
            str(inner_frame.framerate),
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

        return Frame(
            path=outpath,
            style="video",
            framerate=inner_frame.framerate,
            duration=inner_frame.duration,
        )

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


GENERATOR_CLASSES_BY_NAME: Dict[str, Type[ImageGenerator]] = {
    "dall-e": DalleImageGenerator,
    "stable-diffusion": StableDiffusionImageGenerator,
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


def create_image_generator(
    width: int,
    height: int,
    *,
    model: Optional[
        Literal["stable-diffusion", "dall-e", "pexels", "pexels-video"]
    ] = None,
) -> ImageGenerator:
    """Produces the standard image generator for generating images of the given
    width and height, based on what is actually installed.
    """
    if model is None:
        model = "stable-diffusion" if HAVE_STABLE_DIFFUSION else "dall-e"

    if model == "pexels":
        return DarkenTopAndBottomImageGenerator(PexelsImageGenerator(width, height))

    if model == "pexels-video":
        return DarkenTopAndBottomImageGenerator(
            PexelsVideosImageGenerator(width, height)
        )

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
        generator = BlurredBackgroundImageResizer(generator, width, height)

    return DarkenTopAndBottomImageGenerator(generator)


def generate_image_from_prompt(prompt: str, width: int, height: int, out: str) -> None:
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
    create_image_generator(width, height).generate(prompt).save(out, format="webp")


def create_images(
    image_descriptions: ImageDescriptions,
    width: int,
    height: int,
    *,
    folder: str,
    min_image_duration: float = 3.0,
    std_image_duration: float = 4.0,
    max_image_duration: float = 5.0,
    max_retries: int = 5,
    model: Optional[
        Literal["stable-diffusion", "dall-e", "pexels", "pexels-video"]
    ] = None,
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

        prompt = random.choice(list(prompts_remaining_in_segment))
        prompts_remaining_in_segment.remove(prompt)

        for attempt in range(max_retries):
            try:
                frame = generator.generate(prompt, folder)
                break
            except Exception as e:
                logging.info(
                    f"Failing to generate image (attempt {attempt+1}/{max_retries})"
                )
                if attempt == max_retries - 1:
                    raise ImagesError(
                        f"Failed to generate image after {max_retries} attempts"
                    ) from e

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
            image_duration = min(image_duration, frame.duration)

            # 4. don't go past the end of the segment
            image_duration = min(image_duration, time_until_next_segment)

        next_image_starts_at += image_duration

    return Images(image_descriptions=image_descriptions, prompts=prompts, images=images)
