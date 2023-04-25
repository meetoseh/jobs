import base64
from dataclasses import dataclass
import random
from typing import Dict, List, Literal, Optional, Set, Tuple, Type
from abc import ABCMeta, abstractmethod
import torch
from shareables.journey_audio_with_dynamic_background.p06_transcript import Timestamp
from shareables.journey_audio_with_dynamic_background.p07_image_descriptions import (
    ImageDescriptions,
)
from shareables.shareable_pipeline_exception import ShareablePipelineException

try:
    from diffusers import StableDiffusionPipeline, DPMSolverMultistepScheduler

    HAVE_STABLE_DIFFUSION = True
except ImportError:
    HAVE_STABLE_DIFFUSION = False
from PIL import Image, ImageFilter, ImageEnhance
import openai
import os
import io
import logging
import time


class ImagesError(ShareablePipelineException):
    def __init__(self, message: str):
        super().__init__(message, 8, "images")


MODEL_CAPABLE_IMAGE_SIZES: Dict[str, List[Tuple[int, int]]] = {
    "dall-e": ((256, 256), (512, 512), (1024, 1024)),
    "stable-diffusion": ((768, 768),),
}


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

    images: List[Tuple[float, str]]
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
        for idx, (visible_at, image_path) in enumerate(self.images):
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

            print(f'   {visible_at} - "{self.prompts[idx]}" -> {image_path}')


class ImageGenerator(metaclass=ABCMeta):
    width: int
    """The width of the images to generate. Readonly"""

    height: int
    """The height of the images to generate. Readonly"""

    @abstractmethod
    def generate(self, prompt: str) -> Image.Image:
        """Generates a new image using the given prompt, at the width and
        height that this generator is capable of.
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

    def generate(self, prompt: str) -> Image.Image:
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
        return Image.open(
            io.BytesIO(base64.b64decode(bytes(response.data[0].b64_json, "utf-8")))
        )


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

    def generate(self, prompt: str) -> Image.Image:
        return self.pipe(prompt).images[0]


GENERATOR_CLASSES_BY_NAME: Dict[str, Type[ImageGenerator]] = {
    "dall-e": DalleImageGenerator,
    "stable-diffusion": StableDiffusionImageGenerator,
}


class BlurredBackgroundImageResizer(ImageGenerator):
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

    def generate(self, prompt: str) -> Image:
        core = self.wrapped.generate(prompt)

        if core.width == self.width and core.height == self.height:
            return core

        out = core.resize((self.width, self.height), Image.NEAREST)
        out = out.filter(ImageFilter.GaussianBlur(15))
        out = ImageEnhance.Brightness(out).enhance(0.6)

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
    model: Optional[Literal["stable-diffusion", "dall-e"]] = None,
) -> ImageGenerator:
    """Produces the standard image generator for generating images of the given
    width and height, based on what is actually installed.
    """
    if model is None:
        model = "stable-diffusion" if HAVE_STABLE_DIFFUSION else "dall-e"

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

    return generator


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
    model: Optional[Literal["stable-diffusion", "dall-e"]] = None,
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
            each image for.

    Returns:
        Images: The images to display, broken down by when they are to be displayed.

    Raises:
        ImagesError: If there is a problem generating the images.
    """
    generator = create_image_generator(width, height, model=model)

    images: List[Tuple[float, str]] = []
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
                image = generator.generate(prompt)
                break
            except Exception as e:
                logging.info(
                    f"Failing to generate image (attempt {attempt+1}/{max_retries})"
                )
                if attempt == max_retries - 1:
                    raise ImagesError(
                        f"Failed to generate image after {max_retries} attempts"
                    ) from e

        image_path = os.path.join(
            folder,
            f"{next_image_starts_at:.3f}.webp",
        )
        image.save(image_path, format="webp")

        prompts.append(prompt)
        images.append((next_image_starts_at, image_path))
        next_image_starts_at += image_duration

    return Images(image_descriptions=image_descriptions, prompts=prompts, images=images)
