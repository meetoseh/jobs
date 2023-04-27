"""Entry point for testing this pipeline"""

import argparse
import asyncio
import logging
import shutil
from typing import Literal, Optional
from shareables.journey_audio.p01_crop_and_normalize import crop_and_normalize
import os
import scipy.fft
from itgs import Itgs
from shareables.journey_audio.p02_load import load
from shareables.journey_audio.p03_sliding_window_repeated_fft import (
    sliding_window_repeated_fft,
)
from shareables.journey_audio.p04_partition_frequency import partition_frequency
from shareables.journey_audio.p05_bin_frames import bin_frames
from shareables.journey_audio_with_dynamic_background.p06_transcript import (
    create_transcript,
)
from shareables.journey_audio_with_dynamic_background.p07_image_descriptions import (
    create_image_descriptions,
)
from shareables.journey_audio_with_dynamic_background.p08_images import create_images
from shareables.journey_audio_with_dynamic_background.p09_render_video import (
    render_video,
)
from shareables.journey_audio.p07_add_audio import add_audio
from dataclasses import dataclass


def main():
    """Entry point for testing this pipeline.

    Execute with the following at the project root level:

    python -m shareables.journey_audio_with_dynamic_background.main --source <source audio file>
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="The path to the source audio file to use for this pipeline",
    )
    parser.add_argument(
        "--title",
        type=str,
        default="Journey Title",
        help="The title of the journey to use for this pipeline",
    )
    parser.add_argument(
        "--instructor",
        type=str,
        default="Instructor Name",
        help="The instructor name to use for this pipeline",
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="The duration to clip the audio to, in seconds, if clipping is desired",
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=["dall-e", "pexels", "pexels-video"],
        help="The model to use for image generation",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=1080,
        help="The width of the video to generate, in pixels",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=1920,
        help="The height of the video to generate, in pixels",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Configures the pipeline to use faster settings, for testing purposes",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)

    asyncio.run(
        run_pipeline(
            args.source,
            args.title,
            args.instructor,
            args.duration,
            args.model,
            width=args.width,
            height=args.height,
            fast=args.fast,
        )
    )


@dataclass
class RunPipelineResult:
    output_path: str
    """Where the output file was written to"""


async def run_pipeline(
    source: str,
    title: str,
    instructor: str,
    duration: Optional[int],
    model: Optional[Literal["dall-e", "pexels", "pexels-video"]],
    dest_folder: str = os.path.join(
        "tmp", "shareables", "journey_audio_with_dynamic_background"
    ),
    width: int = 1080,
    height: int = 1920,
    fast: bool = False,
) -> RunPipelineResult:
    """Runs the pipeline on the source audio file at the given location,
    storing the result in the given folder. This isn't properly asyncio
    most of the time, but does use it in parts.

    This is intended to be suitable both for testing (via the main function)
    and for use in jobs.
    """
    logging.info(
        f"shareables.journey_audio.main.run_pipeline({source=}, {dest_folder=})"
    )

    if not os.path.exists(source):
        logging.error(f"Source audio file not found at {source}")
        raise FileNotFoundError(f"Source audio file not found at {source}")

    os.makedirs(dest_folder, exist_ok=True)

    cropped_and_normalized_path = os.path.join(
        dest_folder, "cropped_and_normalized.raw"
    )
    if os.path.exists(cropped_and_normalized_path):
        logging.debug("Removing old cropped_and_normalized.raw")
        os.remove(cropped_and_normalized_path)

    try:
        res = crop_and_normalize(source, cropped_and_normalized_path, duration=duration)
    except Exception:
        logging.error("crop_and_normalize failed", exc_info=True)

        if os.path.exists(cropped_and_normalized_path):
            logging.debug("Partially written file available, dumping first 100 bytes")
            with open(cropped_and_normalized_path, "rb") as f:
                logging.debug(f.read(100))

        raise

    logging.debug(f"crop and normalize result: {res}")
    raw_audio = load(cropped_and_normalized_path, res.dtype)
    logging.debug(
        f"Loaded raw audio as a numpy array: {raw_audio.shape=}, {raw_audio.dtype=}, {raw_audio.min()=}, {raw_audio.max()=}, {raw_audio.mean()=}"
    )
    window_size = 16384
    fft_audio = sliding_window_repeated_fft(raw_audio, res.sample_rate, 60, window_size)
    logging.debug(
        f"FFT audio: {fft_audio.shape=}, {fft_audio.dtype=}, {fft_audio.min()=}, {fft_audio.max()=}, {fft_audio.mean()=}"
    )
    fft_audio_interpretation = scipy.fft.rfftfreq(window_size, 1 / res.sample_rate)
    logging.debug(f"FFT spans frequencies: {fft_audio_interpretation}")

    frequency_partition = partition_frequency(window_size, res.sample_rate)
    logging.debug(f"Frequency partition: {frequency_partition=}")

    # interpret frequency partition as frequency ranges using the fft interpretation
    interpreted_frequency_partition = [
        (fft_audio_interpretation[start], fft_audio_interpretation[end - 1])
        for start, end in zip(frequency_partition[:-1], frequency_partition[1:])
    ]
    logging.debug(
        f"Interpreted frequency partition: {interpreted_frequency_partition=}"
    )

    audio_visualization = bin_frames(fft_audio, frequency_partition)
    logging.debug(
        f"Audio visualization: {audio_visualization.shape=}, {audio_visualization.dtype=}, {audio_visualization.min()=}, {audio_visualization.max()=}, {audio_visualization.mean()=}"
    )

    transcript = create_transcript(source, duration=duration, instructor=instructor)
    logging.debug(f"Transcript:\n\n{transcript}")

    image_descriptions = create_image_descriptions(
        transcript, model=model, **({"min_seconds_per_image": 10} if fast else {})
    )
    logging.debug(f"Image descriptions:\n\n{image_descriptions}")

    images_folder = os.path.join(dest_folder, "images")
    if os.path.exists(images_folder):
        logging.debug("Removing old images folder")
        shutil.rmtree(images_folder)

    os.makedirs(images_folder, exist_ok=True)

    async with Itgs() as itgs:
        images = await create_images(
            image_descriptions,
            width,
            height,
            itgs=itgs,
            folder=images_folder,
            model=model,
            **(
                {
                    "min_image_duration": 10,
                    "max_image_duration": 30,
                    "std_image_duration": 20,
                    "max_retries": 2,
                }
                if fast
                else {}
            ),
        )

    video_only_path = os.path.join(dest_folder, "video_only.mp4")
    if os.path.exists(video_only_path):
        logging.debug("Removing old video_only.mp4")
        os.remove(video_only_path)

    render_video(
        transcript=transcript,
        images=images,
        title=title,
        instructor_name=instructor,
        audio_visualization=audio_visualization,
        destination_path=video_only_path,
        framerate=60,
        width=width,
        height=height,
    )
    logging.debug("Done rendering video without audio")

    final_path = os.path.join(
        dest_folder, f"final-{for_filepath(instructor)}-{for_filepath(title)}.mp4"
    )
    if os.path.exists(final_path):
        logging.debug("Removing old final.mp4")
        os.remove(final_path)

    add_audio(video_only_path, source, final_path, duration)
    logging.debug("Done adding audio to video")
    return RunPipelineResult(output_path=final_path)


def for_filepath(s: str) -> str:
    return s.replace(" ", "_")


if __name__ == "__main__":
    main()
