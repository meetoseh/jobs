"""Renders the final video"""
import io
from typing import Dict, List, Literal, Optional, Tuple
import numpy as np
from PIL import Image, ImageFont, ImageDraw
from shareables.journey_audio_with_dynamic_background.p06_transcript import Transcript
from shareables.journey_audio_with_dynamic_background.p08_images import Images
import pympanim.frame_gen as fg
import pympanim.worker as pmaw
from temp_files import temp_file
import psutil
import textwrap
import bisect


def render_video(
    transcript: Transcript,
    images: Images,
    title: str,
    instructor_name: str,
    audio_visualization: np.ndarray,
    destination_path: str,
    framerate: int,
    width: int,
    height: int,
) -> None:
    """Renders the video to the given path, with the given framerate. The
    framerate must match the framerate of the audio visualization in step 3.

    The resulting video is silent and needs to go through one more step for
    adding audio.

    Args:
        transcript (Transcript): The transcript for captioning
        images (Images): The images to display, with a 0.5s cross-fade between
            each image
        title (str): The title of the journey the video is for
        instructor_name (str): The name of the instructor
        audio_visualization (np.ndarray): The audio visualization from step 6,
            as (n_frames, n_bins) float64
        destination_path (str): The path to write the video to. mp4 file extension
        framerate (int): The framerate of the video, in frames per second. This
            will only look correct if it matches from step 3. The duration of
            the video is determined by the length of the audio visualization
            and the framerate
        width (int): The width of the video, in pixels
        height (int): The height of the video, in pixels

    Returns:
        None
    """
    with temp_file(ext=".npy") as audio_viz_path:
        np.save(
            audio_viz_path, audio_visualization, allow_pickle=False, fix_imports=False
        )

        duration = audio_visualization.shape[0] / framerate
        _fg = MyFrameGenerator(
            title,
            instructor_name,
            transcript,
            images,
            audio_viz_path,
            framerate,
            duration,
            (width, height),
        )
        pmaw.produce(
            _fg,
            fps=framerate,
            dpi=100,
            bitrate=-1,
            outfile=destination_path,
            settings=pmaw.PerformanceSettings(
                # pmaw doesn't expect less than 3 cpus, so we're using the default but ensuring
                # it uses at least 1 worker
                num_workers=max(psutil.cpu_count(logical=False) // 3, 1),
            ),
        )


FontName = Literal[
    "300 72px Open Sans", "300 44px Open Sans", "300 44px italic Open Sans"
]


BAR_MAX_HEIGHT_PX = 226
BAR_WIDTH = 4
BAR_SPACING = 5


class MyFrameGenerator(fg.FrameGenerator):
    def __init__(
        self,
        title: str,
        instructor_name: str,
        transcript: Transcript,
        images: Images,
        audio_visualization_path: str,
        framerate: int,
        duration: float,
        frame_size: Tuple[int, int],
    ) -> None:
        """Initializes a new frame generator, which is trivially pickleable until it's
        used at which point it cannot be safely pickled.
        """

        self.title: str = title
        """The title of the journey the video is for"""

        self.titleLines: List[str] = textwrap.wrap(self.title, width=28)
        """The title of the journey broken into lines"""

        self.instructor_name: str = instructor_name
        """The name of the instructor"""

        self.transcript: Transcript = transcript
        """The transcript for captioning"""

        self.images: Images = images
        """The images to display, with a 0.5s cross-fade between each image"""

        self.audio_visualization_path: str = audio_visualization_path
        """The audio visualization from step 6, as (n_frames, n_bins) float64,
        as a path to a file which can be loaded using np.load
        """

        self.framerate: int = framerate
        """The framerate of the video, in frames per second."""

        self.cross_fade_duration: float = 0.5
        """How long to cross-fade images for."""

        self._duration: float = duration
        """The duration of the video in seconds, required as the audio visualization
        may not be ready when this is requested
        """

        self._frame_size: Tuple[int, int] = frame_size
        """The size of the video, in pixels. Must match the background image"""

        self.fonts: Optional[Dict[FontName, ImageFont.ImageFont]] = None
        """If font's have been loaded, the loaded fonts. Otherwise None."""

        self.audio_visualization: Optional[np.ndarray] = None
        """If the audio visualization has been loaded on this process, the loaded
        visualization. Otherwise None."""

        self.phrase_time_offsets: Optional[List[float]] = None
        """If the phrase time offsets have been loaded on this process, the loaded
        offsets. Otherwise None."""

        self.image_time_offsets: Optional[List[float]] = None
        """If the image time offsets have been loaded on this process, the loaded
        offsets. Otherwise None."""

    @property
    def duration(self) -> float:
        """Duration in milliseconds"""
        return self._duration * 1000

    @property
    def frame_size(self) -> Tuple[int, int]:
        return self._frame_size

    def start(self) -> None:
        """Called when we're on the thread and process that is going to be used
        to generate frames
        """
        with open("assets/OpenSans-Light.ttf", "rb") as f:
            open_sans_light_raw = f.read()

        with open("assets/OpenSans-LightItalic.ttf", "rb") as f:
            open_sans_light_italic_raw = f.read()

        self.fonts = {
            "300 72px Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_light_raw),
                size=72,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.LAYOUT_BASIC,
            ),
            "300 44px Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_light_raw),
                size=44,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.LAYOUT_BASIC,
            ),
            "300 44px italic Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_light_italic_raw),
                size=44,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.LAYOUT_BASIC,
            ),
        }

        self.audio_visualization = np.load(self.audio_visualization_path)
        self.phrase_time_offsets = [
            tr.start.in_seconds() for (tr, _) in self.transcript.phrases
        ]
        self.image_time_offsets = [ts for (ts, _) in self.images.images]

    def generate_at(self, time_ms):
        return fg.img_to_bytes(self.generate_at_pil(time_ms))

    def generate_at_pil(self, time_ms: float) -> Image:
        frame_index = round((time_ms * self.framerate) / 1000)

        time_seconds = time_ms / 1000

        image_index = bisect.bisect_right(self.image_time_offsets, time_seconds) - 1
        time_into_image = time_seconds - self.image_time_offsets[image_index]
        time_until_next_image = (
            self.image_time_offsets[image_index + 1] - time_seconds
            if image_index + 1 < len(self.image_time_offsets)
            else None
        )

        from_img_index = image_index
        to_img_index = image_index

        if image_index > 0 and time_into_image < self.cross_fade_duration / 2:
            from_img_index -= 1

        if (
            time_until_next_image is not None
            and time_until_next_image < self.cross_fade_duration / 2
        ):
            to_img_index += 1

        if to_img_index - from_img_index == 2:
            from_img_index += 1

        if from_img_index == to_img_index:
            result = Image.new("RGB", self.frame_size)
            bknd = self.images.images[from_img_index][1].get_image_at(time_into_image)
            result.paste(bknd, (0, 0))
            bknd.close()
        else:
            from_img_starts_at = self.images.images[from_img_index][0]
            to_img_starts_at = self.images.images[to_img_index][0]

            cross_fade_starts_at = (
                self.image_time_offsets[to_img_index] - self.cross_fade_duration / 2
            )
            cross_fade_progress = (
                time_seconds - cross_fade_starts_at
            ) / self.cross_fade_duration
            assert (
                0 <= cross_fade_progress <= 1
            ), f"{self.image_time_offsets=}, {to_img_index=}, {self.cross_fade_duration=}, {cross_fade_starts_at=}, {cross_fade_progress=}, {time_seconds=}, {time_into_image=}, {time_until_next_image=}, {image_index=}, {from_img_index=}, {to_img_index=}"

            time_into_from_img = min(
                to_img_starts_at - from_img_starts_at,
                time_seconds - cross_fade_starts_at,
            )
            time_into_to_img = max(0, time_seconds - to_img_starts_at)

            if cross_fade_progress == 0:
                bknd1 = self.images.images[from_img_index][1].get_image_at(
                    time_into_from_img
                )
                result = Image.new("RGB", self.frame_size)
                result.paste(bknd1, (0, 0))
                bknd1.close()
            elif cross_fade_progress == 1:
                bknd2 = self.images.images[to_img_index][1].get_image_at(
                    time_into_to_img
                )
                result = Image.new("RGB", self.frame_size)
                result.paste(bknd2, (0, 0))
                bknd2.close()
            else:
                bknd1 = self.images.images[from_img_index][1].get_image_at(
                    time_into_from_img
                )
                bknd2 = self.images.images[to_img_index][1].get_image_at(
                    time_into_to_img
                )

                result = Image.blend(bknd1, bknd2, cross_fade_progress)

                bknd1.close()
                bknd2.close()

        draw = ImageDraw.Draw(result)

        y = 160
        if self.frame_size[1] < 1920:
            y = 60

        for idx, line in enumerate(self.titleLines):
            if idx > 0:
                y += 80

            x = 104

            if self.frame_size == (1920, 1080):
                # center align
                bbox = draw.textbbox(
                    (0, 0), line, font=self.fonts["300 72px Open Sans"]
                )
                x = (self.frame_size[0] - bbox[2]) // 2

            draw.text(
                (x, y),
                line,
                fill=(255, 255, 255),
                font=self.fonts["300 72px Open Sans"],
            )

        y += 114
        subtitle = f"with {self.instructor_name}"
        x = 104

        if self.frame_size == (1920, 1080):
            # center align
            bbox = draw.textbbox(
                (0, 0), subtitle, font=self.fonts["300 44px italic Open Sans"]
            )
            x = (self.frame_size[0] - bbox[2]) // 2

        draw.text(
            (x, y),
            subtitle,
            fill=(255, 255, 255),
            font=self.fonts["300 44px italic Open Sans"],
        )
        y += 391 - 274

        draw.line(
            (80, y, self.frame_size[0] - 80, y), fill=(255, 255, 255, 125), width=1
        )

        num_lines = self.audio_visualization.shape[1]
        total_width = (num_lines * BAR_WIDTH) + ((num_lines - 1) * BAR_SPACING)

        start_x = (self.frame_size[0] - total_width) // 2
        center_y = self.frame_size[1] // 2
        bars_bottom_y = center_y + (BAR_MAX_HEIGHT_PX // 2)

        for i in range(num_lines):
            bar_height = int(
                self.audio_visualization[frame_index, i] * BAR_MAX_HEIGHT_PX
            )
            x = start_x + (i * (BAR_WIDTH + BAR_SPACING))
            y = center_y - (bar_height // 2)
            draw.rectangle(
                (x, y, x + BAR_WIDTH, y + bar_height),
                fill=(255, 255, 255),
            )

        daily_oseh_y = self.frame_size[1] - 191
        transcript_line_height = 60

        transcript_phrase_index = (
            bisect.bisect_right(self.phrase_time_offsets, time_seconds) - 1
        )
        caption = self.transcript.phrases[transcript_phrase_index][1]

        caption_wrapped = textwrap.wrap(caption, width=45)
        if self.frame_size == (1920, 1080):
            y = center_y + 60
        else:
            transcript_center_y = bars_bottom_y + 90
            transcript_height = (len(caption_wrapped) - 1) * transcript_line_height + (
                draw.textbbox(
                    (0, 0), caption_wrapped[-1], font=self.fonts["300 44px Open Sans"]
                )[3]
            )
            y = transcript_center_y - transcript_height // 2

        for line in caption_wrapped:
            line_bbox = draw.textbbox(
                (0, 0),
                line,
                font=self.fonts["300 44px Open Sans"],
            )

            draw.text(
                ((self._frame_size[0] - line_bbox[2]) // 2, y),
                line,
                fill=(255, 255, 255),
                font=self.fonts["300 44px Open Sans"],
            )
            y += transcript_line_height

        x = 104
        my_daily_oseh_text = "My daily #oseh"
        my_daily_oseh_font = self.fonts["300 44px Open Sans"]

        if self.frame_size == (1920, 1080):
            # center align
            bbox = draw.textbbox((0, 0), my_daily_oseh_text, font=my_daily_oseh_font)
            x = (self.frame_size[0] - bbox[2]) // 2

        draw.text(
            (x, daily_oseh_y),
            my_daily_oseh_text,
            fill=(255, 255, 255),
            font=my_daily_oseh_font,
        )

        return result
