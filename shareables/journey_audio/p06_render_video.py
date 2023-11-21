"""Renders the final video"""
import io
from typing import Dict, List, Literal, Optional, Tuple
import numpy as np
from PIL import Image, ImageFont, ImageDraw
import pympanim.frame_gen as fg
import pympanim.worker as pmaw
from temp_files import temp_file
import psutil
import textwrap


def render_video(
    background_image_path: str,
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
        background_image_path (str): The path to the background image, matching
            the dimensions of the video. May not visually look correct except at
            the tested 1080x1920 resolution.
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
            background_image_path,
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
        background_image_path: str,
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

        self.background_image_path: str = background_image_path
        """The path to the background image in the video dimensions"""

        self.audio_visualization_path: str = audio_visualization_path
        """The audio visualization from step 6, as (n_frames, n_bins) float64,
        as a path to a file which can be loaded using np.load
        """

        self.framerate: int = framerate
        """The framerate of the video, in frames per second."""

        self._duration: float = duration
        """The duration of the video in seconds, required as the audio visualization
        may not be ready when this is requested
        """

        self._frame_size: Tuple[int, int] = frame_size
        """The size of the video, in pixels. Must match the background image"""

        self.fonts: Optional[Dict[FontName, ImageFont.FreeTypeFont]] = None
        """If font's have been loaded, the loaded fonts. Otherwise None."""

        self.background_image: Optional[Image.Image] = None
        """If the background image has been loaded on this process, the loaded
        image. Otherwise None."""

        self.audio_visualization: Optional[np.ndarray] = None
        """If the audio visualization has been loaded on this process, the loaded
        visualization. Otherwise None."""

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
                layout_engine=ImageFont.Layout.BASIC,
            ),
            "300 44px Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_light_raw),
                size=44,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.Layout.BASIC,
            ),
            "300 44px italic Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_light_italic_raw),
                size=44,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.Layout.BASIC,
            ),
        }

        self.background_image = Image.open(self.background_image_path)
        self.audio_visualization = np.load(self.audio_visualization_path)

    def generate_at(self, time_ms):
        return fg.img_to_bytes(self.generate_at_pil(time_ms))

    def generate_at_pil(self, time_ms: float) -> Image.Image:
        assert self.background_image is not None
        assert self.fonts is not None
        assert self.audio_visualization is not None
        frame_index = round((time_ms * self.framerate) / 1000)

        result = self.background_image.copy()
        draw = ImageDraw.Draw(result)

        y = 160
        for idx, line in enumerate(self.titleLines):
            if idx > 0:
                y += 80

            draw.text(
                (104, y),
                line,
                fill=(255, 255, 255),
                font=self.fonts["300 72px Open Sans"],
            )

        y += 114
        draw.text(
            (104, y),
            f"with {self.instructor_name}",
            fill=(255, 255, 255),
            font=self.fonts["300 44px italic Open Sans"],
        )
        y += 391 - 274
        draw.line((80, y, 1000, y), fill=(255, 255, 255, 125), width=1)

        num_lines = self.audio_visualization.shape[1]
        total_width = (num_lines * BAR_WIDTH) + ((num_lines - 1) * BAR_SPACING)

        start_x = (self.frame_size[0] - total_width) // 2

        for i in range(num_lines):
            bar_height = int(
                self.audio_visualization[frame_index, i] * BAR_MAX_HEIGHT_PX
            )
            x = start_x + (i * (BAR_WIDTH + BAR_SPACING))
            y = (self.frame_size[1] - bar_height) // 2
            draw.rectangle(
                (x, y, x + BAR_WIDTH, y + bar_height),
                fill=(255, 255, 255),
            )

        draw.text(
            (102, 1729),
            "My daily #oseh",
            fill=(255, 255, 255),
            font=self.fonts["300 44px Open Sans"],
        )

        return result
