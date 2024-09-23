"""Renders the final video"""

import io
from typing import Dict, List, Literal, Optional, Tuple
import numpy as np
from PIL import Image, ImageFont, ImageDraw
import pympanim.frame_gen as fg
import pympanim.worker as pmaw
from lib.ease import ease
from lib.transcripts.model import TimeRange, Timestamp, Transcript
from shareables.journey_audio.settings import TITLE_PAUSE_TIME_SECONDS
from temp_files import temp_file
import psutil
import textwrap
import cairosvg


TITLE_CROSSFADE_TIME_SECONDS = 0.5
TITLE_BRANDMARK_SPACER = 100
TITLE_LINE_HEIGHT = 108
TITLE_INSTRUCTOR_SPACER = 24
TITLE_INSTRUCTOR_LINE_HEIGHT = 75

CC_FADE_TIME_SECONDS = 0.5
CC_SHOW_EARLY_SECONDS = CC_FADE_TIME_SECONDS
CC_HOLD_LATE_SECONDS = 3 + CC_FADE_TIME_SECONDS
CC_MAX_ADJUST = CC_HOLD_LATE_SECONDS + 1
CC_LINE_HEIGHT = 72
CC_PHRASE_SPACER_HEIGHT = 24

CC_AUDIO_VIZ_SPACER = 100


def render_video(
    background_image_path: str,
    title: str,
    instructor_name: str,
    audio_visualization: np.ndarray,
    destination_path: str,
    framerate: int,
    width: int,
    height: int,
    transcript: Transcript,
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
        transcript (Transcript): The transcript of the audio

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
            duration + TITLE_PAUSE_TIME_SECONDS,
            (width, height),
            transcript,
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
                ooo_balance=10,
                ooo_cap=50,
                ooo_error=500,
            ),
        )


FontName = Literal[
    "400 90px Open Sans", "400 55px Open Sans", "400 50px italic Open Sans"
]


BAR_MAX_HEIGHT_PX = 226
BAR_WIDTH = 4
BAR_SPACING = 5


class MyFrameGenerator(fg.FrameGenerator):
    """A single frame generator for the entire video; we could upgrade
    this to something more structured if we're going to have more than
    a handful of animations
    """

    def __init__(
        self,
        title: str,
        instructor_name: str,
        background_image_path: str,
        audio_visualization_path: str,
        framerate: int,
        duration: float,
        frame_size: Tuple[int, int],
        transcript: Transcript,
    ) -> None:
        """Initializes a new frame generator, which is trivially pickleable until it's
        used at which point it cannot be safely pickled.
        """

        self.title: str = title
        """The title of the journey the video is for"""

        self.titleLines: List[str] = textwrap.wrap(self.title, width=20)
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

        self._transcript = adjust_transcript_for_display(
            transcript,
            show_early_seconds=CC_SHOW_EARLY_SECONDS,
            hold_late_seconds=CC_HOLD_LATE_SECONDS,
            max_adjust=CC_MAX_ADJUST,
        )
        """The transcript to use for closed captioning"""

        self._transcript_block_height = compute_transcript_block_height(
            self._transcript,
            line_height=CC_LINE_HEIGHT,
            phrase_spacer_height=CC_PHRASE_SPACER_HEIGHT,
            show_early_seconds=CC_SHOW_EARLY_SECONDS,
            hold_late_seconds=CC_HOLD_LATE_SECONDS,
        )
        """The height of the transcript at its tallest point, in pixels"""

        self.fonts: Optional[Dict[FontName, ImageFont.FreeTypeFont]] = None
        """If font's have been loaded, the loaded fonts. Otherwise None."""

        self.background_image: Optional[Image.Image] = None
        """If the background image has been loaded on this process, the loaded
        image. Otherwise None."""

        self.audio_visualization: Optional[np.ndarray] = None
        """If the audio visualization has been loaded on this process, the loaded
        visualization. Otherwise None."""

        self.brandmark: Optional[Image.Image] = None
        """If the brandmark has been loaded on this process, the loaded brandmark.
        Otherwise, None.
        
        Loaded at 85px width, 82px height
        """

        self.wordmark: Optional[Image.Image] = None
        """If the wordmark has been loaded on this process, the loaded wordmark.
        Otherwise, None.

        Loaded at 171px width, 40px height
        """

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
        with open("assets/OpenSans-Regular.ttf", "rb") as f:
            open_sans_regular_raw = f.read()

        with open("assets/OpenSans-Italic.ttf", "rb") as f:
            open_sans_regular_italic_raw = f.read()

        self.fonts = {
            "400 90px Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_regular_raw),
                size=90,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.Layout.BASIC,
            ),
            "400 55px Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_regular_raw),
                size=55,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.Layout.BASIC,
            ),
            "400 50px italic Open Sans": ImageFont.truetype(
                font=io.BytesIO(open_sans_regular_italic_raw),
                size=50,
                index=0,
                encoding="unic",
                layout_engine=ImageFont.Layout.BASIC,
            ),
        }

        self.background_image = Image.open(self.background_image_path)
        self.background_image.load()

        if self.background_image.mode != "RGBA":
            self.background_image = self.background_image.convert("RGBA")

        self.audio_visualization = np.load(self.audio_visualization_path)

        with temp_file(".png") as brandmark_filepath:
            cairosvg.svg2png(
                url="assets/Oseh_Brandmark_White.svg",
                write_to=brandmark_filepath,
                output_width=85,
                output_height=82,
                background_color="transparent",
            )
            self.brandmark = Image.open(brandmark_filepath, formats=["png"])
            self.brandmark.load()

        with temp_file(".png") as wordmark_filepath:
            cairosvg.svg2png(
                url="assets/Oseh_Wordmark_White.svg",
                write_to=wordmark_filepath,
                output_width=171,
                output_height=40,
                background_color="transparent",
            )
            self.wordmark = Image.open(wordmark_filepath, formats=["png"])
            self.wordmark.load()

    def generate_at(self, time_ms):
        return fg.img_to_bytes(self.generate_at_pil(time_ms))

    def generate_at_pil(self, time_ms: float) -> Image.Image:
        assert self.background_image is not None
        assert self.fonts is not None
        assert self.audio_visualization is not None

        time_seconds = time_ms / 1000
        if time_seconds < TITLE_PAUSE_TIME_SECONDS - TITLE_CROSSFADE_TIME_SECONDS:
            return self._generate_title_screen(
                opacity=1.0,
                background=self.background_image.copy(),
            )
        elif time_seconds < TITLE_PAUSE_TIME_SECONDS:
            time_into_crossfade = TITLE_PAUSE_TIME_SECONDS - time_seconds
            opacity = ease(time_into_crossfade / TITLE_CROSSFADE_TIME_SECONDS)
            result = self._generate_title_screen(
                opacity=opacity,
                background=self.background_image.copy(),
            )
            return self._generate_closed_captioning_screen(
                opacity=1.0 - opacity,
                audio_time_ms=0,
                background=result,
            )

        audio_time_ms = time_ms - (TITLE_PAUSE_TIME_SECONDS * 1000)
        return self._generate_closed_captioning_screen(
            opacity=1.0,
            audio_time_ms=audio_time_ms,
            background=self.background_image.copy(),
        )

    def _generate_title_screen(
        self, opacity: float, background: Image.Image
    ) -> Image.Image:
        assert self.fonts is not None
        assert self.brandmark is not None

        title_block_height = len(self.titleLines) * TITLE_LINE_HEIGHT
        total_centered_height = (
            self.brandmark.height
            + TITLE_BRANDMARK_SPACER
            + title_block_height
            + TITLE_INSTRUCTOR_SPACER
            + TITLE_INSTRUCTOR_LINE_HEIGHT
        )

        total_centered_start_y = (self._frame_size[1] - total_centered_height) // 2
        y = total_centered_start_y

        result = (
            background
            if opacity == 1
            else Image.new("RGBA", self._frame_size, (0, 0, 0, 0))
        )
        result.paste(
            self.brandmark,
            ((self._frame_size[0] - self.brandmark.width) // 2, y),
            self.brandmark,
        )

        draw = ImageDraw.Draw(result)

        y += self.brandmark.height + TITLE_BRANDMARK_SPACER
        for line in self.titleLines:
            line_bbox = draw.textbbox(
                (0, 0), line, font=self.fonts["400 90px Open Sans"]
            )
            line_width = line_bbox[2] - line_bbox[0]
            draw.text(
                ((self._frame_size[0] - line_width) // 2, y),
                line,
                fill=(234, 234, 235),
                font=self.fonts["400 90px Open Sans"],
            )
            y += TITLE_LINE_HEIGHT

        y += TITLE_INSTRUCTOR_SPACER
        name_bbox = draw.textbbox(
            (0, 0), self.instructor_name, font=self.fonts["400 50px italic Open Sans"]
        )
        name_width = name_bbox[2] - name_bbox[0]
        draw.text(
            ((self._frame_size[0] - name_width) // 2, y),
            self.instructor_name,
            fill=(234, 234, 235),
            font=self.fonts["400 50px italic Open Sans"],
        )
        y += TITLE_INSTRUCTOR_LINE_HEIGHT
        if opacity != 1:
            new_result = result.copy()
            new_result.putalpha(0)
            result = Image.blend(new_result, result, opacity)
            result = Image.alpha_composite(background, result)

        return result

    def _generate_closed_captioning_screen(
        self, *, opacity: float, audio_time_ms: float, background: Image.Image
    ) -> Image.Image:
        audio_time_seconds = audio_time_ms / 1000
        audio_frame_index = (
            0 if audio_time_ms <= 0 else round((audio_time_ms * self.framerate) / 1000)
        )

        phrase_indices = self._transcript.find_phrases_at_time(
            audio_time_seconds,
            margin_early=CC_SHOW_EARLY_SECONDS,
            margin_late=CC_HOLD_LATE_SECONDS,
        )

        phrases: List[Tuple[str, float]] = []
        for phrase_index in phrase_indices:
            phrase_tr, phrase = self._transcript.phrases[phrase_index]

            phrase_dom_start = phrase_tr.start.in_seconds() - CC_SHOW_EARLY_SECONDS
            time_into_phrase_dom = audio_time_seconds - phrase_dom_start
            time_until_end_dom = (
                phrase_tr.end.in_seconds() + CC_HOLD_LATE_SECONDS - audio_time_seconds
            )

            if time_into_phrase_dom < CC_FADE_TIME_SECONDS:
                phrase_opacity = time_into_phrase_dom / CC_FADE_TIME_SECONDS
            elif time_until_end_dom < CC_FADE_TIME_SECONDS:
                phrase_opacity = time_until_end_dom / CC_FADE_TIME_SECONDS
            else:
                phrase_opacity = 1.0

            phrase_opacity = min(1.0, max(0.0, phrase_opacity))
            phrases.append((phrase, phrase_opacity))

        return self._generate_closed_captioning_screen_with_phrases(
            phrases=phrases,
            audio_time_frame_index=audio_frame_index,
            overall_opacity=opacity,
            background=background,
        )

    def _generate_closed_captioning_screen_with_phrases(
        self,
        *,
        phrases: List[Tuple[str, float]],
        audio_time_frame_index: int,
        overall_opacity: float,
        background: Image.Image,
    ) -> Image.Image:
        assert self.fonts is not None
        assert self.audio_visualization is not None
        assert self.wordmark is not None

        result = Image.new("RGBA", self._frame_size, (0, 0, 0, 0))
        result.paste(
            self.wordmark,
            ((self._frame_size[0] - self.wordmark.width) // 2, 50),
            self.wordmark,
        )

        draw = ImageDraw.Draw(result)

        # transcript grows from the bottom of its block
        total_centered_height = (
            self._transcript_block_height + CC_AUDIO_VIZ_SPACER + BAR_MAX_HEIGHT_PX
        )
        total_centered_start_y = (self._frame_size[1] - total_centered_height) // 2
        transcript_bottom_y = total_centered_start_y + self._transcript_block_height

        center_x = self._frame_size[0] // 2

        y = transcript_bottom_y
        transcript_font = self.fonts["400 55px Open Sans"]
        for phrase_index in range(len(phrases) - 1, -1, -1):
            phrase, phrase_opacity = phrases[phrase_index]
            lines = phrase.split("\n")

            phrase_top_y = y - (len(lines) * CC_LINE_HEIGHT)
            y = phrase_top_y
            for line in lines:
                line_bbox = draw.textbbox((0, 0), line, font=transcript_font)
                line_width = line_bbox[2] - line_bbox[0]
                draw.text(
                    (center_x - (line_width // 2), y),
                    line,
                    fill=(234, 234, 235, round(phrase_opacity * 255)),
                    font=transcript_font,
                )
                y += CC_LINE_HEIGHT
            y = phrase_top_y - CC_PHRASE_SPACER_HEIGHT

        audio_viz_center_y = (
            transcript_bottom_y + CC_AUDIO_VIZ_SPACER + (BAR_MAX_HEIGHT_PX // 2)
        )

        num_lines = self.audio_visualization.shape[1]
        total_width = (num_lines * BAR_WIDTH) + ((num_lines - 1) * BAR_SPACING)

        start_x = (self.frame_size[0] - total_width) // 2

        for i in range(num_lines):
            bar_height = int(
                self.audio_visualization[audio_time_frame_index, i] * BAR_MAX_HEIGHT_PX
            )
            x = start_x + (i * (BAR_WIDTH + BAR_SPACING))
            y = audio_viz_center_y - (bar_height // 2)
            draw.rectangle(
                (x, y, x + BAR_WIDTH, y + bar_height),
                fill=(255, 255, 255),
            )

        if overall_opacity != 1:
            new_result = result.copy()
            new_result.putalpha(0)
            result = Image.blend(new_result, result, overall_opacity)

        return Image.alpha_composite(background, result)


def adjust_transcript_for_display(
    transcript: Transcript,
    *,
    show_early_seconds: float,
    hold_late_seconds: float,
    max_adjust: float,
) -> Transcript:
    """Returns a modified version of the original transcript which will work better
    when rendered on the video.

    This adjusts the start and end times to reduce overlapping phrases and
    rewraps the text.
    """
    phrases = transcript.phrases
    if not phrases:
        return transcript

    simple_phrases: List[Tuple[float, float]] = [
        (tr.start.in_seconds(), tr.end.in_seconds()) for (tr, _) in phrases
    ]
    adjusted_phrases: List[Tuple[TimeRange, str]] = []

    for i in range(0, len(simple_phrases) - 1):
        dom_end_of_this_phrase = simple_phrases[i][1] + hold_late_seconds
        dom_start_of_next_phrase = simple_phrases[i + 1][0] - show_early_seconds
        adjusted_ends_at = simple_phrases[i][1]
        if (
            dom_end_of_this_phrase > dom_start_of_next_phrase
            and dom_end_of_this_phrase - dom_start_of_next_phrase < max_adjust
        ):
            adjusted_ends_at -= dom_end_of_this_phrase - dom_start_of_next_phrase
            if adjusted_ends_at < simple_phrases[i][0]:
                adjusted_ends_at = simple_phrases[i][0]

        adjusted_phrases.append(
            (
                TimeRange(
                    Timestamp.from_seconds(simple_phrases[i][0]),
                    Timestamp.from_seconds(adjusted_ends_at),
                ),
                "\n".join(
                    textwrap.wrap(phrases[i][1], width=32, fix_sentence_endings=True)
                ),
            )
        )
    adjusted_phrases.append(
        (
            phrases[-1][0],
            "\n".join(
                textwrap.wrap(phrases[-1][1], width=32, fix_sentence_endings=True)
            ),
        )
    )
    return Transcript(adjusted_phrases)


def compute_transcript_block_height(
    transcript: Transcript,
    *,
    line_height: int,
    phrase_spacer_height: int,
    show_early_seconds: float,
    hold_late_seconds: float,
) -> int:
    """Computes the maximum height required to render the given
    transcript, in pixels, using the given line height and spacing
    between difficult simultaneous phrases.
    """
    # PERF:
    #  This algorithm is O(n^2) and could be improved to O(n),
    #  however it would only matter if we had a lot of overlapping
    #  phrases

    max_height = 0

    for start_phrase_idx in range(0, len(transcript.phrases)):
        start_dom_ends_at = (
            transcript.phrases[start_phrase_idx][0].end.in_seconds() + hold_late_seconds
        )
        end_phrase_idx_excl = start_phrase_idx + 1
        while end_phrase_idx_excl < len(transcript.phrases):
            next_end_dom_starts_at = (
                transcript.phrases[end_phrase_idx_excl][0].end.in_seconds()
                - show_early_seconds
            )
            if next_end_dom_starts_at >= start_dom_ends_at:
                break
            end_phrase_idx_excl += 1

        phrases_height = 0
        for phrase_idx in range(start_phrase_idx, end_phrase_idx_excl):
            if phrase_idx > start_phrase_idx:
                phrases_height += phrase_spacer_height

            lines = transcript.phrases[phrase_idx][1].count("\n") + 1
            phrases_height += lines * line_height

        if phrases_height > max_height:
            max_height = phrases_height

    return max_height


if __name__ == "__main__":

    def _main():
        with temp_file(".npy") as audio_viz_path:
            audio_viz = np.random.rand(600, 16)
            np.save(audio_viz_path, audio_viz, allow_pickle=False, fix_imports=False)

            fg = MyFrameGenerator(
                "test title",
                "test instructor",
                "tmp/bknd2.jpeg",
                audio_viz_path,
                6,
                10,
                (1080, 1920),
                Transcript(
                    [
                        (
                            TimeRange(
                                Timestamp.from_seconds(0), Timestamp.from_seconds(10)
                            ),
                            "example phrase would go here",
                        )
                    ]
                ),
            )
            fg.start()
            img = fg.generate_at_pil(2925)
            img.show()

    _main()
