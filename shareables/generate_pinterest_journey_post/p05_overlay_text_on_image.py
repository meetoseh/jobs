import os
import secrets
from typing import List
from itgs import Itgs
from graceful_death import GracefulDeath
from shareables.generate_pinterest_journey_post.p02_create_text_content import (
    PinterestTextContentForPost,
)
from shareables.generate_pinterest_journey_post.p04_create_image import (
    PinterestRawImage,
)
from dataclasses import dataclass
from PIL import Image, ImageFont, ImageDraw
import io
import textwrap


@dataclass
class PinterestFinalImage:
    path: str
    """The path to the image file with the text overlaid on it"""

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


@dataclass
class TextWithSize:
    font: ImageFont.FreeTypeFont
    text: str
    width: int
    height: int


@dataclass
class OverlayTextWrapped:
    parts: List[List[TextWithSize]]
    """A list of each logical idea, with each idea text wrapped. We preserve
    newlines provided from the llm to produce the parts, and then textwrap
    each part to produce the TextWithSize objects. After this we're ready to
    determine where to place each part on the image.
    """


def wrap_text(
    font: ImageFont.FreeTypeFont, draw: ImageDraw.ImageDraw, text: str
) -> OverlayTextWrapped:
    """Determines the appropriate line wrapping for the given text and font.

    Args:
        font (ImageFont.FreeTypeFont): the font to use
        text (str): the text to wrap
    """
    parts = []
    for part in text.splitlines():
        part = part.strip()
        if not part:
            continue

        wrapped = textwrap.wrap(part, width=40)
        sizes = [draw.textbbox((0, 0), line, font=font) for line in wrapped]
        parts.append(
            [
                TextWithSize(
                    font=font,
                    text=line,
                    width=size[2] - size[0],
                    height=size[3] - size[1],
                )
                for line, size in zip(wrapped, sizes)
            ]
        )

    return OverlayTextWrapped(parts=parts)


@dataclass
class TextWithPosition:
    text: str
    font: ImageFont.FreeTypeFont
    x: int
    y: int


def determine_text_positions(
    wrapped: OverlayTextWrapped, width: int, height: int
) -> List[TextWithPosition]:
    """Positions the text within an image of the given width and height.

    Args:
        wrapped (OverlayTextWrapped): the text to position
        width (int): the width of the image, in pixels
        height (int): the height of the image, in pixels

    Returns:
        List[TextWithPosition]: the text with its position
    """
    line_height = 60
    part_separator = 40

    total_height = 0
    for part in wrapped.parts:
        total_height += (len(part) - 1) * line_height + part[-1].height + part_separator

    y = (height - total_height) // 2

    positions: List[TextWithPosition] = []
    for part in wrapped.parts:
        for line_idx, line in enumerate(part):
            x = (width - line.width) // 2
            positions.append(TextWithPosition(text=line.text, font=line.font, x=x, y=y))
            y += line_height if line_idx < len(part) - 1 else line.height
        y += part_separator

    return positions


async def overlay_text_on_image(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    text: PinterestTextContentForPost,
    image: PinterestRawImage,
    folder: str,
) -> PinterestFinalImage:
    """Overlays the text on the image and returns the final image.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): used for signal handling to stop early
        text (PinterestTextContentForPost): the text to overlay on the image
        image (PinterestRawImage): the raw image to overlay the text on
        folder (str): the folder to save the image to
    """
    with open("assets/OpenSans-Medium.ttf", "rb") as f:
        open_sans_medium_raw = f.read()

    font = ImageFont.truetype(
        font=io.BytesIO(open_sans_medium_raw),
        size=44,
        index=0,
        encoding="unic",
        layout_engine=ImageFont.Layout.BASIC,
    )

    img = Image.open(image.path)
    img.load()

    draw = ImageDraw.Draw(img)
    wrapped = wrap_text(font, draw, text.overlaid_text)
    positions = determine_text_positions(wrapped, img.width, img.height)

    for position in positions:
        draw.text((position.x, position.y), position.text, font=position.font)

    final_path = os.path.join(folder, f"final-{secrets.token_urlsafe(8)}.jpg")
    img.save(final_path, quality=90, optimize=True, progressive=True)
    return PinterestFinalImage(path=final_path)
