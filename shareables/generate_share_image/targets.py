from typing import List, Sequence, Tuple

from images import ImageTarget


RESOLUTIONS = list(
    dict.fromkeys(
        [
            # 1x
            (600, 315),
            # 2x
            (1200, 630),
            # legacy sharers
            (400, 300),
            # linkedin and other thumbnails
            (80, 150),
            # imessage
            (600, 600),
        ]
    )
)
"""The typical resolutions for open graph meta images"""


def _share_image_jpeg_targets(w: int, h: int) -> List[ImageTarget]:
    if w * h < 300 * 300:
        return []

    return [
        ImageTarget(
            required=True,
            width=w,
            height=h,
            format="jpeg",
            quality_settings={"quality": 95, "optimize": True, "progressive": True},
        )
    ]


def _share_image_png_targets(w: int, h: int) -> List[ImageTarget]:
    if w * h >= 300 * 300:
        return []

    return [
        ImageTarget(
            required=True,
            width=w,
            height=h,
            format="png",
            quality_settings={"optimize": True},
        )
    ]


def _make_share_image_targets(
    resolutions: Sequence[Tuple[int, int]]
) -> List[ImageTarget]:
    result = []
    for w, h in resolutions:
        result.extend(_share_image_jpeg_targets(w, h))
        result.extend(_share_image_png_targets(w, h))
    return result


TARGETS = _make_share_image_targets(RESOLUTIONS)
"""The typical targets for open graph meta images"""
