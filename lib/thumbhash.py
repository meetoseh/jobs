# COPIED FROM https://github.com/justinforlenza/thumbhash-py
# WITH https://github.com/justinforlenza/thumbhash-py/issues/1 resolved
# and typing/static analysis improvements

import math
from typing import List, Tuple, Union
from pathlib import Path
from itertools import chain
from PIL import Image, ImageOps


def rgba_to_thumb_hash(w: int, h: int, rgba: List[int]) -> List[int]:
    """
    Encodes an RGBA image to a ThumbHash
    """
    if w > 100 or h > 100:
        raise ValueError(f"{w}x{h} doesn't fit in 100x100")

    avg_r, avg_g, avg_b, avg_a = 0, 0, 0, 0

    for i in range(w * h):
        j = i * 4
        alpha = rgba[j + 3] / 255
        avg_r += alpha / 255 * rgba[j]
        avg_g += alpha / 255 * rgba[j + 1]
        avg_b += alpha / 255 * rgba[j + 2]
        avg_a += alpha

    if avg_a:
        avg_r /= avg_a
        avg_g /= avg_a
        avg_b /= avg_a

    has_alpha = avg_a < w * h

    l_limit = 5 if has_alpha else 7
    lx = max(1, round(l_limit * w / max(w, h)))
    ly = max(1, round(l_limit * h / max(w, h)))
    l, p, q, a = [], [], [], []

    for i in range(w * h):
        j = i * 4
        alpha = rgba[j + 3] / 255
        r = avg_r * (1 - alpha) + alpha / 255 * rgba[j]
        g = avg_g * (1 - alpha) + alpha / 255 * rgba[j + 1]
        b = avg_b * (1 - alpha) + alpha / 255 * rgba[j + 2]
        l.append((r + g + b) / 3)
        p.append((r + g) / 2 - b)
        q.append(r - g)
        a.append(alpha)

    def encode_channel(channel: List[Union[int, float]], nx: int, ny: int):
        dc = 0
        ac = []
        scale = 0
        fx: List[Union[int, float]] = [0] * w

        for cy in range(ny):
            cx = 0
            while cx * ny < nx * (ny - cy):
                f = 0.0
                for x in range(w):
                    fx[x] = math.cos(math.pi / w * cx * (x + 0.5))
                for y in range(h):
                    fy = math.cos(math.pi / h * cy * (y + 0.5))
                    for x in range(w):
                        f += channel[x + y * w] * fx[x] * fy
                f /= w * h
                if cx > 0 or cy > 0:
                    ac.append(f)
                    scale = max(scale, abs(f))
                else:
                    dc = f
                cx += 1
        if scale:
            for i in range(len(ac)):
                ac[i] = 0.5 + 0.5 / scale * ac[i]
        return dc, ac, scale

    l_dc, l_ac, l_scale = encode_channel(l, max(3, lx), max(3, ly))
    p_dc, p_ac, p_scale = encode_channel(p, 3, 3)
    q_dc, q_ac, q_scale = encode_channel(q, 3, 3)
    a_dc, a_ac, a_scale = encode_channel(a, 5, 5) if has_alpha else (1.0, [], 1.0)

    is_landscape = w > h
    header24 = (
        round(63 * l_dc)
        | (round(31.5 + 31.5 * p_dc) << 6)
        | (round(31.5 + 31.5 * q_dc) << 12)
        | (round(31 * l_scale) << 18)
        | (has_alpha << 23)
    )
    header16 = (
        (ly if is_landscape else lx)
        | (round(63 * p_scale) << 3)
        | (round(63 * q_scale) << 9)
        | (is_landscape << 15)
    )
    thumb_hash = [
        header24 & 255,
        (header24 >> 8) & 255,
        header24 >> 16,
        header16 & 255,
        header16 >> 8,
    ]

    is_odd = False

    if has_alpha:
        thumb_hash.append(round(15 * a_dc) | (round(15 * a_scale) << 4))

    for ac in [l_ac, p_ac, q_ac]:
        for f in ac:
            u = int(round(15.0 * f))
            if is_odd:
                thumb_hash[-1] |= u << 4
            else:
                thumb_hash.append(u)
            is_odd = not is_odd

    if has_alpha:
        for f in a_ac:
            u = int(round(15.0 * f))
            if is_odd:
                thumb_hash[-1] |= u << 4
            else:
                thumb_hash.append(u)
            is_odd = not is_odd

    return thumb_hash


def image_to_thumb_hash(fp: str | bytes | Path) -> List[int]:
    """
    Opens given image file and encodes to a ThumbHash
    """
    img = Image.open(fp)

    img = img.convert("RGBA")
    img.thumbnail((100, 100))

    img = ImageOps.exif_transpose(img)

    rgba_2d = list(img.getdata())

    rgba = list(chain(*rgba_2d))

    thumb_hash = rgba_to_thumb_hash(img.width, img.height, rgba)

    return thumb_hash


def thumb_hash_to_average_rgba(
    thumb_hash: List[int],
) -> Tuple[float, float, float, float]:
    """
    Extracts the average color from a ThumbHash
    """
    if len(thumb_hash) < 5:
        raise ValueError("ThumbHash is too short")

    header = thumb_hash[0] | (thumb_hash[1] << 8) | (thumb_hash[2] << 16)
    l = (header & 63) / 63.0
    p = ((header >> 6) & 63) / 31.5 - 1.0
    q = ((header >> 12) & 63) / 31.5 - 1.0
    has_alpha = (header >> 23) != 0
    a = (thumb_hash[5] & 15) / 15.0 if has_alpha else 1.0
    b = l - 2.0 / 3.0 * p
    r = (3.0 * l - b + q) / 2.0
    g = r - q

    return (max(0.0, min(1.0, r)), max(0.0, min(1.0, g)), max(0.0, min(1.0, b)), a)


def thumb_hash_to_approximate_aspect_ratio(thumb_hash: List[int]) -> float:
    """
    Extracts approxmiate aspect ratio of a ThumbHash
    """
    if len(thumb_hash) < 5:
        raise ValueError("ThumbHash is too short")

    has_alpha = (thumb_hash[2] & 0x80) != 0
    l_max = 5 if has_alpha else 7
    l_min = thumb_hash[3] & 7
    is_landscape = (thumb_hash[4] & 0x80) != 0
    lx = l_max if is_landscape else l_min
    ly = l_min if is_landscape else l_max

    return lx / ly


def thumb_hash_to_rgba(thumb_hash: List[int]) -> Tuple[int, int, List[int]]:
    header24 = thumb_hash[0] | (thumb_hash[1] << 8) | (thumb_hash[2] << 16)
    header16 = thumb_hash[3] | (thumb_hash[4] << 8)
    l_dc = (header24 & 63) / 63
    p_dc = ((header24 >> 6) & 63) / 31.5 - 1
    q_dc = ((header24 >> 12) & 63) / 31.5 - 1
    l_scale = ((header24 >> 18) & 31) / 31
    has_alpha = header24 >> 23
    p_scale = ((header16 >> 3) & 63) / 63
    q_scale = ((header16 >> 9) & 63) / 63
    is_landscape = header16 >> 15
    lx = max(3, (5 if has_alpha else 7) if is_landscape else header16 & 7)
    ly = max(3, header16 & 7 if is_landscape else (5 if has_alpha else 7))
    a_dc = (thumb_hash[5] & 15) / 15 if has_alpha else 1
    a_scale = (thumb_hash[5] >> 4) / 15

    ac_start = 6 if has_alpha else 5
    ac_index = 0

    def decode_channel(nx, ny, scale):
        nonlocal ac_index

        ac: List[float] = []
        cy = 0
        while cy < ny:
            cx = 0 if cy else 1
            while cx * ny < nx * (ny - cy):
                ac.append(
                    (
                        (
                            (
                                thumb_hash[ac_start + (ac_index >> 1)]
                                >> ((ac_index & 1) << 2)
                            )
                            & 15
                        )
                        / 7.5
                        - 1
                    )
                    * scale
                )
                ac_index += 1
                cx += 1
            cy += 1
        return ac

    l_ac = decode_channel(lx, ly, l_scale)
    p_ac = decode_channel(3, 3, p_scale * 1.25)
    q_ac = decode_channel(3, 3, q_scale * 1.25)
    a_ac = decode_channel(5, 5, a_scale) if has_alpha else None

    ratio = thumb_hash_to_approximate_aspect_ratio(thumb_hash)

    print(ratio)

    w = 32 if ratio > 1 else round(32 * ratio)
    h = 32 if ratio <= 1 else round(32 / ratio)

    rgba = [0] * (w * h * 4)
    fx, fy = [], []

    y = 0
    i = 0

    while y < h:
        x = 0
        while x < w:
            l = l_dc
            p = p_dc
            q = q_dc
            a = a_dc
            for cx in range(max(lx, 5 if has_alpha else 3)):
                fx.append(math.cos(math.pi / w * (x + 0.5) * cx))
            for cy in range(max(ly, 5 if has_alpha else 3)):
                fy.append(math.cos(math.pi / h * (y + 0.5) * cy))

            j = 0
            cy = 0
            while cy < ly:
                cx = 0 if cy else 1
                fy2 = fy[cy] * 2
                while cx * ly < lx * (ly - cy):
                    l += l_ac[j] * fx[cx] * fy2

                    cx += 1
                    j += 1
                cy += 1

            j = 0
            for cy in range(3):
                fy2 = fy[cy] * 2
                for cx in range(0 if cy else 1, 3 - cy):
                    f = fx[cx] * fy2
                    p += p_ac[j] * f
                    q += q_ac[j] * f
                    j += 1

            if a_ac is not None:
                j = 0
                for cy in range(5):
                    fy2 = fy[cy] * 2
                    for cx in range(0 if cy else 1, 5 - cy):
                        a += a_ac[j] * fx[cx] * fy2
                        j += 1

            # Convert to RGB
            b = l - 2 / 3 * p
            r = (3 * l - b + q) / 2
            g = r - q
            rgba[i] = max(0, min(255, round(255 * r)))
            rgba[i + 1] = max(0, min(255, round(255 * g)))
            rgba[i + 2] = max(0, min(255, round(255 * b)))
            rgba[i + 3] = max(0, min(255, round(255 * a)))

            x += 1
            i += 4

        y += 1

    return (w, h, rgba)
