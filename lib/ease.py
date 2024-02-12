from typing import List, Optional
import json

_ease4: Optional[List[int]] = None


def _ensure_ease4():
    global _ease4
    if _ease4 is None:
        with open("assets/ease4.json") as f:
            _ease4 = json.load(f)


def ease(x: float) -> float:
    """Returns the y-value of the standard cubic-bezier "ease" function at an x-value of t"""
    # PERF: I found no usable python bezier packages so I just use the
    # precomputed ones we have as a fallback for slow pcs in javascript;
    # it would not take very long to translate Bezier.ts to python if we
    # want more / more accurate easings

    approx_x = round(x * 1000)
    if approx_x <= 0:
        return 0
    if approx_x >= 1000:
        return 1
    _ensure_ease4()
    assert _ease4 is not None
    return _ease4[approx_x]
