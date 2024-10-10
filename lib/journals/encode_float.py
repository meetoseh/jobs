from typing import Union


def encode_float(v: Union[int, float]) -> str:
    """Encodes a float as string in a way that can be exactly replicated
    in javascript via

    return v.toLocaleString('en-US', {
        minimumFractionDigits: 3,
        maximumFractionDigits: 3,
        notation: 'standard',
        useGrouping: false,
    });
    """
    return f"{v:.3f}"
