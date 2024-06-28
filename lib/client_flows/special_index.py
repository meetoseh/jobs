from enum import Enum, auto


class SpecialIndex(Enum):
    """Fixed values for working with non-string indices"""

    ARRAY_INDEX = auto()
    """Means that any valid array index at this part matches"""
