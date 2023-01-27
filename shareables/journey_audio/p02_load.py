"""The second step of this process, where we load the audio into memory as
a numpy array. 

Note that there is no implicit scale for the audio data. 
"""
import numpy as np


def load(path: str, dtype: np.dtype) -> np.ndarray:
    """Loads the normalized file produced from the first step as a numpy array."""
    return np.fromfile(path, dtype=dtype)
