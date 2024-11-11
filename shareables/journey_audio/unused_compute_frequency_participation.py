"""The fourth step in the process, where we calculate the average participation for
each frequency.
"""

import numpy as np


def compute_frequency_participation(fft_audio: np.ndarray) -> np.ndarray:
    """Accepts the (num_frames, num_frequencies) float64 array produced by the sliding
    window repeated fft step and computes the (num_frequences,) float64 array of
    average participation for each frequency. The sum of the frequency participation
    is 1

    Args:
        fft_audio (np.ndarray): The (num_frames, num_frequencies) float64 array produced by the sliding
            window repeated fft step.

    Returns:
        np.ndarray: The (num_frequences,) float64 array of average participation for each frequency.
    """
    raw_participation = np.mean(fft_audio, axis=0)

    # taylor softmax; less aggressive than softmax, but still much easier
    # to interpret than the raw participation
    items = 1 + raw_participation + 0.5 * raw_participation**2
    return items / np.sum(items)
