from typing import List
import numpy as np


def bin_frames(fft_audio: np.ndarray, frequency_partition: List[int]) -> np.ndarray:
    """Applies the given partition of the frequency space to the given
    (num_frames, num_frequencies) float64 array of frequency-space audio data
    to produce a (num_frames, num_bins) float64 array of frequency-bin amplitudes
    which can be rendered directly as a vertical bar graph in each frame. The
    amplitudes are scaled to be between 0 and 1.

    This is accomplished by summing summing the frequencies within each bin
    independently at each frame.

    Args:
        fft_audio (np.ndarray): The (num_frames, num_frequencies) float64 array produced by the sliding
            window repeated fft step.
        frequency_partition (List[int]): A list of integers with `num_bins+1` entries where
            entry 0 is 0 and entry[-1] is num_frequencies, in strictly increasing order. Bin
            1 corresponds to frequencies in the range [frequency_partition[0], frequency_partition[1]),
            etc.

    Returns:
        np.ndarray: The (num_frames, num_bins) float64 array of frequency-bin amplitudes.
    """
    result = np.zeros(
        (fft_audio.shape[0], len(frequency_partition) - 1), dtype=np.float64
    )

    first_relevant_frame = np.argmax(np.any(fft_audio > 0, axis=1))
    last_relevant_frame = np.subtract(
        fft_audio.shape[0], np.argmax(np.any(fft_audio[::-1] > 0, axis=1))
    )

    for i in range(len(frequency_partition) - 1):
        np.sum(
            fft_audio[:, frequency_partition[i] : frequency_partition[i + 1]],
            axis=1,
            out=result[:, i],
        )

    rel_result = result[first_relevant_frame:last_relevant_frame]
    np.subtract(rel_result, np.min(rel_result), out=rel_result)
    np.divide(rel_result, np.max(rel_result), out=rel_result)

    # reduce jitter
    current_vals = np.zeros((result.shape[1],), dtype=np.float64)
    for frame in range(result.shape[0]):
        current_vals = 0.85 * current_vals + 0.15 * result[frame, :]
        result[frame, :] = current_vals

    return result
