"""Partitions the frequency space retrieved by the audio sliding window FFT"""

from typing import List
import scipy.fft
import numpy as np


# music settings: 750Hz, 23 bins


def partition_frequency(
    window_size: int,
    sample_rate: int,
    max_relevant_frequency: int = 477,
    min_bins: int = 13,
    max_bins: int = 19,
) -> List[int]:
    """Partitions the frequency space retrieved by the audio sliding window FFT
    into the bins that should be used for the audio visualization. The returned
    partition may be for only a subspace of the available frequencies.

    Args:
        window_size (int): The size of the sliding window used to compute the FFT.
            Must be a power of two, e.g., 16384. Typically higher sample rates
            necessitate larger window sizes to get the same resolution.
        sample_rate (int): The sample rate of the audio, e.g., 44100
        max_relevant_frequency (int): The maximum frequency that should be
            considered relevant. Frequencies above this value are ignored.
            Typically, 512.
        min_bins (int): the minimum number of bins to produce.
        max_bins (int): the maximum number of bins to produce.

    Returns:
        list[int]: A (partial) partition of the frequency space, specified as a
            list of indices within the frequency space, in strictly ascending
            order. This has length `num_bins + 1` where `num_bins` is the number
            of bins in the partition.
    """
    fft_audio_interpretation = scipy.fft.rfftfreq(window_size, 1 / sample_rate)
    max_relevant_frequency_index = np.searchsorted(
        fft_audio_interpretation, max_relevant_frequency, side="right"
    )
    num_relevant_frequencies = (
        max_relevant_frequency_index  # 1 to this index, inclusive
    )

    num_bins = (min_bins + max_bins) // 2
    if int(num_relevant_frequencies) % num_bins != 0:
        avg_bins = num_bins
        best_remainder = int(num_relevant_frequencies) % num_bins
        best_num_bins = num_bins

        offset = 1
        while best_remainder > 0:
            below_option = avg_bins - offset
            above_option = avg_bins + offset

            if below_option < min_bins and above_option > max_bins:
                break

            if below_option >= min_bins:
                below_remainder = int(num_relevant_frequencies) % below_option
                if below_remainder < best_remainder:
                    best_remainder = below_remainder
                    best_num_bins = below_option

            if above_option <= max_bins:
                above_remainder = int(num_relevant_frequencies) % above_option
                if above_remainder < best_remainder:
                    best_remainder = above_remainder
                    best_num_bins = above_option

            offset += 1

        num_bins = best_num_bins

    return list(
        range(
            1,
            int(num_relevant_frequencies) + 2,
            num_relevant_frequencies // num_bins,
        )
    )
