"""The third step in the process, where we convert from

(total_audio_samples,) -> (target_video_samples, window_size//2)

Typically, this is going from

44100 samples/second * 15 seconds * 4 bytes per sample = 2,646,000 bytes

assuming a window size of 16384 samples, since only 8192 samples are
required to represent the positive frequencies, and each sample is 4 bytes:

60 frames/second * 15 seconds * 8192 samples/frame * 8 bytes/sample = 58,982,400 bytes
"""

import numpy as np
import scipy.fft


def sliding_window_repeated_fft(
    raw_audio: np.ndarray, sample_rate: int, framerate: int, window_size: int
) -> np.ndarray:
    """Given raw_audio (total_audio_samples,) uint32 or uint64, where each sample was taken
    uniformly at sample_rate samples/second, and we want to produce a video at framerate
    frames/second, where each frame incorporates window_size samples of audio in its
    frequency-space representation (positive frequencies only, made real via the absolute
    value), this function returns a numpy array of shape (target_video_samples, window_size//2)
    where each row is the frequency-space representation of the corresponding window of
    audio.

    Conceptually, this is "neatest" when the window size is selected to partition the
    audio, though this is rarely the case in practice as it would be impossible to
    detect common frequencies in the audio if the window size is too small.

    Frames which are within the window size of the ends of the audio are zero-padded

    Args:
        raw_audio (np.ndarray): The raw audio data, as a numpy array of shape (total_audio_samples,)
        sample_rate (int): How many samples there are in a given second of audio. Typically, 44100
        framerate (int): How many frames there are in a given second of video. Typically, 60
        window_size (int): How many samples to include in each window. Typically, 16384. Must be power of 2

    Returns:
        np.ndarray: A numpy array of shape (target_video_samples, window_size//2) where each row is the
            frequency-space representation of the corresponding window of audio.
    """
    assert sample_rate > 0, "sample_rate must be positive"
    assert framerate > 0, "framerate must be positive"
    assert window_size > 0, "window_size must be positive"
    assert window_size % 2 == 0, "window_size must be even"
    assert window_size & (window_size - 1) == 0, "window_size must be a power of 2"

    half_window_size = window_size >> 1

    total_audio_samples = raw_audio.shape[0]
    total_video_samples = (framerate * total_audio_samples) // sample_rate

    result = np.zeros((total_video_samples, half_window_size), dtype=np.float64)

    frame_duration = 1 / framerate
    frame_time = 0
    frame = 0

    # first loop: until the first frame that has a full audio section
    while True:
        window_start_sample = int(frame_time * sample_rate) - half_window_size

        if window_start_sample >= 0:
            break

        window_end_sample = window_start_sample + window_size
        frame_time += frame_duration
        frame += 1

    # inner loop: until the last frame that has a full audio section
    while True:
        window_start_sample = int(frame_time * sample_rate) - half_window_size
        window_end_sample = window_start_sample + window_size

        if window_end_sample > total_audio_samples or frame >= total_video_samples:
            break

        compute_frame(
            raw_audio[window_start_sample:window_end_sample],
            out=result[frame],
        )
        frame_time += frame_duration
        frame += 1

    # final loop: until the end of the video
    while frame < total_video_samples:
        window_start_sample = int(frame_time * sample_rate) - half_window_size
        frame_time += frame_duration
        frame += 1

    return result


def compute_frame(audio: np.ndarray, out: np.ndarray) -> np.ndarray:
    """Computes the frequency-space representation of a window of audio,
    storing the result in out.
    """
    destroyable_converted_x = audio.astype(np.float64, copy=True)
    np.abs(scipy.fft.rfft(destroyable_converted_x, overwrite_x=True)[:-1], out=out)  # type: ignore
    return out
