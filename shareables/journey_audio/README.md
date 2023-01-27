# journey audio shareable

This module produces video files which act as previews of journeys. This
incorporates the following information:

- The title of the class
- The instructor for the class
- The audio content, for the audio
- The audio content, represented visually somewhat similarly to an oscilloscope

# Audio Visualization

The audio is first converted to an uncompressed wav file using linear
pulse-code modulation (LPCM). This means the audio is loaded as the amplitude
of the analog signal sampled at uniform intervals, using a uniformly linear
quantization. Since the input audio is typically stereo, there are two independent
audio streams corresponding to the left and right side of the microphone (like
we have left and right ears, rather than one ear on our forehead).

To convert the audio to mono, the channels are averaged. This does result in a
rare case where if the left and right channels are out of phase then the result
can be null. If this happens, it could be fixed by taking the difference, though
it's unlikely enough that for now it's ignored.

The mono channel is now simply an array of audio amplitudes. Using a sliding window
approach, we can convert this two a 2-d array, e.g.,

```
[1,2,3,4,5,6,7,8,9,10]
```

using a sliding window of width 3 becomes

```
[
    [1,2],
    [1,2,3],
    [2,3,4],
    [3,4,5],
    [4,5,6],
    [5,6,7],
    [6,7,8],
    [7,8,9],
    [8,9,10],
    [9,10]
]
```

the edges, where there isn't a full window, are converted to an array of zeros
with length `window_size/2`

For the remaining audio, a fast fourier transform (FFT) is applied to convert
the audio into frequency space. The negative terms are dropped (i.e., the second
half of the resulting array), and the remaining terms are converted to their absolute
values to get an all real-valued array with length `window_size/2`

The resulting 2d array, where the first axis represents time and the second axis
represents the frequency domain with `window_size` samples and a spacing of
`1 / sample_frequency` (e.g., 44.1kHz becomes `1/44100`)

The next step is to create a partition of the frequency space with 20 different
bins and where each bin participates roughly equally.

Given a partition `p_0, ..., p_20` where `b_i = [p_i, p_i+1)`, the participation
of bin i, `P(i)` is the average of the average amplitude within the bin at each
time.

The goal is a roughly equal participation across bins. For example, one could
approach this by minimizing

`E(p) = sum((P(i) - avgP)^2)`

Since only the average across the time domain is required for this calculation,
we compute the average amplitude at each frequency across time to get a 1D
array which can be thought of as the frequency participation rate. This is then
the minimum raggedness word wrap problem, which can be solved in linear time,
though the solution is complicated: https://xxyxyz.org/line-breaking/

Once the partitions have been selected, each point is time is converted into
an array of 20 elements, representing the average of each bin. Then time is
down-sampled to the desired framerate by maxing along the time axis, again
using a sliding window technique, and finally each frame shows the bins via
a bar graph.
