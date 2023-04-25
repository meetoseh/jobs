# journey video shareable

This module produces video files which act as previews of journeys. This
incorporates the following information:

- The title of the class
- The instructor for the class
- The audio content

Unlike in the standard journey_audio shareable, this does not take in a journey
background image. Instead, it produces a transcript of the audio file using
OpenAI, then uses that transcript to generate image file descriptions using
OpenAI Completions, and then finally converts those to images using OpenAI
Images.

Those images are then crossfaded between, overlaid with an oscilloscope (
like with journey_audio) and crisp captioning.
