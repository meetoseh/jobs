"""The second step in this pipeline is to convert the transcript into
image descriptions which can be fed into the images api. This is done
using the conversions api.
"""
from dataclasses import dataclass
import json
import math
import re
from typing import List, Literal, Optional, Set, Tuple, TypedDict
import openai
from shareables.journey_audio_with_dynamic_background.p06_transcript import (
    TimeRange,
    Transcript,
)
from shareables.shareable_pipeline_exception import ShareablePipelineException
import time
import logging
import os
import io


class ImageDescriptionsError(ShareablePipelineException):
    def __init__(self, message: str):
        super().__init__(message, 7, "image_descriptions")


@dataclass
class ImageDescriptions:
    transcript: Transcript
    """The transcript, used for a better string representation of the object."""

    image_descriptions: List[Tuple[TimeRange, List[str]]]
    """The image descriptions to use for each timerange. Since multiple images
    may be needed for each timerange, multiple descriptions are provided for
    each timerange, of which any number may be used, in any order.
    """

    def __str__(self):
        """Produces a stringified representation of this object, which looks
        like the following:

        1. (00:00:00.000 --> 00:00:15.000) "Hello, my name is Anna"
           a. A calm and peaceful background setting.
           b. A person's chest rising and falling as they breathe.
        2. (00:00:15.000 --> 00:01:00.000) "Today we are going to talk about the sun"
           a. A bright and sunny day.
           b. A smiling person on a street.
        """
        result = io.StringIO()
        for i, (timerange, descriptions) in enumerate(self.image_descriptions):
            result.write(f'{i}. ({timerange}) "{self.transcript.phrases[i][1]}"\n')
            for j, description in enumerate(descriptions):
                result.write(f"   {chr(ord('a') + j)}. {description}\n")
        return result.getvalue()


class ChatCompletionMessage(TypedDict):
    role: Literal["user", "system", "assistant"]
    content: str


def create_image_description_prompt_dalle(
    transcript: Transcript, timerange: TimeRange
) -> List[ChatCompletionMessage]:
    return [
        {
            "role": "system",
            "content": """You are a storyboard artist for an animation studio. You produce
suggestions for the background images to use in the animation at various points
during the movie. Your suggestions must be a series of comma separated tags. 

Each prompt includes tags from each of the following categories:

The first tag is always the general description. For example, "Monumental
old ruins tower of a dark misty forest"

Include emotional prompt words, which have mood and energy. For example,
these are positive mood, low energy words: "light, peaceful, calm, serene".
These are negative mood, high energy words: "dark, ghostly, shocking, terrifying".
Include several of only one category of emotional prompt words.

Include several of only one category of size and structure words. For example,
for big and free use curvaceous, swirling, organic. For small and structured,
use ornate, delicate, neat, precise.

Include several of only one category of looks and vibes. For example, for
vaporwave, use neon, pink, blue, geometric. For cybernetic, use glows,
greens, metals, armor. 

Include several of only one category of lighting. For example, for golden hour use
dusk, sunset, sunrise, warm lighting. For overcast use cloudy, foggy, misty,
flat lighting.

Include one tag indicating category of illustration style. For example,
stencil art. For example, charcoal sketch. For example, vector art. Prefer
to use vector art, storybook, or watercolor & pen.

The following are examples of image descriptions which are acceptable:

1. Woman on stage with large audience in chairs, bright, energetic, monemental, ordered, defined, gold, copper, Victoriana, professional lighting, storybook
2. Child alone in a cave, muted, bleak, washed-out, curvaceous, earthy, chaotic, stone, dark, mystery, twilight, slow shutter speed, watercolor & pen
3. Deer drinking from a rocky river, light, peaceful, comforting, natural, organic, chaotic, stone, dark, mystery, dusk, sunset, sunrise - warm lighting, strong shadows, watercolor & pen

The output should be as a numbered list as in the above example. You must
provide image descriptions even if no visual component is present in the
transcript, using an appropriate abstraction.

===

You are given the entire transcript of the audio, but you are only responsible
for the specified section. You must output 5 image descriptions.
""",
        },
        {
            "role": "user",
            "content": f"""For the following transcript:

{transcript}

===
            
What are the image descriptions for the following section of the transcript?


{timerange}
{next(text for (tr, text) in transcript.phrases if tr == timerange)}
""",
        },
    ]


def create_image_description_prompt_pexels(
    transcript: Transcript, timerange: TimeRange
) -> List[ChatCompletionMessage]:
    return [
        {
            "role": "system",
            "content": """You produce suggestions for abstract nature background images to use in a visualization
of a talk at various points during the transcript. Your suggestions must be a
numbered list where each item contains 2 tags generally describing the image
contents. These should be broad descriptions of the type of shot or the contents
in the shot. Since these images are going to come from stock photo websites and
not drawn, avoid pictures of people or specific objects as it will cause confusion.
For example, it's confusing to have a picture of a person talking, when it's not the
person talking in the video. However, we can't just have a black screen so we always
need something to show. If it doesn't matter what we show, just use a generic
term like Beautiful or Nature, which generally results in a pretty video. Be creative:
think beyond the literal meaning of the words. A person meditating is focusing inward,
so use terms that focus inward like Cave, Interior, or Tunnel. Note that some terms
often result in really weird pseudo-inappropriate videos, like "Hands" or "Feet", so
avoid those.

The following are examples of image descriptions which are acceptable:

1. Spring, Beautiful
2. Ocean, Aerial Footage
3. Astronomy, Galaxy
4. Birds Eye View, City
5. Flame, Fireplace

===

You are given the entire transcript of the audio, but you are only responsible
for the specified section. You must output 5 image descriptions.
""",
        },
        {
            "role": "user",
            "content": f"""For the following transcript:

{transcript}

===
            
What are the image descriptions for the following section of the transcript?


{timerange}
{next(text for (tr, text) in transcript.phrases if tr == timerange)}
""",
        },
    ]


def create_image_description_prompt(
    transcript: Transcript,
    timerange: TimeRange,
    *,
    model: Literal["dall-e", "pexels", "pexels-video"] = "dall-e",
) -> List[ChatCompletionMessage]:
    """Creates the prompt to provide the completions API to produce
    a list of image descriptions for the given part of the transcript. The
    resulting descriptions from the api will be provided in a number list, e.g.,

    1. A blue sky with clouds
    2. A person standing in a field
    3. A flock of birds flying in the sky

    Args:
        transcript (str): the transcript to use for the prompt

    Returns:
        str: the prompt to provide to the completions api
    """
    if model == "dall-e":
        return create_image_description_prompt_dalle(transcript, timerange)

    if model in ("pexels", "pexels-video"):
        return create_image_description_prompt_pexels(transcript, timerange)

    raise ValueError(f"Invalid model: {model}")


def parse_image_descriptions(completion: str) -> List[str]:
    """Parses the image descriptions from the completion provided by the
    completions api.

    Args:
        completion (str): the completion to parse

    Returns:
        List[str]: the parsed image descriptions

    Raises:
        ValueError: if the completion does not contain a numbered list
    """
    state: Literal["before_list", "in_list"] = "before_list"
    current_item: Optional[str] = None
    image_descriptions: List[str] = []

    item_regex = re.compile(r"^\s*(\d+)\.(.+)$")

    for line in completion.splitlines():
        item_match = item_regex.match(line)
        if state == "before_list":
            if item_match:
                if int(item_match.group(1)) != 1:
                    raise ValueError(
                        f"Invalid completion: {completion} (expected start of list, got {line})"
                    )
                state = "in_list"
                current_item = item_match.group(2)
        elif state == "in_list":
            if item_match:
                image_descriptions.append(current_item.strip())
                if int(item_match.group(1)) != len(image_descriptions) + 1:
                    raise ValueError(
                        f"Invalid completion: {completion} (expected item {len(image_descriptions) + 1}, got {line})"
                    )
                current_item = item_match.group(2)
            else:
                current_item += " " + line

    if current_item:
        image_descriptions.append(current_item.strip())

    if not image_descriptions:
        raise ValueError(f"Invalid completion: {completion}")

    return image_descriptions


def _temperature_for_attempt(attempt: int) -> float:
    """Gets the temperature used in the openai completion model for the given
    attempt, where attempt 0 is the first attempt.
    """
    return 1.0 + (attempt * 0.1) * (-1 if attempt % 2 == 0 else 1)


def create_image_descriptions(
    transcript: Transcript,
    *,
    max_completion_retries: int = 5,
    api_delay: float = 1.0,
    model: Optional[Literal["dall-e", "pexels", "pexels-video"]] = None,
    min_seconds_per_image: float = 3.0,
) -> ImageDescriptions:
    """Creates the image descriptions for the given transcript.

    Args:
        transcript (Transcript): The transcript to use to create the image descriptions
        max_completion_retries (int, optional): The maximum number of times to retry the completion. We
            will retry with a different temperature, following the pattern 1,1.1,0.9,1.2,0.8,1.3,0.7,...
        api_delay (float, optional): How long to wait between openai requests.
        model (str, optional): The model to use to generate the image descriptions. If not provided,
            we will use the dall-e model.
        min_seconds_per_image (float, optional): We ensure there are enough images that if every
            image was only shown for this many seconds, there would still be enough images.

    Returns:
        ImageDescriptions: The image descriptions for the given transcript

    Raises:
        ImageDescriptionsError: if we are unable to create the image descriptions, even after
            exhausting all retries
    """
    if model is None:
        model = "dall-e"

    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
    result: List[Tuple[TimeRange, List[str]]] = []
    for timerange, _ in transcript.phrases:
        if result:
            time.sleep(api_delay)
        messages = create_image_description_prompt(transcript, timerange, model=model)
        logging.info(
            f"Creating image descriptions for {timerange} using the following prompt:\n\n{json.dumps(messages, indent=2)}"
        )
        descriptions: Set[str] = set()
        target_num_descriptions = max(
            1, math.ceil(timerange.get_width_in_seconds() / min_seconds_per_image)
        )
        while len(descriptions) < target_num_descriptions:
            attempt = 0
            while True:
                if descriptions:
                    logging.info(
                        f"Not enough descriptions ({len(descriptions)}/{target_num_descriptions}), getting more..."
                    )

                try:
                    completion = openai.ChatCompletion.create(
                        model="gpt-3.5-turbo",
                        messages=messages,
                        temperature=_temperature_for_attempt(attempt),
                        api_key=openai_api_key,
                    )
                    logging.info(f"Got completion:\n\n{completion}")
                    new_descriptions = parse_image_descriptions(
                        completion.choices[0].message.content
                    )
                    if model in ("pexels", "pexels-video"):
                        new_descriptions = [
                            d.replace(",", "") for d in new_descriptions
                        ]
                    logging.info(f"Parsed descriptions: {json.dumps(new_descriptions)}")
                    time.sleep(api_delay)
                    break
                except Exception:
                    attempt += 1
                    if attempt >= max_completion_retries:
                        raise ImageDescriptionsError(
                            f"Unable to create image descriptions for {timerange}"
                        )
                    logging.info("Failed to parse completion, retrying...")
                    time.sleep(api_delay)

            for desc in new_descriptions:
                descriptions.add(desc)

        result.append((timerange, list(descriptions)[:target_num_descriptions]))
    return ImageDescriptions(transcript=transcript, image_descriptions=result)
