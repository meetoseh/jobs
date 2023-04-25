"""The second step in this pipeline is to convert the transcript into
image descriptions which can be fed into the images api. This is done
using the conversions api.
"""
from dataclasses import dataclass
import json
import math
import re
from typing import List, Literal, Optional, Tuple, TypedDict
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


def create_image_description_prompt_stable_diffusion(
    transcript: Transcript, timerange: TimeRange
) -> List[ChatCompletionMessage]:
    return [
        {
            "role": "system",
            "content": """You are a storyboard artist for an animation studio. You produce
suggestions for the background images to use in the animation at various points
during the movie. Your suggestions must be a series of | separated tags. 
At least 5 tags are required, starting with the noun phrase which best describes
the content of the image. You should include a well-known artist name to indicate
the style of the image. Prefer human portrait images, nature images, abstract
scenery, mystical animals, and abstract architecture. Avoid images including written
text, logos, hands, or feet in the description.

The following are examples of image descriptions which are not acceptable:

1. A person sitting, standing up, or lying down.
2. An image of a serene nature scene, such as a calm lake or a beach at sunset

The following are examples of image descriptions which are acceptable:

1. Elsa| d & d| fantasy| intricate| elegant| highly detailed| digital painting| artstation| concept art| matte| sharp focus| illustration| hearthstone| art by artgerm and greg rutkowski and alphonse mucha| 8k
2. strong warrior princess| centered| key visual| intricate| highly detailed| breathtaking beauty| precise lineart| vibrant| comprehensive cinematic| Carne Griffiths| Conrad Roset
3. photo of 8k ultra realistic harbour| port| boats| sunset| beautiful light| golden hour| full of colour| cinematic lighting| battered| trending on artstation| 4k| hyperrealistic| focused| extreme details| unreal engine 5| cinematic| masterpiece| art by studio ghibli
4. Children's drawing using pencils an crayons on a white piece of paper| grass flowers butterflies, with pet dragon| forest in background| poorly drawn| Crayola| faber castell

The output should be as a numbered list as in the above examples. You must
provide image descriptions even if no visual component is present in the
transcript, using an appropriate abstraction. For example, a person saying
hello could be described as

1. inside a girl room| cyberpunk vibe| neon glowing lights| sharp focus| photorealistic| unreal engine 5| girl in the bed| window that shows the skyscrapers in the background| deviantart| highly detailed| sharp focus| sci-fi| stunningly beautiful| dystopian| iridescent gold| cinematic lighting| dark

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
            "content": """You are a storyboard artist for an animation studio. You produce
suggestions for the background images to use in the animation at various points
during the movie. Your suggestions must be 3-7 words generally describing the
image contents, suitable to be used as a search query on a stock image website.
Do not use any proper nouns, such as names of people, places, or brands.

The following are examples of image descriptions which are acceptable:

1. Woman in packed stadium
2. Child alone in a cave
3. Deer drinking from a river

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


def create_image_description_prompt(
    transcript: Transcript,
    timerange: TimeRange,
    *,
    model: Literal["stable-diffusion", "dall-e", "pexels"] = "dall-e",
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
    if model == "stable-diffusion":
        return create_image_description_prompt_stable_diffusion(transcript, timerange)

    if model == "dall-e":
        return create_image_description_prompt_dalle(transcript, timerange)

    if model == "pexels":
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
    model: Optional[Literal["stable-diffusion", "dall-e", "pexels"]] = None,
) -> ImageDescriptions:
    """Creates the image descriptions for the given transcript.

    Args:
        transcript (Transcript): The transcript to use to create the image descriptions
        max_completion_retries (int, optional): The maximum number of times to retry the completion. We
            will retry with a different temperature, following the pattern 1,1.1,0.9,1.2,0.8,1.3,0.7,...
        api_delay (float, optional): How long to wait between openai requests.

    Returns:
        ImageDescriptions: The image descriptions for the given transcript

    Raises:
        ImageDescriptionsError: if we are unable to create the image descriptions, even after
            exhausting all retries
    """
    if model is None:
        from p08_images import HAVE_STABLE_DIFFUSION

        model = "stable-diffusion" if HAVE_STABLE_DIFFUSION else "dall-e"

    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
    result: List[Tuple[TimeRange, List[str]]] = []
    for timerange, _ in transcript.phrases:
        if result:
            time.sleep(api_delay)
        messages = create_image_description_prompt(transcript, timerange, model=model)
        logging.info(
            f"Creating image descriptions for {timerange} using the following prompt:\n\n{json.dumps(messages, indent=2)}"
        )
        descriptions: List[str] = []
        target_num_descriptions = max(
            1, math.ceil(timerange.get_width_in_seconds() / 3)
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

            descriptions.extend(new_descriptions)

        result.append((timerange, descriptions[:target_num_descriptions]))
    return ImageDescriptions(transcript=transcript, image_descriptions=result)
