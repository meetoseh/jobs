import logging
import os
from typing import List, Literal

import openai
from graceful_death import GracefulDeath
from itgs import Itgs
from dataclasses import dataclass
from lib.chatgpt.model import ChatCompletionMessage
from lib.redis_api_limiter import ratelimit_using_redis
from shareables.generate_pinterest_journey_post.p01_get_journey_info import (
    PinterestJourneyForPost,
)
from shareables.generate_pinterest_journey_post.p02_create_text_content import (
    PinterestTextContentForPost,
)
import json


@dataclass
class PinterestImagePrompt:
    llm_model: Literal["gpt-3.5-turbo"]
    """The large language model used to generate the image prompt"""

    image_model: Literal["stability-ai"]
    """The image model that the prompt is intended for"""

    prompts: List[str]
    """The prompt to use to generate the image. Several options are provided
    in case any cause issues with the image model
    """

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


def anify(word: str):
    word = word.lower()
    if word[0] in "aeiou":
        return "an " + word
    return "a " + word


def create_prompt(
    journey: PinterestJourneyForPost, text_content: PinterestTextContentForPost
) -> List[ChatCompletionMessage]:
    """Creates the prompt for the large language model to generate the image
    prompt for the image model.
    """
    pin_json = json.dumps(
        {
            "content": text_content.overlaid_text,
            "title": text_content.title,
            "description": text_content.description,
        },
        indent=4,
    )
    return [
        {
            "role": "system",
            "content": """As an pinterest artist, others come to you with ideas
for pins and you create an image background that can be used for the pin. You
specialize in creative, digital art. In this first step, you output descriptions
of images in the following example format:

===

[
    "string", 
    "string"
]

===

Where each string represents one suggestion you give to the requester, and you
have at least 3 different suggestions. Your suggestions must be a series of 
comma separated tags. You should choose these tags with the following 
technique:

The first tag is always the general description. For example, "Monumental
old ruined tower within a dark misty forest"

Include emotional prompt words, which have mood and energy. For example,
these are positive mood, low energy words: "light, peaceful, calm, serene".
These are negative mood, high energy words: "dark, ghostly, shocking, terrifying".
Include several of only one category of emotional prompt words.

Include several of only one category of size and structure words. For example,
for big and free use words like curvaceous, swirling, organic. For small and
structured, use words like ornate, delicate, neat, precise.

Include several of only one category of lighting. For example, for golden hour use
dusk, sunset, sunrise, warm lighting. For overcast use cloudy, foggy, misty,
flat lighting.

Here's a specific example of a suggestion that you might output:

===

[
    "Cottage in a field of flowers, light, peaceful, comforting, natural, organic, dusk, sunset, strong shadows",
    "Empty abandoned bus stop in the desert, dark, ghostly, terrifying, unnatural, neat, precise, elaborate, cloudy, misty, flat lighting"
]

===
""",
        },
        {
            "role": "user",
            "content": f"""What images should be created for a pin about {anify(journey.category)} class called
{journey.title}? Here's what we're planning on posting:

===

{pin_json}
""",
        },
    ]


def parse_completion(msg: str) -> List[str]:
    """Parses the suggestions from the large language model to provide to the
    image model

    Args:
        msg (str): The message from the large language model

    Returns:
        List[str]: The suggestions to provide to the image model
    """
    json_starts_at = msg.index("[")
    json_ends_at = msg.rindex("]")
    json_raw = msg[json_starts_at : json_ends_at + 1]
    json_parsed = json.loads(json_raw)

    assert isinstance(json_parsed, list)
    assert all(isinstance(suggestion, str) for suggestion in json_parsed)
    return json_parsed


def _temperature_for_attempt(attempt: int) -> float:
    """Gets the temperature used in the openai completion model for the given
    attempt, where attempt 0 is the first attempt.
    """
    return 1.0 + (((attempt + 1) // 2) * 0.1) * (-1 if attempt % 2 == 0 else 1)


async def create_image_prompt(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    journey: PinterestJourneyForPost,
    text_content: PinterestTextContentForPost,
    max_attempts: int = 5,
) -> PinterestImagePrompt:
    """Generates the prompt for the image model to use to generate the
    image for the pin.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): used for detecting signals and stopping early
        journey (PinterestJourneyForPost): the journey to create the image for
        text_content (PinterestTextContentForPost): the text content to use
        max_attempts (int): the maximum number of attempts using the llm before
            raising an error. This covers both overloaded servers and invalid
            responses. We'll retry with very slightly different temperatures
            each time.

    Returns:
        PinterestImagePrompt: the prompt to use to generate the image
    """
    llm_model = "gpt-3.5-turbo"
    image_model = "stability-ai"

    llm_prompt = create_prompt(journey, text_content)
    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
    openai_client = openai.Client(api_key=openai_api_key)

    logging.info(
        f"Generating image prompts using llm prompt\n{json.dumps(llm_prompt, indent=2)}"
    )
    for attempt in range(max_attempts):
        if gd.received_term_signal:
            raise Exception("received term signal")
        try:
            await ratelimit_using_redis(
                itgs,
                key="external_apis:api_limiter:chatgpt",
                time_between_requests=3,
            )
            completion = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=llm_prompt,
                temperature=_temperature_for_attempt(attempt),
            )
            logging.info(f"Got completion: {completion}")
            content = completion.choices[0].message.content
            assert content is not None, completion
            image_prompts = parse_completion(content)
            return PinterestImagePrompt(
                llm_model=llm_model,
                image_model=image_model,
                prompts=image_prompts,
            )
        except Exception:
            if attempt == max_attempts - 1:
                raise
            logging.exception(
                f"Failed to generate image prompts (attempt {attempt + 1}/{max_attempts})..."
            )

    assert False, "unreachable"
