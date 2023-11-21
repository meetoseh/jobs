import json
import logging
import os
from typing import List
from itgs import Itgs
from graceful_death import GracefulDeath
from dataclasses import dataclass
from lib.chatgpt.model import ChatCompletionMessage
from lib.redis_api_limiter import ratelimit_using_redis
from shareables.generate_pinterest_journey_post.p01_get_journey_info import (
    PinterestJourneyForPost,
)
import openai


@dataclass
class PinterestTextContentForPost:
    overlaid_text: str
    """The text which should go on the image"""

    title: str
    """The pin title"""

    description: str
    """The pin description"""

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


def create_prompt(journey: PinterestJourneyForPost) -> List[ChatCompletionMessage]:
    transcript = " ".join([p[1] for p in journey.transcript.phrases])
    return [
        {
            "role": "system",
            "content": """As an avid pinner, you're constantly on the lookout for interesting
content. You generally pin things related to your interests, such as
self-care, mental health, and personal growth.

You should respond to sources of inspiration by composing a pin like in the following
example:

===

{"content": "string", "title": "string", "description": "string"}

===

Where the content is 2 or 3 sentences that will be overlaid on top of a suitable
image and is the only thing that people will see when deciding whether or not to
engage. It should be a combination of a call to action and a personal message
that is relevant to the thing being pinned, and should be written in a way that
makes people curious. Make sure to use newlines to separate important pieces of
content.

The title is used for SEO and is 30-100 characters which provides additional
context to the content, and the description is 100-500 characters which provides
additional context on top of the content and title. Avoid repetition in the
content and title: they will always read the content before the title, so the
title should act as a continuation of the content.

Here's a specific example of a pin that you might compose:

===

{
    "content": "Feel better about yourself by taking care of yourself\\nI use this breathwork technique because it really works", 
    "title": "Learn a 3-part breath to achieve a state of calm.", 
    "description": "Take a minute for yourself with this short, free audio guide to a 3-part breath. |#self-care #breathwork"
}

===

The content newlines will be preserved. This is useful, for example, if you
are inspired by two particularly powerful sentences, putting them on separate
lines will make them stand out more.

For the description, ending with one or two highly relevant hashtags can make
your pin more discoverable. For example, if you're pinning a recipe for a
vegan chocolate cake, you might end the description with "#vegan #cake".
""",
        },
        {
            "role": "user",
            "content": f"""Here's an interesting talk called {journey.title} by {journey.instructor}
that appears to be worth pinning.

Description: {journey.description}

Transcript: {transcript}
""",
        },
    ]


def parse_completion(message: str) -> PinterestTextContentForPost:
    """Attempts to parse the pin included in the given ai-generated message.
    Raises an error if the message is not a valid pin.
    """
    # We don't know where the json starts, so we're going to look for the
    # first open curly bracket, then take until the last close curly bracket
    # and parse that as json
    start = message.index("{")
    finish = message.rindex("}") + 1

    pin_json = message[start:finish]
    pin_raw = json.loads(pin_json)

    return PinterestTextContentForPost(
        overlaid_text=pin_raw["content"].strip(),
        title=pin_raw["title"].strip(),
        description=pin_raw["description"].strip(),
    )


def _temperature_for_attempt(attempt: int) -> float:
    """Gets the temperature used in the openai completion model for the given
    attempt, where attempt 0 is the first attempt.
    """
    return 1.0 + (((attempt + 1) // 2) * 0.1) * (-1 if attempt % 2 == 0 else 1)


async def create_text_content(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    journey: PinterestJourneyForPost,
    max_attempts: int = 5,
) -> PinterestTextContentForPost:
    """Decides on the text-portion of the content to use for a new pin
    which will link to the given journey.
    """
    prompt = create_prompt(journey)
    logging.info(
        f"Creating text content for pin of {journey.title} by {journey.instructor} "
        f"using the prompt:\n{json.dumps(prompt, indent=2)}"
    )

    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]
    openai_client = openai.Client(api_key=openai_api_key)
    for attempt in range(max_attempts):
        if gd.received_term_signal:
            raise Exception("received term signal")

        try:
            await ratelimit_using_redis(
                itgs,
                key="external_apis:api_limiter:chatgpt",
                time_between_requests=3,
            )
            completion = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=prompt,
                temperature=_temperature_for_attempt(attempt),
            )
            logging.info(f"Got completion: {completion}")
            content = completion.choices[0].message.content
            assert content is not None
            return parse_completion(content)
        except Exception:
            if attempt == max_attempts - 1:
                raise

            logging.exception(
                f"Failed to create text content for pin of {journey.title} by {journey.instructor} (attempt {attempt+1}/{max_attempts})"
            )
    raise Exception("unreachable")
