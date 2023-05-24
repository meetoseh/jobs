import logging
import os
from typing import Dict, List, Literal, Optional
from graceful_death import GracefulDeath
from itgs import Itgs
from shareables.generate_class.p01_select_classes import (
    InputJourneyPrompt,
    InputJourneyPromptColor,
    InputJourneyPromptNumeric,
    InputJourneyPromptWord,
)
from shareables.generate_class.p02_generate_transcripts import (
    InputJourneyWithTranscript,
    SelectedJourneysWithTranscripts,
)
from lib.transcripts.model import Transcript
from lib.transcripts.gen import parse_vtt_transcript
from dataclasses import dataclass
from lib.chatgpt.model import ChatCompletionMessage
import openai
from lib.redis_api_limiter import ratelimit_using_redis
import re


@dataclass
class GeneratedClassMeta:
    title: str
    description: str
    instructor: str
    prompt: InputJourneyPrompt
    transcript: Transcript
    emotion: str
    category: str


def describe_prompt(prompt: InputJourneyPrompt) -> str:
    if prompt.style == "word":
        return f"Prompt: {prompt.text}\nPrompt Type: word\nPrompt Options: {', '.join(prompt.options)}"
    elif prompt.style == "numeric":
        return f"Prompt: {prompt.text}\nPrompt Type: 1-10"
    elif prompt.style == "color":
        return f"Prompt: {prompt.text}\nPrompt Type: colors\nPrompt Options: {', '.join(prompt.colors)}"
    else:
        raise ValueError(f"Unknown prompt: {prompt=}")


def describe_input(input: InputJourneyWithTranscript) -> str:
    return f"""Title: {input.journey.title}

Description: {input.journey.description}

{describe_prompt(input.journey.prompt)}

Transcript: {input.transcript}
"""


def create_prompt(
    input: SelectedJourneysWithTranscripts,
) -> List[ChatCompletionMessage]:
    """Generates the prompt to provide to the completion large language model in
    order to generate an entirely new title, description, prompt, and transcript
    for a similar class.
    """
    return [
        {
            "role": "system",
            "content": f"""As a content creator, you need to provide a new {input.category.lower()} class that
evokes {input.emotion.lower()}. Your class should be about a minute long, start
with introducing yourself, Amado Chip, and end with a farewell such as Goodbye
or Thanks for practicing with me. Give yourself plenty of time for each phrase,
saying less is often more impactful. Do not reference the entrance question in
the transcript. In total, your class will have four parts: a title and
description for metadata, an entrance question before the class, and then the
transcript for what is said during the class.

Here are {len(input.journeys_with_transcripts)} examples of {input.category.lower()} classes that evoke {input.emotion.lower()}:

===

"""
            + "\n\n===\n\n".join(
                [
                    describe_input(journey_with_transcript)
                    for journey_with_transcript in input.journeys_with_transcripts
                ]
            )
            + """

===

You must output the class in the same format as above. For example, the first line should always
be in the form

Title: <title>

Valid "Prompt Type"s are: word, 1-10, colors. For word prompts, include a comma-separated list of
options for Prompt Option. For color prompts, include a comma-separated list of 6-digit hex colors
for Prompt Options. For 1-10 prompts, do not include Prompt Options. The Prompt cannot exceed 75 
characters in length.
""",
        },
        {
            "role": "user",
            "content": f"Generate a new {input.category.lower()} class by Amado Chip that evokes {input.emotion.lower()}",
        },
    ]


def parse_completion(message: str, emotion: str, category: str) -> GeneratedClassMeta:
    """If the completion is successful, then it is parsed into the corresponding
    new class. Otherwise, an error is raised.

    Args:
        message (str): The completion message
        emotion (str): The emotion that the class should evoke
        category (str): The category of the class

    Raises:
        ValueError: If the completion message is not valid
    """

    valid_titles = frozenset(
        [
            "Title",
            "Description",
            "Prompt",
            "Prompt Type",
            "Prompt Options",
            "Transcript",
        ]
    )
    parts: Dict[str, str] = {}
    current_section: Optional[
        Literal["Title", "Description", "Prompt", "Transcript"]
    ] = None
    current_section_value: Optional[str] = None
    section_header = re.compile(r"^([A-Z][^:]+):\s*(.+)$")

    for line in message.splitlines(keepends=False):
        match = section_header.match(line)
        if not match:
            if current_section is not None:
                current_section_value += line + "\n"
            continue

        if current_section is not None:
            parts[current_section] = current_section_value.strip()

        current_section = match.group(1)
        current_section_value = match.group(2) + "\n"

        if current_section in parts:
            raise ValueError(f"Duplicate section: {current_section}")

        if current_section not in valid_titles:
            raise ValueError(f"Invalid section: {current_section}")

    if current_section is not None:
        parts[current_section] = current_section_value.strip()

    if "Title" not in parts:
        raise ValueError("Missing Title")

    if "Description" not in parts:
        raise ValueError("Missing Description")

    if "Prompt" not in parts:
        raise ValueError("Missing Prompt")

    if "Prompt Type" not in parts:
        raise ValueError("Missing Prompt Type")

    prompt: Optional[InputJourneyPrompt] = None
    if parts["Prompt Type"] == "word":
        if "Prompt Options" not in parts:
            raise ValueError("Missing Prompt Options for word prompt")

        prompt = InputJourneyPromptWord(
            style="word",
            text=parts["Prompt"],
            options=[
                p.strip() for p in parts["Prompt Options"].split(",") if p.strip() != ""
            ],
        )

        if any(len(opt) > 45 for opt in prompt.options):
            raise ValueError("Prompt options must be less than 45 characters")
        if len(prompt.options) < 2:
            raise ValueError("Must have at least 2 prompt options")

    elif parts["Prompt Type"] == "1-10":
        prompt = InputJourneyPromptNumeric(
            style="numeric", text=parts["Prompt"], min=1, max=10, step=1
        )
    elif parts["Prompt Type"] == "colors":
        if "Prompt Options" not in parts:
            raise ValueError("Missing Prompt Options for color prompt")

        colors = [
            p.strip() for p in parts["Prompt Options"].split(",") if p.strip() != ""
        ]
        color_regex = re.compile(r"^#[0-9a-fA-F]{6}$")
        for color in colors:
            if not color_regex.match(color):
                raise ValueError(f"Invalid color: {color} in {colors=}")

        prompt = InputJourneyPromptColor(
            style="color",
            text=parts["Prompt"],
            colors=colors,
        )
    else:
        raise ValueError(f"Unknown prompt type: {parts['Prompt Type']}")

    if "Transcript" not in parts:
        raise ValueError("Missing Transcript")

    transcript = parts["Transcript"]
    if not transcript.startswith("WEBVTT\n"):
        transcript = "WEBVTT\n\n" + transcript

    parsed_transcript = parse_vtt_transcript(transcript)

    if len(prompt.text) > 75:
        raise ValueError(
            f"Prompt is too long: {len(prompt.text)=} > 75 for {prompt.text=}"
        )

    return GeneratedClassMeta(
        title=parts["Title"],
        description=parts["Description"],
        instructor="Amado Chip",
        prompt=prompt,
        transcript=parsed_transcript,
        emotion=emotion,
        category=category,
    )


def _temperature_for_attempt(attempt: int) -> float:
    """Gets the temperature used in the openai completion model for the given
    attempt, where attempt 0 is the first attempt.
    """
    return 1.0 + (((attempt + 1) // 2) * 0.05) * (-1 if attempt % 2 == 0 else 1)


async def create_class_meta(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    input: SelectedJourneysWithTranscripts,
    max_attempts: int = 10,
) -> GeneratedClassMeta:
    """Attempts to generate a new class using the given input as inspiration.
    This gives a title, description, prompt, and transcript for the class, but
    does not go so far as to convert that transcript into actual audio.

    Args:
        itgs (Itgs): The integrations to (re)use
        gd (GracefulDeath): Used for signalling when to stop early
        input (SelectedJourneysWithTranscripts): The input to use for inspiration
        max_attempts (int): The maximum number of times to retry the generation
            before giving up
    """
    prompt = create_prompt(input)
    openai_api_key = os.environ["OSEH_OPENAI_API_KEY"]

    logging.info(f"Generating a new class using prompt:\n\n{prompt}")
    for attempt in range(max_attempts):
        if gd.received_term_signal:
            raise Exception("received term signal")

        try:
            await ratelimit_using_redis(
                itgs,
                key="external_apis:api_limiter:chatgpt",
                time_between_requests=3,
            )
            completion = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=prompt,
                temperature=_temperature_for_attempt(attempt),
                api_key=openai_api_key,
            )
            logging.info(f"Got completion:\n\n{completion}")
            generated_class_meta = parse_completion(
                completion.choices[0].message.content, input.emotion, input.category
            )
            logging.info(f"Parsed completion:\n\n{generated_class_meta}")
            return generated_class_meta
        except Exception:
            if attempt == max_attempts - 1:
                raise
            logging.exception(
                f"Failed to generate class on attempt {attempt + 1}", exc_info=True
            )
