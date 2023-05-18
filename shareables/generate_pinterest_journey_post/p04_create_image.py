import logging
from itgs import Itgs
from graceful_death import GracefulDeath
from dataclasses import dataclass
from shareables.generate_pinterest_journey_post.p03_create_image_prompt import (
    PinterestImagePrompt,
)
import shareables.journey_audio_with_dynamic_background.p08_images as images


@dataclass
class PinterestRawImage:
    path: str
    """The path to the image file"""

    @classmethod
    def from_dict(kls, raw: dict):
        return kls(**raw)


async def create_raw_image(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    prompt: PinterestImagePrompt,
    folder: str,
    max_attempts_per_prompt: int = 2,
) -> PinterestRawImage:
    """Generates the raw image for the pin using the given prompt.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): used for signal handling to stop early
        prompt (PinterestImagePrompt): the prompt to use to generate the image
        folder (str): the folder to save the image to
        max_attempts_per_prompt (int): the maximum number of attempts to use
            for each available prompt, failing if no prompt succeeds
    """
    generator = images.DarkenImageGenerator(
        images.StabilityAIImageGenerator(
            1000, 1500, initial_width=256, initial_height=384
        )
    )

    for img_prompt_idx, img_prompt in enumerate(prompt.prompts):
        logging.debug(
            f"Generating image using prompt {img_prompt_idx + 1}/{len(prompt.prompts)}: {img_prompt}"
        )
        for attempt in range(max_attempts_per_prompt):
            if gd.received_term_signal:
                raise Exception("received term signal")
            try:
                img = await generator.generate(itgs, img_prompt, folder)
                assert img.style == "image"
                return PinterestRawImage(path=img.path)
            except:
                if (
                    img_prompt_idx == len(prompt.prompts) - 1
                    and attempt == max_attempts_per_prompt - 1
                ):
                    raise
                logging.exception(
                    f"Failed to generate image using prompt {img_prompt_idx + 1}/{len(prompt.prompts)} "
                    f"(attempt {attempt + 1}/{max_attempts_per_prompt})"
                )
