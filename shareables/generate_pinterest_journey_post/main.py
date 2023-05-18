import asyncio
import json
import os
from typing import Literal, Optional
from itgs import Itgs
from graceful_death import GracefulDeath
import dataclasses
import shareables.generate_pinterest_journey_post.p01_get_journey_info as p01
import shareables.generate_pinterest_journey_post.p02_create_text_content as p02
import shareables.generate_pinterest_journey_post.p03_create_image_prompt as p03
import shareables.generate_pinterest_journey_post.p04_create_image as p04
import shareables.generate_pinterest_journey_post.p05_overlay_text_on_image as p05
import argparse
import logging


@dataclasses.dataclass
class PinterestJourneyPost:
    """The result of generating a new pinterest post"""

    journey: p01.PinterestJourneyForPost
    """The journey information that was used to generate the post"""

    text_content: p02.PinterestTextContentForPost
    """The text content that was used for the post"""

    image_prompt: p03.PinterestImagePrompt
    """The image prompt used to generate the image for the post"""

    raw_image: p04.PinterestRawImage
    """The raw image that was generated for the post"""

    final_image: p05.PinterestFinalImage
    """The final image for post, with the appropriate text overlaid on it"""


async def main():
    """Can be used if this pipeline is invoked directly, using command line arguments
    to feed into the pipeline
    """
    parser = argparse.ArgumentParser(
        description="Generate a new pinterest pin for a journey"
    )
    parser.add_argument(
        "--journey_uid",
        type=str,
        required=False,
        help="The uid of the journey to generate a pin for, False for the most recent ai-generated journey",
    )
    parser.add_argument(
        "--resume",
        type=str,
        required=False,
        default="overlay_text_on_image",
        help="If set, will resume the pipeline from the last step that was completed or this step, whichever is earlier",
        choices=[
            "none",
            "get_journey_info",
            "create_text_content",
            "create_image_prompt",
            "create_raw_image",
            "overlay_text_on_image",
        ],
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    gd = GracefulDeath()
    async with Itgs() as itgs:
        await run_pipeline(
            itgs,
            gd=gd,
            journey_uid=args.journey_uid,
            resume=args.resume if args.resume != "none" else None,
        )


async def get_latest_ai_journey(itgs: Itgs) -> str:
    """Used if the journey uid is not specified; fetches the most recently
    generated ai journey. This is primarily intended for the command line
    interface, as a more sophisticated query should normally be used
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    response = await cursor.execute(
        """
        SELECT
            uid
        FROM journeys
        WHERE
            special_category = 'ai' AND deleted_at IS NULL
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    if not response.results:
        raise Exception("no ai journeys found")
    return response.results[0][0]


async def run_pipeline(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    folder: str = os.path.join("tmp", "shareables", "generate_pinterest_journey_post"),
    journey_uid: str = None,
    resume: Optional[
        Literal[
            "get_journey_info",
            "create_text_content",
            "create_image_prompt",
            "create_raw_image",
            "overlay_text_on_image",
        ]
    ] = None,
) -> PinterestJourneyPost:
    os.makedirs(folder, exist_ok=True)
    steps = [
        "get_journey_info",
        "create_text_content",
        "create_image_prompt",
        "create_raw_image",
        "overlay_text_on_image",
    ]
    restore_up_to_excl = steps.index(resume) + 1 if resume else 0
    journey_path = os.path.join(folder, "journey.json")
    text_content_path = os.path.join(folder, "text_content.json")
    image_prompt_path = os.path.join(folder, "image_prompt.json")
    raw_image_path = os.path.join(folder, "raw_image.json")
    final_image_path = os.path.join(folder, "final_image.json")

    if restore_up_to_excl <= 0 or not os.path.exists(journey_path):
        if journey_uid is None:
            journey_uid = await get_latest_ai_journey(itgs)
        journey = await p01.get_journey_info(itgs, gd=gd, journey_uid=journey_uid)
        with open(journey_path, "w") as f:
            json.dump(dataclasses.asdict(journey), f, indent=2)
    else:
        with open(journey_path, "r") as f:
            journey = p01.PinterestJourneyForPost.from_dict(json.load(f))
        journey_uid = journey.uid

    logging.debug(
        f"Selected journey for pinterest post:\n\n{json.dumps(dataclasses.asdict(journey), indent=2)}"
    )

    if restore_up_to_excl <= 1 or not os.path.exists(text_content_path):
        text_content = await p02.create_text_content(itgs, gd=gd, journey=journey)
        with open(text_content_path, "w") as f:
            json.dump(dataclasses.asdict(text_content), f, indent=2)
    else:
        with open(text_content_path, "r") as f:
            text_content = p02.PinterestTextContentForPost.from_dict(json.load(f))

    logging.debug(
        f"Selected text content for pinterest post:\n\n{json.dumps(dataclasses.asdict(text_content), indent=2)}"
    )

    if restore_up_to_excl <= 2 or not os.path.exists(image_prompt_path):
        image_prompt = await p03.create_image_prompt(
            itgs, gd=gd, journey=journey, text_content=text_content
        )
        with open(image_prompt_path, "w") as f:
            json.dump(dataclasses.asdict(image_prompt), f, indent=2)
    else:
        with open(image_prompt_path, "r") as f:
            image_prompt = p03.PinterestImagePrompt.from_dict(json.load(f))

    logging.debug(
        f"Selected image prompt for pinterest post:\n\n{json.dumps(dataclasses.asdict(image_prompt), indent=2)}"
    )

    if restore_up_to_excl <= 3 or not os.path.exists(raw_image_path):
        raw_image = await p04.create_raw_image(
            itgs, gd=gd, prompt=image_prompt, folder=folder
        )
        with open(raw_image_path, "w") as f:
            json.dump(dataclasses.asdict(raw_image), f, indent=2)
    else:
        with open(raw_image_path, "r") as f:
            raw_image = p04.PinterestRawImage.from_dict(json.load(f))

    logging.debug(
        f"Selected raw image for pinterest post:\n\n{json.dumps(dataclasses.asdict(raw_image), indent=2)}"
    )

    if restore_up_to_excl <= 4 or not os.path.exists(final_image_path):
        final_image = await p05.overlay_text_on_image(
            itgs, gd=gd, text=text_content, image=raw_image, folder=folder
        )
        with open(final_image_path, "w") as f:
            json.dump(dataclasses.asdict(final_image), f, indent=2)
    else:
        with open(final_image_path, "r") as f:
            final_image = p05.PinterestFinalImage.from_dict(json.load(f))

    logging.debug(
        f"Selected final image for pinterest post:\n\n{json.dumps(dataclasses.asdict(final_image), indent=2)}"
    )

    result = PinterestJourneyPost(
        journey=journey,
        text_content=text_content,
        image_prompt=image_prompt,
        raw_image=raw_image,
        final_image=final_image,
    )
    logging.info(
        f"Generated pinterest journey post:\n\n{json.dumps(dataclasses.asdict(result), indent=2)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
