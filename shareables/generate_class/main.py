import logging
import os
from typing import Optional
from graceful_death import GracefulDeath
from itgs import Itgs
import shareables.generate_class.p01_select_classes as p01
import shareables.generate_class.p02_generate_transcripts as p02
import shareables.generate_class.p03_create_class_meta as p03
import shareables.generate_class.p04_generate_audio_parts as p04
import shareables.generate_class.p05_stitch_audio as p05
import shareables.generate_class.p06_generate_background_music as p06
import shareables.generate_class.p07_add_background_music as p07
import argparse
import asyncio
import json
import dataclasses


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inputs",
        type=str,
        default=os.path.join("tmp", "generate_class.json"),
        help="The path the input classes to the pipeline",
    )
    parser.add_argument(
        "--no-inputs",
        action="store_true",
        help="If set, the pipeline will instead select classes from the database",
    )
    parser.add_argument(
        "--reuse-meta",
        action="store_true",
        help="If set, if meta information was generated on a previous run, its reused for step 3",
    )
    parser.add_argument(
        "--reuse-voice",
        action="store_true",
        help="If set, if voice audio was generated on a previous run, its reused for steps 4-5",
    )
    parser.add_argument(
        "--reuse-music",
        action="store_true",
        help="If set, if background music was generated on a previous run, its reused for step 6",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)

    inputs_path = args.inputs
    if args.no_inputs:
        inputs_path = None

    asyncio.run(
        run_pipeline(
            gd=GracefulDeath(),
            inputs_path=inputs_path,
            reuse_meta=args.reuse_meta,
            reuse_voice=args.reuse_voice,
            reuse_music=args.reuse_music,
        )
    )


def parse_inputs_from_file(path: str) -> p01.SelectedJourneys:
    """Parses the journeys to use from the json file at the given path,
    replacing the standard part 1 of this pipeline. The file should be formatted
    as

    ```json
    {
        "category": "string",
        "emotion": "string",
        "classes": [
            {
            "title": "string",
            "description": "string",
            "instructor": "string",
            "prompt": {
                "style": "word",
                "text": "string",
                "options": ["string"]
            },
            "audio_path": "string"
            }
        ]
    }
    ```
    """
    with open(path) as f:
        data = json.load(f)

    return p01.SelectedJourneys(
        category=data["category"],
        emotion=data["emotion"],
        journeys=[
            p01.InputJourney(
                title=journey["title"],
                description=journey["description"],
                instructor=journey["instructor"],
                prompt=p01.prompt_from_dict(journey["prompt"]),
                audio_path=journey["audio_path"],
            )
            for journey in data["classes"]
        ],
    )


@dataclasses.dataclass
class GenerateClassResult:
    meta: p03.GeneratedClassMeta
    """Meta information we decided on when creating the audio"""
    music: p06.Music
    """The background music applied to the task"""
    path: str
    """The path to where the final class audio is located"""


async def run_pipeline(
    *,
    gd: GracefulDeath,
    inputs_path: Optional[str] = None,
    dest_folder: str = os.path.join("tmp", "shareables", "generate_class"),
    reuse_meta: bool = False,
    reuse_voice: bool = False,
    reuse_music: bool = False,
):
    os.makedirs(dest_folder, exist_ok=True)
    generated_meta_path = os.path.join(dest_folder, "meta.json")
    generated_voice_path = os.path.join(dest_folder, "voice.json")
    generated_music_path = os.path.join(dest_folder, "music.json")
    async with Itgs() as itgs:
        inputs = (
            parse_inputs_from_file(inputs_path)
            if inputs_path is not None
            else await p01.select_classes(itgs, folder=dest_folder, gd=gd)
        )
        logging.debug(f"Selected inputs: {inputs=}, augmenting with transcripts...")
        inputs = await p02.generate_transcripts(itgs, journeys=inputs, gd=gd)
        logging.debug(f"Augmented inputs: {inputs=}, creating class meta...")
        if reuse_meta and os.path.exists(generated_meta_path):
            with open(generated_meta_path, "r") as f:
                data = json.load(f)
            generated_class_meta = p03.GeneratedClassMeta(
                title=data["title"],
                description=data["description"],
                instructor=data["instructor"],
                prompt=p01.prompt_from_dict(data["prompt"]),
                transcript=p03.parse_vtt_transcript("WEBVTT\n\n" + data["transcript"]),
            )
        else:
            generated_class_meta = await p03.create_class_meta(
                itgs, input=inputs, gd=gd
            )
            with open(generated_meta_path, "w") as f:
                json.dump(
                    {
                        "title": generated_class_meta.title,
                        "description": generated_class_meta.description,
                        "instructor": generated_class_meta.instructor,
                        "prompt": dataclasses.asdict(generated_class_meta.prompt),
                        "transcript": str(generated_class_meta.transcript),
                    },
                    f,
                    indent=4,
                )

        if reuse_voice and os.path.exists(generated_voice_path):
            with open(generated_voice_path, "r") as f:
                raw_voice_data = json.load(f)

            voice_path = raw_voice_data["path"]
            stitched_class = p05.GeneratedClassWithJustVoice(
                meta=generated_class_meta, voice_audio=voice_path
            )
        else:
            logging.debug(
                f"Created class meta, producing disjoint audio using text-to-speech..."
            )
            disjoint_class = await p04.generate_audio_parts(
                itgs, folder=dest_folder, gd=gd, meta=generated_class_meta
            )
            logging.info(f"{disjoint_class=}")
            stitched_class = await p05.stitch_audio(
                itgs, folder=dest_folder, disjoint=disjoint_class
            )

            with open(generated_voice_path, "w") as f:
                json.dump(
                    {"path": stitched_class.voice_audio},
                    f,
                    indent=4,
                )

        logging.info(f"{stitched_class=}")

        if reuse_music and os.path.exists(generated_music_path):
            with open(generated_music_path, "r") as f:
                raw_music_data = json.load(f)

            background_music = p06.Music(
                path=raw_music_data["path"],
                attribution=[
                    p06.MusicAttribution(
                        text=attribution["text"],
                        by_str=attribution["by_str"],
                        url=attribution["url"],
                    )
                    for attribution in raw_music_data["attribution"]
                ],
                duration=raw_music_data["duration"],
            )
        else:
            logging.debug("Created voice, generating background music...")
            background_music = await p06.generate_background_music(
                itgs, gd=gd, voice=stitched_class, folder=dest_folder
            )

            with open(generated_music_path, "w") as f:
                json.dump(
                    dataclasses.asdict(background_music),
                    f,
                    indent=4,
                )

        if gd.received_term_signal:
            logging.info("Graceful death received, exiting...")
            return
        logging.debug("Adding background music to voice...")
        combined_audio = await p07.add_background_music(
            itgs,
            meta=generated_class_meta,
            voice_path=stitched_class.voice_audio,
            music_path=background_music.path,
            folder=dest_folder,
        )
        logging.info(f"{generated_class_meta=}\n\n{combined_audio=}")
        return GenerateClassResult(
            meta=generated_class_meta,
            music=background_music,
            path=combined_audio.path,
        )


if __name__ == "__main__":
    main()
