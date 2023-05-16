from dataclasses import dataclass
import secrets
import shutil
from itgs import Itgs
from shareables.generate_class.p05_stitch_audio import GeneratedClassMeta
import logging
import json
import os
import subprocess


@dataclass
class CombinedAudio:
    path: str
    """The path to the audio file with background music and voice combined"""


async def add_background_music(
    itgs: Itgs,
    *,
    meta: GeneratedClassMeta,
    voice_path: str,
    music_path: str,
    folder: str,
) -> CombinedAudio:
    """Produces the combined audio for the class using the voice and background
    music.

    Args:
        itgs (Itgs): the integrations to (re)use
        meta (GeneratedClassMeta): the metadata for the class
        voice_path (str): the path to the voice audio
        music_path (str): the path to the background music
        folder (str): the folder where files can be stored; must exist and
            must be cleaned up by the callee

    Returns:
        CombinedAudio: the path to the combined audio
    """
    outpath = os.path.join(folder, f"voice_with_music-{secrets.token_urlsafe(8)}.wav")

    ffmpeg = shutil.which("ffmpeg")
    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-i",
        voice_path,
        "-i",
        music_path,
        "-filter_complex",
        "amix=duration=first:weights=1 0.05:normalize=0",
        outpath,
    ]
    logging.debug(f"Running command: {json.dumps(cmd)}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise Exception(
            "ffmpeg failed\n\nstdout: ```\n"
            + result.stdout.decode("utf-8")
            + "\n```\n\nstderr: ```\n"
            + result.stderr.decode("utf-8")
            + "\n```"
        )

    return CombinedAudio(path=outpath)
