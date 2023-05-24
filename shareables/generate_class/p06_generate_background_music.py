import json
import logging
import os
import secrets
from typing import Any, Coroutine, List, Literal, Optional
from graceful_death import GracefulDeath
from itgs import Itgs
from abc import ABC, abstractmethod
from dataclasses import dataclass
from lib.redis_api_limiter import ratelimit_using_redis
from shareables.generate_class.p03_create_class_meta import GeneratedClassMeta
from shareables.generate_class.p05_stitch_audio import (
    AudioInfo,
    GeneratedClassWithJustVoice,
    concat_audio_files,
    get_audio_info,
    pad_end_with_silence,
)
from shareables.shareable_pipeline_exception import ShareablePipelineException
import shutil
import subprocess
import requests
from urllib.parse import urlencode


class MusicError(ShareablePipelineException):
    def __init__(self, msg: str):
        super().__init__(msg, step_number=6, step_name="add_background_music")


@dataclass
class MusicPrompt:
    """A prompt for generating music"""

    prompt: str
    """The prompt to use"""

    min_duration: float
    """The minimum duration the music should be in seconds"""


@dataclass
class MusicAttribution:
    text: str
    """The text to use when attributing the music, pre-formatted, long"""

    by_str: str
    """If you want to attribution with a by string, e.g., "by lemonmusicstudio on Pixabay",
    the by-str to use
    """

    url: Optional[str] = None
    """If there is a url that can also be linked to for attribution, the url, otherwise
    None
    """


@dataclass
class Music:
    path: str
    """The path to the music file, locally"""

    attribution: List[MusicAttribution]
    """The attributions for the music, in order of appearance"""

    duration: float
    """The duration of the music file in seconds"""

    fade_after: Optional[float] = None
    """If set and there is music generated after this, then the music will crossfade
    into the next music file for the given duration in seconds
    """

    delay_after: Optional[float] = None
    """If set and there is music generated after this, then the music will be silent
    for the given duration in seconds before the next music file starts. Applied before
    the crossfade when both are set.
    """


class MusicGenerator(ABC):
    @abstractmethod
    async def generate_prompts(
        self, itgs: Itgs, *, voice: GeneratedClassWithJustVoice, voice_info: AudioInfo
    ) -> Coroutine[Any, Any, List[MusicPrompt]]:
        """Generates a new list of prompts of the music to use for the given class.
        The music will be generated using generate_music in order of the prompts until
        the entire class has background music.
        """
        raise NotImplementedError()

    @abstractmethod
    async def generate_music(
        self, itgs: Itgs, *, prompt: MusicPrompt, folder: str
    ) -> List[Music]:
        """Generates a new music file that is at least `min_duration` seconds long.
        May consist of multiple parts, since depending on the music generator a minimum
        duration cannot be enforced
        """
        raise NotImplementedError()


class CCMixterMusicGenerator(MusicGenerator):
    async def generate_prompts(
        self, itgs: Itgs, *, voice: GeneratedClassWithJustVoice, voice_info: AudioInfo
    ) -> List[MusicPrompt]:
        # There's not enough content in ccmixter to do any kind of filtering beyond
        # the bare minimum, so we just return a single, fixed prompt
        return [MusicPrompt(prompt="instrumental", min_duration=voice_info.duration)]

    async def generate_music(
        self, itgs: Itgs, *, prompt: MusicPrompt, folder: str
    ) -> List[Music]:
        result: List[Music] = []
        result_duration = 0
        while result_duration < prompt.min_duration:
            await ratelimit_using_redis(
                itgs, key="external_apis:api_limiter:ccmixter", time_between_requests=3
            )
            listing_url = "https://ccmixter.org/api/query?" + urlencode(
                {
                    "tags": prompt.prompt,
                    "limit": "1",
                    "rand": "1",
                    "f": "js",
                    "lic": "by",
                    "score": "10",
                }
            )
            response = requests.get(listing_url)
            response.raise_for_status()
            data = response.json()

            upload = data[0]
            name = upload["upload_name"]
            user_name = upload["user_name"]
            artist_page_url = upload["artist_page_url"]
            file = upload["files"][0]
            file_name = file["file_name"]
            download_url = file["download_url"]

            logging.debug(
                f"Downloading {name} by {user_name} via {download_url} ({file_name})..."
            )
            await ratelimit_using_redis(
                itgs, key="external_apis:api_limiter:ccmixter", time_between_requests=30
            )

            outpath = os.path.join(folder, f"{secrets.token_urlsafe(2)}-{file_name}")
            response = requests.get(
                download_url,
                headers={
                    "Accept": "*/*",
                    "Accept-Encoding": "identity;q=1, *;q=0",
                    "Referer": listing_url,
                    "User-Agent": "tj@oseh.com; videos with attribution; /r/oseh",
                },
                stream=True,
            )
            response.raise_for_status()

            with open(outpath, "wb") as f:
                for chunk in response:
                    f.write(chunk)

            info = get_audio_info(outpath)
            if info.duration < 7:
                logging.debug(
                    f"Skipping song {name} by {user_name} because it's too short (<7 seconds)"
                )
                continue

            result_duration += info.duration
            if result:
                result[-1].fade_after = 5
                result_duration -= 5
            result.append(
                Music(
                    path=outpath,
                    attribution=[
                        MusicAttribution(
                            text=f"{name} by {user_name} on ccmixter.org",
                            by_str=f"by {user_name} on ccmixter.org",
                            url=artist_page_url,
                        )
                    ],
                    duration=info.duration,
                    fade_after=None,
                    delay_after=None,
                )
            )
        return result


MusicGenerationMethod = Literal["ccmixter"]


def create_music_generator(
    itgs: Itgs, *, method: MusicGenerationMethod
) -> MusicGenerator:
    """Creates a music generator for the given method"""
    if method == "ccmixter":
        return CCMixterMusicGenerator()

    raise ValueError(f"unknown music generator {method=}")


def combine_using_crossfade(*, segments: List[Music], folder: str, out: str):
    """Combines all of the specified segments into a single audio file. Requires
    that all of the segments except the last one indicate a crossfade duration,
    which is applied.

    Args:
        segments (list[Music]): the segments to combine. At least 2 segments,
            and all but the last one must have a crossfade duration
        folder (str): the folder to save temporary files to
        out (str): the path to save the combined file to
    """
    assert len(segments) >= 2
    assert all(
        segment.fade_after is not None for segment in segments[:-1]
    ), f"{segments=}"

    if len(segments) == 2:
        filter_complex = f"acrossfade=d={segments[0].fade_after:.3f}:c1=tri:c2=tri"
    elif len(segments) == 3:
        filter_complex = (
            f"[0][1]acrossfade=d={segments[0].fade_after:.3f}:c1=tri:c2=tri[a01]; "
            f"[a01][2]acrossfade=d={segments[1].fade_after:.3f}:c1=tri:c2=tri"
        )
    else:
        filter_complex = f"[0][1]acrossfade=d={segments[0].fade_after:.3f}:c1=tri:c2=tri[a01]; " + (
            "; ".join(
                f"[a{idx:02d}][{idx + 1}]acrossfade=d={segment.fade_after:.3f}:c1=tri:c2=tri[a{idx + 1:02d}]"
                for idx, segment in zip(range(1, len(segments) - 2), segments[1:-2])
            )
            + f"; [a{len(segments) - 2:02d}][{len(segments) - 1}]acrossfade=d={segments[-2].fade_after:.3f}:c1=tri:c2=tri"
        )

    ffmpeg = shutil.which("ffmpeg")
    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        *sum(
            (
                [
                    "-i",
                    segment.path,
                ]
                for segment in segments
            ),
            start=[],
        ),
        "-vn",
        "-filter_complex",
        filter_complex,
        out,
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


async def combine_music_segments(
    itgs: Itgs, *, segments: List[Music], folder: str
) -> Music:
    """Combines all the given segments into a single segment with a longer list
    of attributions and no delay/cross-fade

    Arguments:
        itgs (Itgs): the integrations to (re)use
        segments (list[Music]): the segments to combine
        folder (str): the folder to save files to. multiple temporary files may
            be needed, the callee is responsible for cleaning them up
    """
    if not segments:
        raise ValueError("cannot combine empty list of segments")

    padded_segments: List[Music] = []
    for segment in segments:
        if segment.delay_after is None:
            padded_segments.append(segment)
        else:
            tmp_path = os.path.join(folder, f"{secrets.token_urlsafe(16)}.wav")
            pad_end_with_silence(
                segment.path, duration=segment.delay_after, output_path=tmp_path
            )
            padded_segments.append(
                Music(
                    path=tmp_path,
                    attribution=segment.attribution,
                    duration=segment.duration + segment.delay_after,
                    fade_after=segment.fade_after,
                    delay_after=None,
                )
            )

    current = padded_segments
    while len(current) > 1:
        if current[0].fade_after is not None:
            fade_until = 1
            while (
                len(current) > fade_until + 2
                and current[fade_until].fade_after is not None
            ):
                fade_until += 1

            combined_path = os.path.join(folder, f"{secrets.token_urlsafe(16)}.wav")
            logging.debug(f"Combining first {fade_until+1} segments using crossfade...")
            combine_using_crossfade(
                segments=current[: fade_until + 1], folder=folder, out=combined_path
            )
            new_segment = Music(
                path=combined_path,
                attribution=sum(
                    (segm.attribution for segm in current[: fade_until + 1]), start=[]
                ),
                duration=get_audio_info(combined_path).duration,
                fade_after=None,
                delay_after=None,
            )
            current = [new_segment] + current[fade_until + 1 :]
        else:
            concat_until = 1
            while (
                len(current) > concat_until + 2
                and current[concat_until].fade_after is None
            ):
                concat_until += 1

            combined_path = os.path.join(folder, f"{secrets.token_urlsafe(16)}.wav")
            logging.debug(f"Combining first {concat_until+1} segments using concat...")
            concat_audio_files(
                paths=[segment.path for segment in current[: concat_until + 1]],
                out=combined_path,
                folder=folder,
            )
            new_segment = Music(
                path=combined_path,
                attribution=sum(
                    (segm.attribution for segm in current[: concat_until + 1]), start=[]
                ),
                duration=get_audio_info(combined_path).duration,
                fade_after=None,
                delay_after=None,
            )
            current = [new_segment] + current[concat_until + 1 :]

    logging.debug(f"Finished combining segments, returning {current[0]}")
    return current[0]


async def generate_background_music(
    itgs: Itgs,
    *,
    gd: GracefulDeath,
    voice: GeneratedClassWithJustVoice,
    folder: str,
    method: MusicGenerationMethod = "ccmixter",
    max_attempts_per_prompt: int = 2,
    max_prompt_attempts: int = 5,
) -> Music:
    """Generates background music which can be used for the given class.

    Args:
        itgs (Itgs): the integrations to (re)use
        gd (GracefulDeath): Used to stop early when receiving a term signal
        voice (GeneratedClassWithJustVoice): the class to add background music to
        folder (str): the folder to store files in, if needed. the callee is responsible
            for cleaning this folder up
        method ("pixabay"): the method to use to generate audio. options:
            - "pixabay": searches Pixabay public, royalty-free music. Uses chatgpt
              to determine search term. Always enabled safessearch and orders by
              popular, but takes a offset in the top 40 results.
        max_attempts_per_prompt (int): the maximum number of attempts to generate
            music for a given prompt
        max_prompt_attempts (int): the maximum number of prompts to try before
            giving up on generating music for the class, raising an error.

    Returns:
        The music which can be added to the class. Should be at least as long
        as the class, though it may be longer.
    """
    voice_info = get_audio_info(voice.voice_audio)
    generator = create_music_generator(itgs, method=method)

    last_error: Optional[Exception] = None
    segments: Optional[List[Music]] = None

    for prompt_attempt in range(max_prompt_attempts):
        if gd.received_term_signal:
            raise Exception("received term signal")
        logging.debug(
            f"Generating prompt for music, attempt {prompt_attempt+1}/{max_prompt_attempts}..."
        )
        try:
            prompts = await generator.generate_prompts(
                itgs, voice=voice, voice_info=voice_info
            )
            logging.debug(
                f"Generated {len(prompts)} prompts for music, generating music...\n\n{prompts}"
            )
        except Exception as e:
            last_error = e
            logging.exception(
                f"Attempt to generate music prompt {prompt_attempt+1}/{max_prompt_attempts} failed",
                exc_info=True,
            )
            continue

        partial_segments: List[Music] = []
        for prompt in prompts:
            for music_attempt in range(max_attempts_per_prompt):
                if gd.received_term_signal:
                    raise Exception("received term signal")
                logging.debug(
                    f"Generating music, attempt {music_attempt+1}/{max_attempts_per_prompt}..."
                )
                try:
                    segment = await generator.generate_music(
                        itgs, prompt=prompt, folder=folder
                    )
                    logging.debug(f"Generated music: {segment}")
                    partial_segments.extend(segment)
                    break
                except Exception as e:
                    last_error = e
                    logging.exception(
                        f"Attempt to generate music with prompt {prompt} {music_attempt+1}/{max_attempts_per_prompt} failed",
                        exc_info=True,
                    )
                    continue
            else:
                break
        else:
            segments = partial_segments
            break
    else:
        err_msg = f"Failed to generate music for class after {max_prompt_attempts=}, {max_attempts_per_prompt=}"
        if last_error is None:
            raise MusicError(err_msg)
        else:
            raise MusicError(err_msg) from last_error

    logging.debug(
        f"Generated {len(segments)} segments of music, combining into one music segment..."
    )
    music = await combine_music_segments(itgs, segments=segments, folder=folder)
    logging.info(f"Combined music segment: {music}")
    return music
