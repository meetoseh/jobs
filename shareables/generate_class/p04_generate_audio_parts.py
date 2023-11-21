import asyncio
import logging
import os
import secrets
from typing import List
from itgs import Itgs
from graceful_death import GracefulDeath
from dataclasses import dataclass
from shareables.generate_class.p03_create_class_meta import GeneratedClassMeta
import requests


is_dev = os.environ["ENVIRONMENT"] == "dev"


@dataclass
class GeneratedClassWithDisjointAudio:
    """A generated class which includes text-to-speech'd audio, but that audio
    is specified as one file per phrase, where the phrases haven't been joined
    together yet.
    """

    meta: GeneratedClassMeta
    audio_parts: List[str]
    """The paths to each audio part, in order"""


async def generate_audio_parts(
    itgs: Itgs, *, folder: str, gd: GracefulDeath, meta: GeneratedClassMeta
) -> GeneratedClassWithDisjointAudio:
    """Generates the audio parts using a text-to-speech api, with one audio
    segment generated per phrase in the transcript.
    """
    user_id = os.environ["OSEH_PLAY_HT_USER_ID"]
    api_key = os.environ["OSEH_PLAY_HT_SECRET_KEY"]

    result: List[str] = []

    for _, phrase in meta.transcript.phrases:
        if gd.received_term_signal:
            raise Exception("recieved term signal")

        logging.info(f"Queueing audio job for phrase {phrase}...")

        response = None
        for start_attempt in range(3):
            response = requests.post(
                "https://play.ht/api/v2/tts",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": f"Bearer {api_key}",
                    "X-User-Id": user_id,
                },
                json={
                    "text": phrase,
                    "voice": "amado",
                    "quality": "high" if is_dev else "premium",
                    "output_format": "mp3",
                    "speed": 1,
                    "sample_rate": 24000,
                },
            )
            if (
                response.status_code >= 500
                and response.status_code <= 599
                and start_attempt < 2
            ):
                logging.warning(
                    f"Received {response.status_code} from play.ht when starting job ({start_attempt+1}/3)..."
                )
                await asyncio.sleep(30)
                continue
            response.raise_for_status()
            break

        assert response is not None
        data: dict = response.json()
        job_id: str = data["id"]
        attempt: int = 0
        while attempt < 40 and (
            data.get("output") is None or not isinstance(data["output"].get("url"), str)
        ):
            if gd.received_term_signal:
                raise Exception("recieved term signal")
            logging.debug(
                f"Waiting for audio job to complete... {attempt+1}/40\n\n{data=}"
            )
            await asyncio.sleep(5)
            response = requests.get(
                f"https://play.ht/api/v2/tts/{job_id}",
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {api_key}",
                    "X-User-Id": user_id,
                },
            )

            if response.status_code >= 500 and response.status_code <= 599:
                logging.warning(
                    f"Received {response.status_code} from play.ht, treating as if still running, but delaying 30s..."
                )
                await asyncio.sleep(30)
                continue

            response.raise_for_status()
            data = response.json()
            attempt += 1

        if data.get("output") is None or not isinstance(data["output"].get("url"), str):
            raise Exception(f"Audio job failed: {data=}")

        audio_url: str = data["output"]["url"]
        outpath: str = os.path.join(folder, f"{secrets.token_urlsafe(16)}.mp3")
        logging.info(f"Downloading result from {audio_url} to {outpath}...")

        response = requests.get(audio_url, stream=True)
        response.raise_for_status()
        with open(outpath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logging.info(f"Downloaded result from {audio_url} to {outpath}.")
        result.append(outpath)

    return GeneratedClassWithDisjointAudio(
        meta=meta,
        audio_parts=result,
    )
