FADE_TIME = 3
TITLE_PAUSE_TIME_SECONDS = 3


def standard_audio_fade(duration: int) -> str:
    return f"t=out:st={duration + TITLE_PAUSE_TIME_SECONDS -FADE_TIME}:d={FADE_TIME}:curve=qsin"
