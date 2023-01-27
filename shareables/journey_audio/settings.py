FADE_TIME = 3


def standard_audio_fade(duration: int) -> str:
    return f"t=out:st={duration-FADE_TIME}:d={FADE_TIME}:curve=qsin"
