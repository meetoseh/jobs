from typing import List, Literal, Set, TypedDict, Union

# https://www.iana.org/assignments/media-types/video/H264-SVC
# https://developer.mozilla.org/en-US/docs/Web/Media/Formats/codecs_parameter#avc_profiles
H264_PROFILE_NAMES_TO_IDC_HEX_AND_CONSTRAINTS_HEX = {
    "Baseline": ("42", "00"),
    "Main": ("4D", "00"),
    "High": ("64", "00"),
    "High 10": ("6E", "00"),
}


class FFProbeVideoStreamForCodecs(TypedDict):
    codec_type: Literal["video"]
    codec_name: Literal["h264"]
    profile: Literal["Baseline", "Main", "High"]
    level: int


class FFProbeAudioStreamForCodecs(TypedDict):
    codec_type: Literal["audio"]
    codec_name: Literal["aac"]


class FFProbeInfoForCodecs(TypedDict):
    streams: List[Union[FFProbeVideoStreamForCodecs, FFProbeAudioStreamForCodecs]]


def determine_codecs_from_probe(info: FFProbeInfoForCodecs) -> List[str]:
    """Determines the unique codecs from the result of the given ffprobe,
    in RFC 6381 format. Only a limited set of codecs are supported.
    """

    result: Set[str] = set()
    for stream in info["streams"]:
        if stream["codec_type"] == "audio":
            result.add(_determine_audio_codec_for_stream(stream))
        elif stream["codec_type"] == "video":
            result.add(_determine_video_codec_for_stream(stream))
    return list(result)


def _determine_audio_codec_for_stream(stream: FFProbeAudioStreamForCodecs) -> str:
    # mp4a is referring to, I believe, just how we are going to specify
    # the codec. 40 means aac, and 2 means AAC-LC (Low Complexity), which
    # supports all the profiles essentially, so seems like we can't go wrong
    # with hinting it.
    assert stream["codec_name"] == "aac"
    return "mp4a.40.2"


def _determine_video_codec_for_stream(stream: FFProbeVideoStreamForCodecs) -> str:
    # avc1 is AVC (H.264)

    # the second element is referred to as 'avcoti' in the formal syntax
    # is the hexadecimal representation of the following three bytes
    # - profile_idc
    # - constraint_set flags
    # - level_idc
    assert stream["codec_name"] == "h264"
    (
        profile_idc_hex,
        constraints_hex,
    ) = H264_PROFILE_NAMES_TO_IDC_HEX_AND_CONSTRAINTS_HEX[stream["profile"]]
    level_decimal = stream["level"]
    level_hex = f"{level_decimal:02x}"
    return f"avc1.{profile_idc_hex}{constraints_hex}{level_hex}"
