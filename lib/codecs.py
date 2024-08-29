from dataclasses import dataclass
from typing import Dict, List, Literal, Set, Tuple, TypedDict, Union

H264Profile = Literal["Baseline", "Main", "High", "High 10", "High 4:2:2"]

H264_EXOTIC_PROFILES: Set[H264Profile] = {"High 10", "High 4:2:2"}
"""h264 profiles without broad support"""

# https://www.iana.org/assignments/media-types/video/H264-SVC
# https://developer.mozilla.org/en-US/docs/Web/Media/Formats/codecs_parameter#avc_profiles
H264_PROFILE_NAMES_TO_IDC_HEX_AND_CONSTRAINTS_HEX: Dict[
    H264Profile, Tuple[str, str]
] = {
    "Baseline": ("42", "00"),
    "Main": ("4D", "00"),
    "High": ("64", "00"),
    "High 10": ("6E", "00"),
    "High 4:2:2": ("7A", "00"),
}


class FFProbeVideoStreamForCodecs(TypedDict):
    codec_type: Literal["video"]
    codec_name: Literal["h264"]
    profile: H264Profile
    level: int


class FFProbeAudioStreamForCodecs(TypedDict):
    codec_type: Literal["audio"]
    codec_name: Literal["aac"]


class FFProbeInfoForCodecs(TypedDict):
    streams: List[Union[FFProbeVideoStreamForCodecs, FFProbeAudioStreamForCodecs]]


@dataclass
class H264ProfileForEncoding:
    """Describes an h264 profile with enough details to tell ffmpeg to use it"""

    profile: H264Profile
    """The standard name for the profile"""
    profile_ffmpeg_name: str
    """The name ffmpeg uses for the profile"""


FALLBACK_H264_PROFILE = H264ProfileForEncoding("High", "high")
"""The profile to include additionally if an exotic profile is detected"""


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


def is_video_profile_exotic(codecs: List[str]):
    """Determines if the given video profile is exotic; exotic profiles need
    non-exotic backup options (typically main or high)
    """
    for codec in codecs:
        if codec.startswith("avc1."):
            profile_hex = codec[5:7]
            if profile_hex not in ("42", "4D", "64"):
                return True

            constraints_hex = codec[7:9]
            if constraints_hex != "00":
                return True

    return False


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
