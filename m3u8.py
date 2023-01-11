"""This module helps process m3u8 files
https://en.wikipedia.org/wiki/M3U
"""
import asyncio
from dataclasses import dataclass
import os
from typing import Dict, List, Literal, Optional, Tuple
import aiofiles


@dataclass
class M3UContent:
    """A single piece of content within an m3u vod"""

    runtime_seconds: float
    """The runtime of this segment in fractional seconds"""

    additional_claims: List[str]
    """any additional claims made about this segment, e.g.,DEFAULT=yes"""

    path: str
    """The path to where this content can be found. Relative paths are relative
    to the VOD file.
    """


@dataclass
class M3UVod:
    """Describes a vod file for m3u8 - a series of audio files available at particular
    locations.
    """

    target_duration: int
    """The target duration of each segment, in seconds. The actual duration of
    each second will not exceed this value after flooring. This corresponds to
    the EXT-X-TARGETDURATION tag.
    """

    claims: Dict[str, str]
    """A dictionary of claims that were found in the vod file, but may or may not
    have been parsed. for example, EXT-X-VERSION
    """

    content: List[M3UContent]
    """The content in order"""


@dataclass
class M3UVodReference:
    """Describes a reference to an m3u vod file from a m3u master file"""

    bandwidth: int
    """The maximum bandwidth of this vod file, in bits per second"""

    codecs: List[str]
    """The codecs used in this vod file"""

    claims: List[Tuple[str, str]]
    """All claims made about this vod file. Quoted strings in values will be unquoted
    and are allowed to contain commas or be unstripped. This is in the order the claims
    were found.
    """

    path: str
    """The path to where this vod file can be found. Relative paths are relative
    to the master file.
    """

    vod: M3UVod
    """The parsed contents of the vod file"""


@dataclass
class M3UPlaylist:
    """Describes a playlist file for m3u8 - which is an m3u8 file that points at
    vod files.
    """

    claims: Dict[str, str]
    """A dictionary of claims that were found in the file, but may or may not
    have been parsed. for example, EXT-X-VERSION
    """

    vods: List[M3UVodReference]
    """The vod files referenced in this playlist, in the order they were found"""


async def parse_m3u_vod(local_filepath: str) -> M3UVod:
    """Parses the m3u vod file at the given location, returning the parsed
    contents.
    """
    state: Literal["start", "x-tags", "content-header", "content-path"] = "start"
    claims: Dict[str, str] = dict()
    content_header_claims: List[str] = None
    content: List[M3UContent] = list()
    async with aiofiles.open(local_filepath, "r") as f:
        async for line in f:
            line = line.strip()
            if line == "":
                continue

            if state == "start":
                if line != "#EXTM3U":
                    raise ValueError(f"Expected #EXTM3U, got {line}")
                state = "x-tags"
                continue

            if state == "x-tags":
                if line.startswith("#EXT-X-"):
                    key, value = line.split(":", 1)
                    claims[key[1:]] = value
                    continue

                state = "content-header"
                # no continue is intentional; we want to process this line as content

            if state == "content-header":
                if line == "#EXT-X-ENDLIST":
                    break

                if not line.startswith("#EXTINF:"):
                    raise ValueError(f"Expected #EXTINF, got {line}")
                _, value = line.split(":", 1)
                content_header_claims = [
                    segment.strip() for segment in value.split(",")
                ]
                state = "content-path"
                if len(content_header_claims) < 1:
                    raise ValueError(f"Expected at least one claim in {line}")
                try:
                    dur = float(content_header_claims[0])
                except ValueError:
                    raise ValueError(
                        f"Expected a number claim for first claim in {line=}, got {content_header_claims[0]=}"
                    )
                if dur <= 0:
                    raise ValueError(
                        f"Expected a positive number claim for first claim in {line=}, got {content_header_claims[0]=}"
                    )
                continue

            assert state == "content-path", f"Unknown {state=}"
            assert (
                content_header_claims is not None
            ), "content_header_claims should not be None"
            if line.startswith("#"):
                raise ValueError(f"expected path, got tag: {line=}")

            content.append(
                M3UContent(
                    runtime_seconds=float(content_header_claims[0]),
                    additional_claims=content_header_claims[1:],
                    path=line,
                )
            )
            state = "content-header"
            content_header_claims = None

    if "EXT-X-TARGETDURATION" not in claims:
        raise ValueError(
            f"EXT-X-TARGETDURATION not found; {local_filepath=}, {claims=}"
        )

    try:
        target_duration = int(claims["EXT-X-TARGETDURATION"])
    except ValueError:
        raise ValueError("EXT-X-TARGETDURATION is not an integer")

    return M3UVod(
        target_duration=target_duration,
        claims=claims,
        content=content,
    )


async def parse_m3u_playlist(local_filepath: str, *, parallel=10) -> M3UPlaylist:
    """Parses the m3u playlist file at the given location, returning the parsed
    contents. As vod files are discovered they are parsed in parallel, with a
    limit of `parallel` files being parsed at once.
    """
    state: Literal["start", "x-tags", "vod-header", "vod-path"] = "start"
    claims: Dict[str, str] = dict()

    pending_vod_refs: List[M3UVodReference] = list()
    # vod refs we haven't yet starting parsing

    tasks_to_vod_refs: Dict[asyncio.Task, M3UVodReference] = dict()
    # running tasks

    vods: List[M3UVodReference] = list()
    # vod refs in the order we have found them, regardless of if we've parsed them yet

    last_vod_claims: Optional[List[Tuple[str, str]]] = None
    # if we're in vod-path, the claims from the last vod header

    last_vod_bandwidth: Optional[int] = None
    # the parsed bandwidth from last_vod_claims

    last_vod_codecs: Optional[List[str]] = None
    # the parsed codecs from last_vod_claims

    async with aiofiles.open(local_filepath, "r") as f:
        async for line in f:
            line = line.strip()
            if line == "":
                continue

            if state == "start":
                if line != "#EXTM3U":
                    raise ValueError(f"Expected #EXTM3U, got {line}")
                state = "x-tags"
                continue

            if state == "x-tags":
                if line.startswith("#EXT-X-") and not line.startswith(
                    "#EXT-X-STREAM-INF:"
                ):
                    key, value = line.split(":", 1)
                    claims[key] = value
                    continue

                state = "vod-header"
                # no continue is intentional; we want to process this line as content

            if state == "vod-header":
                if not line.startswith("#EXT-X-STREAM-INF:"):
                    raise ValueError(f"Expected #EXT-X-STREAM-INF, got {line=}")

                _, value = line.split(":", 1)
                last_vod_claims = []

                quote_chars = "\"'"
                quote_char: Optional[Literal['"', "'"]] = None
                item_sep_char = ","
                kv_sep_char = "="
                expecting_item_sep = False

                cur_key: str = ""
                cur_val: Optional[str] = None
                for char in value:
                    if quote_char is not None:
                        if char == quote_char:
                            quote_char = None
                            last_vod_claims.append((cur_key, cur_val))
                            cur_key = ""
                            cur_val = None
                            expecting_item_sep = True
                            continue
                        cur_val += char
                        continue

                    if expecting_item_sep:
                        if char == " ":
                            continue
                        if char == item_sep_char:
                            expecting_item_sep = False
                            continue
                        raise ValueError(
                            f"Expected {item_sep_char=} or space, got {char=}"
                        )

                    if cur_val == "" and char in quote_chars:
                        quote_char = char
                        continue

                    if char == item_sep_char:
                        last_vod_claims.append((cur_key, cur_val or ""))
                        cur_key = ""
                        cur_val = None
                        continue

                    if cur_key == "" and char == " ":
                        continue

                    if char == kv_sep_char:
                        if cur_val is not None:
                            raise ValueError(
                                f"Expected {cur_val=} to be None since we reached {char=}"
                            )
                        cur_val = ""
                        continue

                    if cur_val is None:
                        cur_key += char
                    else:
                        cur_val += char

                if cur_key != "":
                    last_vod_claims.append((cur_key, cur_val or ""))

                bandwidth_claim: Optional[str] = None
                codecs_claim: Optional[str] = None
                for claim_key, claim in last_vod_claims:
                    if claim_key == "BANDWIDTH":
                        bandwidth_claim = claim
                    elif claim_key == "CODECS":
                        codecs_claim = claim

                if bandwidth_claim is None:
                    raise ValueError(f"Expected BANDWIDTH claim in {line=}")

                if codecs_claim is None:
                    raise ValueError(f"Expected CODECS claim in {line=}")

                try:
                    last_vod_bandwidth = int(bandwidth_claim)
                except ValueError:
                    raise ValueError(f"Expected integer bandwidth in {line=}")

                last_vod_codecs = codecs_claim.split(",")
                state = "vod-path"
                continue

            assert state == "vod-path", f"Unexpected state {state=}"
            assert last_vod_claims is not None, "last_vod_claims is None in vod-path"
            assert (
                last_vod_bandwidth is not None
            ), "last_vod_bandwidth is None in vod-path"
            assert last_vod_codecs is not None, "last_vod_codecs is None in vod-path"

            if line.startswith("#"):
                raise ValueError(f"Expected path, got {line=}")

            new_ref = M3UVodReference(
                bandwidth=last_vod_bandwidth,
                codecs=last_vod_codecs,
                claims=last_vod_claims,
                path=line,
                vod=None,
            )
            pending_vod_refs.append(new_ref)
            vods.append(new_ref)
            if len(tasks_to_vod_refs) >= parallel:
                # let's check if theres room but we just haven't swept in a while
                finished_tasks: List[asyncio.Task] = []
                for running_task in tasks_to_vod_refs:
                    if running_task.done():
                        finished_tasks.append(running_task)
                for finished in finished_tasks:
                    vod_ref = tasks_to_vod_refs.pop(finished)
                    vod_ref.vod = finished.result()

            while len(tasks_to_vod_refs) < parallel and pending_vod_refs:
                vod_ref = pending_vod_refs.pop()
                tasks_to_vod_refs[
                    asyncio.create_task(
                        parse_m3u_vod(
                            get_m3u_vod_local_filepath(local_filepath, vod_ref)
                        )
                    )
                ] = vod_ref

            last_vod_claims = None
            last_vod_bandwidth = None
            last_vod_codecs = None
            state = "vod-header"

    while tasks_to_vod_refs or pending_vod_refs:
        while len(tasks_to_vod_refs) < parallel and pending_vod_refs:
            vod_ref = pending_vod_refs.pop()
            tasks_to_vod_refs[
                asyncio.create_task(parse_m3u_vod(vod_ref.path))
            ] = vod_ref

        done, _ = await asyncio.wait(
            tasks_to_vod_refs.keys(), return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            vod_ref = tasks_to_vod_refs.pop(task)
            vod_ref.vod = task.result()

    assert all(
        vod_ref.vod is not None for vod_ref in vods
    ), f"Some vods are None!: {vods=}"
    return M3UPlaylist(
        claims=claims,
        vods=vods,
    )


def get_m3u_vod_local_filepath(master: str, vod: M3UVodReference) -> str:
    """Gets the path to the vod file from a path specified in the master playlist"""
    return os.path.join(os.path.dirname(master), vod.path)


def get_m3u_local_filepath(
    master: str,
    vod: M3UVodReference,
    part: M3UContent,
) -> str:
    """Gets the path to the content file from a path specified within a vod"""
    return os.path.join(
        os.path.dirname(os.path.join(os.path.dirname(master), vod.path)), part.path
    )
