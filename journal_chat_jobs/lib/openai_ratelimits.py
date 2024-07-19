"""Simulates OpenAI's ratelimits from our perspective, so we don't get actual 429
responses
"""

import time
from itgs import Itgs
from typing import List, Literal, Union
from dataclasses import dataclass

from redis_helpers.reserve_openai import ReserveOpenAIArg, safe_reserve_openai


@dataclass
class OpenAIRatelimitBucket:
    cap: int
    """The maximum number that can be held at once"""
    interval: int
    """The interval between refills in seconds"""
    refill: int
    """How many tokens are refilled every interval"""


@dataclass
class OpenAIRatelimitInfo:
    """Describes a ratelimit"""

    requests: List[OpenAIRatelimitBucket]
    """How many requests"""
    tokens: List[OpenAIRatelimitBucket]
    """How many tokens"""


OpenAICategory = Literal["gpt-4o", "gpt-4o-mini"]

# These are based on the actual ratelimits, but also modified to avoid excessive
# burstiness (e.g., consuming the entire 1 minute bucket in 5s)
OPENAI_RATELIMITS_BY_CATEGORY = {
    "gpt-4o": OpenAIRatelimitInfo(
        requests=[
            OpenAIRatelimitBucket(5_000, 60, 5_000),  # actual limit
            OpenAIRatelimitBucket(100, 1, 100),  # don't burst more than 100 req/s
        ],
        tokens=[
            OpenAIRatelimitBucket(800_000, 60, 800_000),  # actual limit
            OpenAIRatelimitBucket(20_000, 1, 20_000),  # don't burst more than 20k tok/s
        ],
    ),
    "gpt-4o-mini": OpenAIRatelimitInfo(
        requests=[
            OpenAIRatelimitBucket(5_000, 60, 5_000),  # actual limit
            OpenAIRatelimitBucket(100, 1, 100),  # don't burst more than 100 req/s
        ],
        tokens=[
            OpenAIRatelimitBucket(160_000, 60, 160_000),  # actual limit
            # top-4 requires 6 comparisons at 2048 tokens each; free+pro simultaneous
            # is 12 comparisons or 24,576 burst tokens
            OpenAIRatelimitBucket(
                28_000, 1, 4_000
            ),  # don't burst more than 4k tok/s for 7s
        ],
    ),
}


@dataclass
class ReserveOpenAITokensResultImmediate:
    type: Literal["immediate"]
    """
    - `immediate`: you are good to send the request immediately
    """


@dataclass
class ReserveOpenAITokensResultWait:
    type: Literal["wait"]
    """
    - `wait`: you should wait for `wait_seconds` seconds before sending the request
    """
    wait_seconds: int


@dataclass
class ReserveOpenAITokensResultFail:
    type: Literal["fail"]
    """
    - `fail`: you should not send the request as you'd have to wait too long. We did
      not actually reserve the tokens.
    """


ReserveOpenAITokensResult = Union[
    ReserveOpenAITokensResultImmediate,
    ReserveOpenAITokensResultWait,
    ReserveOpenAITokensResultFail,
]


async def reserve_openai_tokens(
    itgs: Itgs,
    /,
    *,
    category: OpenAICategory,
    count: int,
    max_wait_seconds: int,
) -> ReserveOpenAITokensResult:
    """Acts as a simulation of openai's ratelimits for the given category, so that
    we can determine if we will be ratelimited and for how long, then wait that
    amount of time before making the request.

    In order for this to work, all openai requests within the given category
    must go through this endpoint and respect the returned wait time.

    Args:
        itgs (Itgs): the integrations to (re)use
        category (OpenAICategory): the category of the request
        count (int): the max_tokens of the request
        max_wait_seconds (int): the maximum number of seconds you are willing to
            wait.
    """
    info = OPENAI_RATELIMITS_BY_CATEGORY[category]
    reservations: List[ReserveOpenAIArg] = []
    for idx, bucket in enumerate(info.requests):
        reservations.append(
            ReserveOpenAIArg(
                key=f"openai:ratelimits:{category}:requests:{idx}",
                cap=bucket.cap,
                interval=bucket.interval,
                refill=bucket.refill,
                min_balance=-(max_wait_seconds // bucket.interval) * bucket.refill,
                amt=1,
            )
        )
    for idx, bucket in enumerate(info.tokens):
        reservations.append(
            ReserveOpenAIArg(
                key=f"openai:ratelimits:{category}:tokens:{idx}",
                cap=bucket.cap,
                interval=bucket.interval,
                refill=bucket.refill,
                min_balance=-(max_wait_seconds // bucket.interval) * bucket.refill,
                amt=count,
            )
        )

    request_at = int(time.time())
    result = await safe_reserve_openai(itgs, now=request_at, args=reservations)
    if result.type == "success":
        if result.wait_time <= 0:
            return ReserveOpenAITokensResultImmediate(type="immediate")
        return ReserveOpenAITokensResultWait(type="wait", wait_seconds=result.wait_time)
    return ReserveOpenAITokensResultFail(type="fail")
