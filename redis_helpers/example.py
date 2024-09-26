"""The latest template for making a new helper. Replace "Example" with your UpperCamelCase name 
and "example" with your snake_case_name
"""

import hashlib
from typing import Any, Literal, Union, Optional, TypedDict, TYPE_CHECKING
from itgs import Itgs
import lib.redis_scripts
import redis.asyncio.client
from dataclasses import dataclass

from redis_helpers.run_with_prep import run_with_prep

if TYPE_CHECKING:
    from typing import Unpack


_SCRIPT = """
"""
_SCRIPT_HASH = hashlib.sha1(_SCRIPT.encode("utf-8")).hexdigest()

ensure_example_script_exists = lib.redis_scripts.make_partial_ensure_script_exists(
    script_name="example", script=_SCRIPT, script_hash=_SCRIPT_HASH
)


@dataclass
class ExampleResultSuccess:
    type: Literal["success"]
    """
    - `success`: the operation completed successfully
    """


@dataclass
class ExampleResultFailure:
    type: Literal["failure"]
    """
    - `failure`: the operation failed
    """


ExampleResult = Union[
    ExampleResultSuccess,
    ExampleResultFailure,
]


class ExampleParams(TypedDict):
    key1: bytes
    """key 1"""
    arg1: bytes
    """arg 1"""


async def example(
    redis: redis.asyncio.client.Redis, /, **params: "Unpack[ExampleParams]"
) -> Optional[ExampleResult]:
    """Does X

    Args:
        redis (redis.asyncio.client.Redis): The redis client or pipeline to invoke the
            script within.
        **params (ExampleParams): The parameters for the script

    Returns:
        (ExampleResult, None): The result, if not run within a pipeline, otherwise None
            as the result is not known until the pipeline is executed.

    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    result = await redis.evalsha(
        _SCRIPT_HASH,
        1,
        params["key1"],  # type: ignore
        params["arg1"],  # type: ignore
    )
    if result is redis:
        return None
    return parse_example_response(result)


async def safe_example(
    itgs: Itgs, /, **params: "Unpack[ExampleParams]"
) -> ExampleResult:
    """Much like example, but runs in the primary redis instance from the given
    integrations. Since this is definitely not within a pipeline, it can verify
    the script exists and ensure it provides a result.

    Args:
        itgs (Itgs): the integrations to (re)use
        **params (ExampleParams): The parameters for the script

    Returns:
        ExampleResult: what the script returned, already parsed
    """
    redis = await itgs.redis()

    async def _prepare(force: bool):
        await ensure_example_script_exists(redis, force=force)

    async def _execute():
        return await example(redis, **params)

    res = await run_with_prep(_prepare, _execute)
    assert res is not None
    return res


def parse_example_response(response: Any) -> ExampleResult:
    """Parses the result of the example script into the typed representation"""
    assert isinstance(response, int), f"{response=}"
    if response == 1:
        return ExampleResultSuccess(type="success")
    if response == -1:
        return ExampleResultFailure(type="failure")
    raise ValueError(f"Unknown response: {response}")
