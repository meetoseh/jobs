import json
import logging
from typing import Any, Dict, Optional, List, Tuple, Union
import hashlib
import time
import redis.asyncio.client
from dataclasses import dataclass

from lib.touch.link_info import TouchLink, TouchLinkBufferedClick

TOUCH_READ_TO_PERSIST_LUA_SCRIPT = """
local persistable_key = KEYS[1]
local buffer_key = KEYS[2]
local start_idx_incl = tonumber(ARGV[1])
local end_idx_incl = tonumber(ARGV[2])

local codes = redis.call("ZRANGE", persistable_key, start_idx_incl, end_idx_incl)
local result = {}

for i = 1, #codes do
    local code = codes[i]
    table.insert(result, code)
    table.insert(
        result,
        redis.call(
            "HMGET", 
            buffer_key .. ':' .. code, 
            "uid", "code", "touch_uid", "page_identifier", 
            "page_extra", "preview_identifier", "preview_extra", 
            "created_at"
        )
    )
    table.insert(
        result,
        redis.call("LRANGE", buffer_key .. ':clicks:' .. code, 0, -1)
    )
end

return result
"""

TOUCH_READ_TO_PERSIST_LUA_SCRIPT_HASH = hashlib.sha1(
    TOUCH_READ_TO_PERSIST_LUA_SCRIPT.encode("utf-8")
).hexdigest()


_last_touch_read_to_persist_ensured_at: Optional[float] = None


async def ensure_touch_read_to_persist_script_exists(
    redis: redis.asyncio.client.Redis, *, force: bool = False
) -> None:
    """Ensures the touch_read_to_persist lua script is loaded into redis."""
    global _last_touch_read_to_persist_ensured_at

    now = time.time()
    if (
        not force
        and _last_touch_read_to_persist_ensured_at is not None
        and (now - _last_touch_read_to_persist_ensured_at < 5)
    ):
        return

    loaded: List[bool] = await redis.script_exists(
        TOUCH_READ_TO_PERSIST_LUA_SCRIPT_HASH
    )
    if not loaded[0]:
        correct_hash = await redis.script_load(TOUCH_READ_TO_PERSIST_LUA_SCRIPT)
        assert (
            correct_hash == TOUCH_READ_TO_PERSIST_LUA_SCRIPT_HASH
        ), f"{correct_hash=} != {TOUCH_READ_TO_PERSIST_LUA_SCRIPT_HASH=}"

    if (
        _last_touch_read_to_persist_ensured_at is None
        or _last_touch_read_to_persist_ensured_at < now
    ):
        _last_touch_read_to_persist_ensured_at = now


@dataclass
class TouchReadToPersistResult:
    code: str
    data: Optional[TouchLink]
    clicks: List[TouchLinkBufferedClick]


async def touch_read_to_persist(
    redis: redis.asyncio.client.Redis,
    persistable_key: Union[str, bytes],
    buffer_key: Union[str, bytes],
    start_idx_incl: int,
    end_idx_incl: int,
) -> Optional[List[TouchReadToPersistResult]]:
    """Reads the given range from the given sorted set using various keys
    related to the buffer. This is tailored for the touch_links:to_persist
    sorted set, though it would actually be used on the
    touch_links:persist_purgatory (which has the same structure)

    Args:
        redis (redis.asyncio.client.Redis): The redis client
        persistable_key (str, bytes): The key of the sorted set to read from,
            usually either `touch_links:to_persist` or `touch_links:persist_purgatory`
        buffer_key (str, bytes): The key prefix for the buffer, e.g., `touch_links:buffer`.
            We will read from `{buffer_key}:{code}` and `{buffer_key}:clicks:{code}`
        start_idx_incl (int): The start index, inclusive. can be negative for from the right
        end_idx_incl (int): The end index, inclusive. can be negative for from the right


    Returns:
        list[
            TouchReadToPersistResult
        ] or None: None if executed within a pipeline since the result isn't known until
            the pipeline is executed. Otherwise, the items within the given range, where
            each item is the code, data, and clicks for the item. Parsed as if via
            `touch_read_to_persist_parse_result`.


    Raises:
        NoScriptError: If the script is not loaded into redis
    """
    res = await redis.evalsha(
        TOUCH_READ_TO_PERSIST_LUA_SCRIPT_HASH,
        2,
        persistable_key,
        buffer_key,
        start_idx_incl,
        end_idx_incl,
    )
    if res is redis:
        return None
    return touch_read_to_persist_parse_result(res)


def touch_read_to_persist_parse_result(
    res: List[Any],
) -> List[TouchReadToPersistResult]:
    """Parses the result of `touch_read_to_persist` into a list of
    `TouchReadToPersistResult` objects.

    Args:
        res (list): The result of `touch_read_to_persist`

    Returns:
        list[TouchReadToPersistResult]: The parsed result
    """
    result: List[TouchReadToPersistResult] = []
    for i in range(0, len(res), 3):
        code = res[i]
        data = (
            TouchLink.from_redis_hget(res[i + 1]) if res[i + 1][0] is not None else None
        )
        try:
            clicks = [
                TouchLinkBufferedClick.parse_raw(args, content_type="application/json")
                for args in res[i + 2]
            ]
        except:
            clicks = []
            for args in res[i + 2]:
                parsed = json.loads(args)
                if parsed.get("parent_uid") is False:
                    parsed["parent_uid"] = None
                if parsed.get("visitor_uid") is False:
                    parsed["visitor_uid"] = None
                if parsed.get("user_sub") is False:
                    parsed["user_sub"] = None
                clicks.append(TouchLinkBufferedClick.parse_obj(parsed))
            logging.warning(
                f"Corrected corrupted clicks; original: {res[i+2]}, fixed: {clicks}"
            )

        result.append(TouchReadToPersistResult(code, data, clicks))
    return result
