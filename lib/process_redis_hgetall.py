from typing import Dict


def process_redis_hgetall_ints(raw: dict) -> Dict[str, int]:
    """Parses the dict returned from redis.hgetall into a dict of str keys and int
    values. The redis library is not particularly consistent about when we get
    str or bytes, so we handle both.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"expected dict, got {type(raw)}")

    res: Dict[str, int] = dict()
    for key, value in raw.items():
        if not isinstance(key, (str, bytes)):
            raise ValueError(f"expected str or bytes, got {type(key)=}")
        if not isinstance(value, (str, bytes)):
            raise ValueError(f"expected str or bytes, got {type(value)=}")

        str_key = key if isinstance(key, str) else str(key, "ascii")
        try:
            int_value = int(value)
        except ValueError:
            raise ValueError(f"while processing {str_key=} and {value=}")
        res[str_key] = int_value
    return res
