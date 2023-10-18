from typing import Dict, List, Optional, Union


NotSet = object()


class RedisHash:
    """Convenience class for working with hash's returned from redis
    by normalizing bytes/string keys and values and including parsing
    options.
    """

    def __init__(
        self,
        raw: Union[
            List[Union[str, bytes]],
            Dict[Union[str, bytes], Union[str, bytes]],
        ],
    ):
        if isinstance(raw, list):
            if len(raw) % 2 != 0:
                raise ValueError("list must have an even number of elements")
            self._map = dict(zip(raw[::2], raw[1::2]))
        else:
            self._map = raw

    def get_bytes(self, key: bytes, *, default: Optional[bytes] = NotSet) -> bytes:
        if key not in self._map:
            str_key = key.decode("utf-8")
            if str_key not in self._map:
                if default is NotSet:
                    raise KeyError(key)
                return default
            res = self._map[str_key]
        res = self._map[key]
        if isinstance(res, str):
            return res.encode("utf-8")
        if res == b"":
            if default is NotSet:
                raise KeyError(key)
            return default
        return res

    def get_str(
        self, key: bytes, *, default: Optional[float] = NotSet, encoding: str = "utf-8"
    ) -> str:
        try:
            return self.get_bytes(key).decode(encoding)
        except KeyError:
            if default is NotSet:
                raise
            return default

    def get_int(self, key: bytes, *, default: Optional[float] = NotSet) -> int:
        try:
            return int(self.get_bytes(key))
        except KeyError:
            if default is NotSet:
                raise
            return default

    def get_float(self, key: bytes, *, default: Optional[float] = NotSet) -> float:
        try:
            return float(self.get_bytes(key))
        except KeyError:
            if default is NotSet:
                raise
            return default

    def items_bytes(self):
        for k, v in self._map.items():
            if isinstance(k, str):
                k = k.encode("utf-8")
            if isinstance(v, str):
                v = v.encode("utf-8")
            yield k, v
