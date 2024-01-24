from typing import Dict, List, Literal, Union, TypeVar, overload
from enum import Enum


class _NotSetEnum(Enum):
    NotSet = 0


NOT_SET = _NotSetEnum.NotSet

DefaultT = TypeVar("DefaultT")


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
            self._map: Dict[Union[str, bytes], Union[str, bytes]] = dict(
                zip(raw[::2], raw[1::2])
            )
        else:
            self._map = raw

    @overload
    def get_bytes(self, key: bytes) -> bytes:
        ...

    @overload
    def get_bytes(self, key: bytes, *, default: DefaultT) -> Union[DefaultT, bytes]:
        ...

    def get_bytes(
        self,
        key: bytes,
        *,
        default: Union[DefaultT, Literal[_NotSetEnum.NotSet]] = NOT_SET
    ) -> Union[DefaultT, bytes]:
        if key not in self._map:
            str_key = key.decode("utf-8")
            if str_key not in self._map:
                if default is NOT_SET:
                    raise KeyError(key)
                return default
            res = self._map[str_key]
        res = self._map[key]
        if isinstance(res, str):
            return res.encode("utf-8")
        if res == b"":
            if default is NOT_SET:
                raise KeyError(key)
            return default
        return res

    @overload
    def get_str(self, key: bytes, *, encoding: str = "utf-8") -> str:
        ...

    @overload
    def get_str(
        self, key: bytes, *, encoding: str = "utf-8", default: DefaultT
    ) -> Union[DefaultT, str]:
        ...

    def get_str(
        self,
        key: bytes,
        *,
        default: Union[DefaultT, Literal[_NotSetEnum.NotSet]] = NOT_SET,
        encoding: str = "utf-8"
    ) -> Union[DefaultT, str]:
        try:
            return self.get_bytes(key).decode(encoding)
        except KeyError:
            if default is NOT_SET:
                raise
            return default

    @overload
    def get_int(self, key: bytes) -> int:
        ...

    @overload
    def get_int(self, key: bytes, *, default: DefaultT) -> Union[DefaultT, int]:
        ...

    def get_int(
        self,
        key: bytes,
        *,
        default: Union[DefaultT, Literal[_NotSetEnum.NotSet]] = NOT_SET
    ) -> Union[DefaultT, int]:
        try:
            return int(self.get_bytes(key))
        except KeyError:
            if default is NOT_SET:
                raise
            return default

    @overload
    def get_float(self, key: bytes) -> float:
        ...

    @overload
    def get_float(self, key: bytes, *, default: DefaultT) -> Union[DefaultT, float]:
        ...

    def get_float(
        self,
        key: bytes,
        *,
        default: Union[DefaultT, Literal[_NotSetEnum.NotSet]] = NOT_SET
    ) -> Union[DefaultT, float]:
        try:
            return float(self.get_bytes(key))
        except KeyError:
            if default is NOT_SET:
                raise
            return default

    def items_bytes(self):
        for k, v in self._map.items():
            if isinstance(k, str):
                k = k.encode("utf-8")
            if isinstance(v, str):
                v = v.encode("utf-8")
            yield k, v

    def __len__(self):
        return len(self._map)
