from fractions import Fraction
import math
from typing import Callable, List, Optional, FrozenSet, Set, Tuple, Union, cast

from lib.devices.ios_device_sizes import IOS_DEVICE_SIZES


def _default_mapper(
    logical_width: int, logical_height: int, density: Fraction
) -> Tuple[int, int]:
    return (math.ceil(logical_width * density), math.ceil(logical_height * density))


def get_sizes_for_devices_newer_than(
    iso8601: str,
    *,
    mapper: Optional[
        Callable[[int, int, Fraction], Union[Tuple[int, int], List[Tuple[int, int]]]]
    ] = None,
    # The mapper is called with (logical width, logical height, physical_pixels_per_logical_pixel)
    # and returns an item to include in the result. by default, the mapper is
    # lambda lw, lh, density: (math.ceil(lw * density), math.ceil(lh * density))
    include_families: Optional[Union[FrozenSet[str], Set[str]]] = None,
    exclude_families: Optional[Union[FrozenSet[str], Set[str]]] = None
) -> Set[Tuple[int, int]]:
    device_sizes = IOS_DEVICE_SIZES
    if include_families is not None:
        device_sizes = [
            size for size in device_sizes if size.family in include_families
        ]
    if exclude_families is not None:
        device_sizes = [
            size for size in device_sizes if size.family not in exclude_families
        ]

    mapper = mapper or _default_mapper

    result: Set[Tuple[int, int]] = set()

    def _add_all(v: Union[Tuple[int, int], List[Tuple[int, int]]]) -> None:
        if not v:
            return
        if isinstance(v[0], int):
            result.add(cast(Tuple[int, int], v))
            return

        for item in cast(List[Tuple[int, int]], v):
            result.add(item)

    for device_size in device_sizes:
        if device_size.release_date_iso8601 > iso8601:
            scale_from_physical = 1
            scale_from_logical = Fraction(device_size.scale_factor)
            while scale_from_logical > 1:
                _add_all(
                    mapper(
                        device_size.logical_width,
                        device_size.logical_height,
                        scale_from_logical,
                    )
                )
                scale_from_physical += 1
                scale_from_logical = Fraction(
                    device_size.scale_factor, scale_from_physical
                )

            _add_all(
                mapper(
                    device_size.logical_width, device_size.logical_height, Fraction(1)
                )
            )

    return result
