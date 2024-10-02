import argparse
import pickle
from typing import Dict, cast, TypedDict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--number1",
        "-n1",
        type=int,
        required=True,
        help="The first file to compare (n for the file of the form leak-{n}.pickle)",
    )
    parser.add_argument(
        "--number2",
        "-n2",
        type=int,
        required=True,
        help="The second file to compare (n for the file of the form leak-{n}.pickle)",
    )
    result = parser.parse_args()
    compare_leaks(result.number1, result.number2)


class RefInfo(TypedDict):
    count: int
    total_size: int
    largest_size: int


class RefData(TypedDict):
    by_type: Dict[str, RefInfo]
    rss: int
    vms: int


def b2h(size: float) -> str:
    """Converts a size in bytes to a human-readable string."""
    sign = "-" if size < 0 else "+"
    size = abs(size)
    unit = "B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{sign}{size:.2f} {unit}"


def compare_leaks(n1: int, n2: int):
    with open(f"leak-{n1}.pickle", "rb") as f:
        data1 = cast(RefData, pickle.load(f))

    with open(f"leak-{n2}.pickle", "rb") as f:
        data2 = cast(RefData, pickle.load(f))

    delta_by_type: Dict[str, RefInfo] = {}

    for type_name, type_info in data2["by_type"].items():
        delta_by_type[type_name] = {
            "count": type_info["count"],
            "total_size": type_info["total_size"],
            "largest_size": type_info["largest_size"],
        }

    for type_name, type_info in data1["by_type"].items():
        base = delta_by_type.get(type_name)
        if base is None:
            base = cast(RefInfo, {"count": 0, "total_size": 0, "largest_size": 0})
            delta_by_type[type_name] = base

        base["count"] -= type_info["count"]
        base["total_size"] -= type_info["total_size"]
        base["largest_size"] -= type_info["largest_size"]

    # remove zeros
    delta_by_type = {
        k: v
        for k, v in delta_by_type.items()
        if v["count"] != 0 or v["total_size"] != 0 or v["largest_size"] != 0
    }

    print("total number of types:", len(delta_by_type))
    as_tuples = [(k, v) for k, v in delta_by_type.items()]

    print("All types in ascending order of total size:")
    sorted_by_ascending_total_size = sorted(as_tuples, key=lambda x: x[1]["total_size"])

    zero_info: RefInfo = {"count": 0, "total_size": 0, "largest_size": 0}
    for type_name, delta_info in sorted_by_ascending_total_size:
        type_info1 = data1["by_type"].get(type_name, zero_info)
        type_info2 = data2["by_type"].get(type_name, zero_info)

        print(
            f"{type_name}: {delta_info['count']} instances, {b2h(delta_info['total_size'])} [in 1: {type_info1['count']} instances, {b2h(type_info1['total_size'])}], [in 2: {type_info2['count']} instances, {b2h(type_info2['total_size'])}]"
        )

    print("\nNet size increase:")
    net_size_increase = sum(
        delta_info["total_size"] for delta_info in delta_by_type.values()
    )
    print(f"{b2h(net_size_increase)}")

    print(f"\nNet count increase:")
    net_count_increase = sum(
        delta_info["count"] for delta_info in delta_by_type.values()
    )
    print(f"{net_count_increase} instances")

    print()
    print("Total size in 1:      ", end="")
    total_size1 = sum(
        type_info["total_size"] for type_info in data1["by_type"].values()
    )
    print(f"{b2h(total_size1)}")
    print("RMS in 1:             ", end="")
    print(f"{b2h(data1['rss'])}")
    print("VMS in 1:             ", end="")
    print(f"{b2h(data1['vms'])}")
    print("Unaccounted RSS in 1: ", end="")
    print(f"{b2h(data1['rss'] - total_size1)}")
    print("Unaccounted VMS in 1: ", end="")
    print(f"{b2h(data1['vms'] - total_size1)}")

    print()
    print("Total size in 2:      ", end="")
    total_size2 = sum(
        type_info["total_size"] for type_info in data2["by_type"].values()
    )
    print(f"{b2h(total_size2)}")
    print("RMS in 2:             ", end="")
    print(f"{b2h(data2['rss'])}")
    print("VMS in 2:             ", end="")
    print(f"{b2h(data2['vms'])}")
    print("Unaccounted RSS in 2: ", end="")
    print(f"{b2h(data2['rss'] - total_size2)}")
    print("Unaccounted VMS in 2: ", end="")
    print(f"{b2h(data2['vms'] - total_size2)}")

    print()
    print("Change in total size:      ", end="")
    print(f"{b2h(total_size2 - total_size1)}")
    print("Change in RSS:             ", end="")
    print(f"{b2h(data2['rss'] - data1['rss'])}")
    print("Change in VMS:             ", end="")
    print(f"{b2h(data2['vms'] - data1['vms'])}")
    print("Change in unaccounted RSS: ", end="")
    print(f"{b2h(data2['rss'] - total_size2 - data1['rss'] + total_size1)}")
    print("Change in unaccounted VMS: ", end="")
    print(f"{b2h(data2['vms'] - total_size2 - data1['vms'] + total_size1)}")


if __name__ == "__main__":
    main()
