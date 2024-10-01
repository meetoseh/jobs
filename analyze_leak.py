import argparse
import pickle
from typing import Dict, List, cast, TypedDict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--number",
        "-n",
        type=int,
        required=True,
        help="Which leak file to analyze (n for the file of the form leak-{n}.pickle)",
    )
    result = parser.parse_args()
    analyze_leak(result.number)


class RefInfo(TypedDict):
    count: int
    total_size: int
    largest_size: int


def _make_bytes_human_readable(size: float) -> str:
    """Converts a size in bytes to a human-readable string."""
    unit = "B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.2f} {unit}"


def analyze_leak(number: int):
    with open(f"leak-{number}.pickle", "rb") as f:
        info = cast(Dict[str, RefInfo], pickle.load(f))

    as_tuples = [(k, v) for k, v in info.items()]
    sorted_by_descending_total_size = sorted(
        as_tuples, key=lambda x: x[1]["total_size"], reverse=True
    )

    print("Top 10 types by total size:")
    for i in range(min(10, len(sorted_by_descending_total_size))):
        type_name, type_info = sorted_by_descending_total_size[i]
        print(f"{type_name}: {_make_bytes_human_readable(type_info['total_size'])}")

    print("\nTop 10 types by count:")
    sorted_by_descending_count = sorted(
        as_tuples, key=lambda x: x[1]["count"], reverse=True
    )
    for i in range(min(10, len(sorted_by_descending_count))):
        type_name, type_info = sorted_by_descending_count[i]
        print(f"{type_name}: {type_info['count']} instances")

    print("\nTop 10 types by largest size:")
    sorted_by_descending_largest_size = sorted(
        as_tuples, key=lambda x: x[1]["largest_size"], reverse=True
    )
    for i in range(min(10, len(sorted_by_descending_largest_size))):
        type_name, type_info = sorted_by_descending_largest_size[i]
        print(f"{type_name}: {_make_bytes_human_readable(type_info['largest_size'])}")

    cumulative_size = 0
    cumulative_count = 0
    for _, type_info in info.items():
        cumulative_size += type_info["total_size"]
        cumulative_count += type_info["count"]

    print(f"\nTotal size of all objects: {_make_bytes_human_readable(cumulative_size)}")
    print(f"Total number of objects: {cumulative_count}")


if __name__ == "__main__":
    main()
