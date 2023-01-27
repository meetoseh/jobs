"""Performs the 5th step of the journey audio sample pipeline, where we partition
the frequency space into bins of roughly equal width

This is effectively the minimum raggedness word wrap problem with floating point
word lengths
"""
from typing import Callable, List
import numpy as np


def find_minimum_raggedness_word_wrap_breaks(
    word_lengths: np.ndarray, num_lines: int = 20
) -> list[int]:
    """Partitions the words into the given number of lines of roughly equal width

    Args:
        word_lengths (np.ndarray): the word lengths to partition as a (n_words,) float64 array
        num_lines (int, optional): the number of lines to partition the words into. Defaults to 20.
            This expects that num_lines << n_words

    Returns:
        list[int]: `num_lines + 1` integers representing the indices of the partition boundaries,
            starting with 0 and ending with n_words. For example, if `num_lines=3` and `n_words=10`
            this might be `[0, 3, 6, 10]`, which would indicate that the first line contains words
            0, 1, and 2, the second line contains words 3, 4, and 5, and the third line contains
            words 6, 7, 8, and 9. Always in strictly ascending order.
    """
    # https://stackoverflow.com/a/5060467
    num_words = word_lengths.shape[0]
    offsets = np.zeros((num_words + 1,), dtype=np.float64)
    np.cumsum(word_lengths, out=offsets[1:])

    total_width = offsets[-1]
    line_width = total_width / num_lines

    def cost(i: int, j: int) -> float:
        """The cost of a line with words i through j, excluding word j"""
        actual_line_width = offsets[j] - offsets[i]
        return (actual_line_width - line_width) ** 2

    # best_costs[line_index][word_index] is the minimum total cost for words [0, word_index)
    # across the first line_index + 1 lines
    best_costs = np.zeros((num_lines + 1, num_words + 1), dtype=np.float64)

    # best_starts[line_index][word_index] is the index that of the word thats starts
    # the last line in order to get the minimum total cost for words [0, word_index)
    # across the first line_index + 1 lines
    best_starts = np.zeros((num_lines + 1, num_words + 1), dtype=np.int32)

    best_costs[0, 1:] = np.inf
    for line_index in range(1, num_lines + 1):
        for word_index in range(num_words + 1):
            best_line_earlier_word_index = find_best_line_start_binary_search(
                best_costs, cost, line_index, word_index
            )
            best_line_cost = best_costs[line_index - 1][
                best_line_earlier_word_index
            ] + cost(best_line_earlier_word_index, word_index)

            best_costs[line_index][word_index] = best_line_cost
            best_starts[line_index][word_index] = best_line_earlier_word_index

    lines = []
    line_end_index = num_words
    for line_index in range(num_lines, 0, -1):
        line_start_index = best_starts[line_index][line_end_index]
        lines.append(line_end_index)
        line_end_index = line_start_index
    lines.append(0)
    lines.reverse()
    return lines


def find_best_line_start_brute_force(
    best_costs: np.ndarray,
    cost: Callable[[int, int], float],
    line_index: int,
    word_index: int,
) -> int:
    """Finds the best word to start the line at the given line_index with if
    the word that starts the next line must be word_index

    Args:
        best_costs (np.ndarray): The minimum total cost for words [0, word_index)
            across the first line_index + 1 lines
        cost (Callable[[int, int], float]): The cost of a line with words i through j, excluding word j
        line_index (int): The line index that we're finding the word to start the line of
        word_index (int): The word index that the next line must start with

    Returns:
        int: The word index that the line should start with
    """
    if line_index == 1:
        return 0

    best_line_cost = np.inf
    best_line_earlier_word_index = -1

    for earlier_word_index in range(word_index + 1):
        cost_to_start_this_line_at_this_word = best_costs[line_index - 1][
            earlier_word_index
        ]
        cost_for_this_line = cost(earlier_word_index, word_index)
        line_score = cost_to_start_this_line_at_this_word + cost_for_this_line
        if line_score < best_line_cost:
            best_line_cost = line_score
            best_line_earlier_word_index = earlier_word_index

    return best_line_earlier_word_index


def find_best_line_start_binary_search(
    best_costs: np.ndarray,
    cost: Callable[[int, int], float],
    line_index: int,
    word_index: int,
) -> int:
    """Finds the best word to start the line at the given line_index with if
    the word that starts the next line must be word_index

    This uses the convexity property of the line scores to perform a binary
    search

    Args:
        best_costs (np.ndarray): The minimum total cost for words [0, word_index)
            across the first line_index + 1 lines
        cost (Callable[[int, int], float]): The cost of a line with words i through j, excluding word j
        line_index (int): The line index that we're finding the word to start the line of
        word_index (int): The word index that the next line must start with

    Returns:
        int: The word index that the line should start with
    """
    if line_index == 1:
        return 0

    if word_index < 3:
        return find_best_line_start_brute_force(
            best_costs, cost, line_index, word_index
        )

    # we know we're done if we find a score where the two adjacent words have a higher score
    low = 0
    high = word_index

    while low < high:
        mid = (low + high) // 2

        line_score_for_mid = best_costs[line_index - 1][mid] + cost(mid, word_index)
        line_score_for_next = best_costs[line_index - 1][mid + 1] + cost(
            mid + 1, word_index
        )

        if line_score_for_mid < line_score_for_next:
            if mid == 0:
                return 0
            line_score_for_prev = best_costs[line_index - 1][mid - 1] + cost(
                mid - 1, word_index
            )
            if line_score_for_prev >= line_score_for_mid:
                return mid
            high = mid - 1
        else:
            if mid + 1 == word_index + 1:
                return word_index + 1

            line_score_for_next_next = best_costs[line_index - 1][mid + 2] + cost(
                mid + 2, word_index
            )
            if line_score_for_next_next >= line_score_for_next:
                return mid + 1
            low = mid + 1

    return low


def word_wrap(text: str, num_lines: int) -> List[str]:
    """This is not used in the processing pipeline, it's just for testing
    the minimum raggedness word wrap algorithm. Note that we don't consider
    spaces as taking up any space, so it can look a little funny if the
    words are of wildly different lengths

    Args:
        text (str): The text to partition into lines
        num_lines (int): The number of lines to partition the text into

    Returns:
        List[str]: The lines of text
    """
    split_text = text.split()
    word_lengths = np.array([len(word) for word in split_text])
    breaks = find_minimum_raggedness_word_wrap_breaks(word_lengths, num_lines)
    print(breaks)
    assert len(breaks) == num_lines + 1
    lines = []
    for i in range(num_lines):
        lines.append(" ".join(split_text[breaks[i] : breaks[i + 1]]))
    return lines


def main():
    num_lines = int(input("Number of lines? "))
    text = input("Text? ")

    print("Our word wrap:")

    lines = word_wrap(text, num_lines)
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
