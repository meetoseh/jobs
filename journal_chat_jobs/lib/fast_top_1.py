import asyncio
import math
from typing import TypeVar, List, Callable, Awaitable, Tuple, Set

T = TypeVar("T")
V = TypeVar("V")
Q = TypeVar("Q")


async def fast_top_1(
    arr: List[T],
    /,
    *,
    compare: Callable[[T, T], Awaitable[int]],
    semaphore: asyncio.Semaphore,
) -> T:
    """Finds the best possible candidate from the given list, assuming that
    the comparison function may be inconsistent (i.e., it may not result
    in a valid partial ordering)

    By construction, it does not repeat known comparisons. This considers
    every item in the list in a deterministic order, which necessarily means
    a large number of comparisons for large lists. Try to filter the list as
    much as possible before calling this function.

    Args:
        arr (List[T]): the list of items to compare
        compare (Callable[[T, T], Awaitable[int]]): the comparison function
        semaphore (asyncio.Semaphore): the semaphore to use for parallelism to compare
    """
    if len(arr) <= 6:  # use 2 for fastest, larger for more accurate
        return await _top_1_pairwise_full(arr, compare=compare, semaphore=semaphore)

    num_subgroups = max(math.ceil(math.sqrt(len(arr))), 2)
    subgroup_size = len(arr) // num_subgroups

    subgroups: List[List[T]] = []
    for i in range(num_subgroups):
        subgroups.append(arr[i * subgroup_size : (i + 1) * subgroup_size])

    best_of_subgroups = await asyncio.gather(
        *[
            fast_top_1(subgroup, compare=compare, semaphore=semaphore)
            for subgroup in subgroups
        ]
    )
    return await fast_top_1(best_of_subgroups, compare=compare, semaphore=semaphore)


async def _top_1_pairwise_full(
    arr: List[T],
    /,
    *,
    compare: Callable[[T, T], Awaitable[int]],
    semaphore: asyncio.Semaphore,
) -> T:
    """Finds the best option amongst the given list using n choose 2 comparisons (so for 6
    options, takes 15 comparisons). All comparisons can be done in parallel if there is enough
    space on the semaphore
    """
    if not arr:
        raise ValueError("Cannot find the best of an empty list")

    if len(arr) == 1:
        return arr[0]

    comparisons: List[List[int]] = [[0] * len(arr) for _ in range(len(arr))]

    running: Set[asyncio.Task] = set()
    # each returns [(i, j), result]

    i = 0
    j = 1
    additional_acquisitions = 0
    while running or i < len(arr) - 1:
        # start as many as we can
        while (additional_acquisitions > 0 or not semaphore.locked()) and i < len(
            arr
        ) - 1:
            if additional_acquisitions > 0:
                additional_acquisitions -= 1
            else:
                await semaphore.acquire()

            running.add(
                asyncio.create_task(
                    _augmented_compare((i, j), arr[i], arr[j], compare=compare)
                )
            )
            j += 1
            if j >= len(arr):
                i += 1
                j = i + 1

        while additional_acquisitions > 0:
            semaphore.release()
            additional_acquisitions -= 1

        if not running:
            await semaphore.acquire()
            additional_acquisitions += 1
            continue

        if i >= len(arr) - 1:
            done, running = await asyncio.wait(
                running, return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                semaphore.release()
                (done_i, done_j), done_comparison = t.result()
                comparisons[done_i][done_j] = done_comparison
                comparisons[done_j][done_i] = -done_comparison
            continue

        something_finished_task = asyncio.create_task(
            asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        )
        acquire_task = asyncio.create_task(semaphore.acquire())
        await asyncio.wait(
            [something_finished_task, acquire_task], return_when=asyncio.FIRST_COMPLETED
        )

        if not acquire_task.cancel():
            acquire_task.result()
            additional_acquisitions += 1

        if not something_finished_task.cancel():
            done, running = await something_finished_task
            for t in done:
                semaphore.release()
                (done_i, done_j), done_comparison = t.result()
                comparisons[done_i][done_j] = done_comparison
                comparisons[done_j][done_i] = -done_comparison

    while additional_acquisitions > 0:
        semaphore.release()
        additional_acquisitions -= 1

    scores = [sum(row) for row in comparisons]
    best_index = max(range(len(arr)), key=lambda i: scores[i])
    return arr[best_index]


async def _augmented_compare(
    retval1: V, a: T, b: T, compare: Callable[[T, T], Awaitable[int]]
) -> Tuple[V, int]:
    return retval1, await compare(a, b)
