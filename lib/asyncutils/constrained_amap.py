import asyncio
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Coroutine,
    Dict,
    Iterator,
    Optional,
    Set,
    Tuple,
    TypeVar,
)
from lib.asyncutils.cancel_policy import CancelPolicy, handle_cancel_policy
import time

T = TypeVar("T")
R = TypeVar("R")


async def constrained_amap(
    original: Iterator[T],
    func: Callable[[T], Coroutine[Any, Any, R]],
    /,
    *,
    max_concurrency: int,
    individual_timeout: Optional[float] = None,
    overall_timeout: Optional[float] = None,
    cancel_policy: CancelPolicy = CancelPolicy.CANCEL_AND_WAIT,
) -> AsyncIterator[R]:
    """Maps from the original iterator to the result of the function, with a maximum
    number of concurrent tasks, and with optional individual timeouts and an optional
    overall timeout.

    When either timeout is reached, no more tasks are queued, the cancel policy
    is applied to running tasks, and then TimeoutError is raised.

    Yields the mapped values in the same order they were provided by the iterator.
    """
    next_index_from_iter = 0
    next_index_to_return = 0
    completed_out_of_order: Dict[int, R] = dict()

    # tuple[int, float] = (index, started_at)
    running_lookup: Dict[asyncio.Task[R], Tuple[int, float]] = {}
    running: Set[asyncio.Task[R]] = set()

    overall_timeout_at: Optional[float] = (
        time.perf_counter() + overall_timeout if overall_timeout is not None else None
    )

    more_tasks_available = True
    while True:
        while more_tasks_available and len(running) < max_concurrency:
            try:
                value = next(original)
            except StopIteration:
                more_tasks_available = False
            else:
                task = asyncio.create_task(func(value))
                running.add(task)
                running_lookup[task] = (next_index_from_iter, time.perf_counter())
                next_index_from_iter += 1

        if not running:
            break

        poll_start_at = time.perf_counter()
        poll_timeout: Optional[float] = None
        if overall_timeout_at is not None:
            poll_timeout = overall_timeout_at - poll_start_at

        if individual_timeout is not None:
            next_individual_timeout_at = (
                next(iter(running_lookup.values()))[1]
                + individual_timeout
                - poll_start_at
            )
            if poll_timeout is None or next_individual_timeout_at < poll_timeout:
                poll_timeout = next_individual_timeout_at

        if poll_timeout is not None and poll_timeout < 0:
            await handle_cancel_policy(running, cancel_policy)
            raise TimeoutError()

        done, _ = await asyncio.wait(
            running,
            timeout=poll_timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done:
            running.remove(task)
            index, _ = running_lookup.pop(task)
            try:
                result = task.result()
            except:
                await handle_cancel_policy(running, cancel_policy)
                raise
            else:
                completed_out_of_order[index] = result

        while next_index_to_return in completed_out_of_order:
            yield completed_out_of_order.pop(next_index_to_return)
            next_index_to_return += 1
