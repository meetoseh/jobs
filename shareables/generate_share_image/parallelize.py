import base64
from dataclasses import dataclass
import multiprocessing
from multiprocessing.synchronize import Event as MPEvent
from multiprocessing.sharedctypes import Synchronized
import secrets
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast
from graceful_death import GracefulDeath, graceful_sleep
from images import ImageTarget
from itgs import Itgs
from lib.progressutils.progress_helper import ProgressHelper
from lib.thumbhash import image_to_thumb_hash
from shareables.generate_share_image.exceptions import ShareImageBounceError
from shareables.generate_share_image.generator import ShareImageGenerator
from pydantic import BaseModel, Field
import asyncio
import os
from queue import Empty as QueueEmpty
import logging
import logging.config
import yaml
import traceback
from PIL import Image
import threading


@dataclass
class GeneratedLocalImageFileExport:
    """This is like LocalImageFileTarget from images but omitting the crop settings"""

    uid: str
    width: int
    height: int
    filepath: str
    format: str
    quality_settings: Dict[str, Any]
    thumbhash: str
    file_size: int


class _GenerateEncodeSubtask(BaseModel):
    target_id: int = Field()
    """Identifier for the image target within the image"""
    encode_format: str = Field()
    """Immediately after generating the image while it is still in in-process memory,
    encode it to this format
    """
    encode_quality_settings: Dict[str, Any] = Field()
    """With these quality settings"""
    out_filepath: str = Field()
    """Saving the encoded image to this file path"""


class _GenerateTask(BaseModel):
    """A mixed process can receive generate tasks, which involve generating an image,
    optionally reporting that image to the outgoing queue, and then performing
    encodings of the image, saving it to disk, and reporting the result to the outgoing
    queue.
    """

    resolution_id: int = Field()
    """Identifier for the resolution of the image to generate"""
    type: Literal["task"] = Field()
    """Discriminatory field"""
    action: Literal["generate"] = Field()
    """Discriminator field"""
    width: int = Field()
    """The width of the image to generate in physical pixels"""
    height: int = Field()
    """The height of the image to generate in physical pixels"""
    encode_subtasks: List[_GenerateEncodeSubtask] = Field()
    """After generating the image, encode it to these formats and save them to these
    file paths."""


class _EncodeTask(BaseModel):
    """A process can be asked to encode an image to a specific format and quality"""

    type: Literal["task"] = Field()
    """Discriminatory field"""
    action: Literal["encode"] = Field()
    """Discriminator field"""
    id: int = Field()
    """Identifier for this task"""
    resolution_id: int = Field()
    """The identifier of the resolution thats being encoded"""
    img: bytes = Field()
    """The image to encode"""
    width: int = Field()
    """The width of the image"""
    height: int = Field()
    """The height of the image"""
    subtasks: List[_GenerateEncodeSubtask] = Field()
    """The subtasks to perform"""


@dataclass
class _MainProcessState:
    """The state we store in the main process,
    to keep track of the work that needs to be done
    """

    unique_resolutions: List[Tuple[int, int]]
    """The unique resolutions, in order, such that index 0 corresponds to
    generate task 0, which corresponds to the task to generate an image at
    that resolution
    """

    targets_by_resolution: Dict[Tuple[int, int], List[ImageTarget]]
    """Given a particular resolution, the targets that need to be generated
    at that resolution, such that index 0 in the targets list corresponds to
    the encode task id 0, which corresponds to the task to encode the image
    at that resolution to the format and quality settings specified in the
    target
    """


async def generate_targets(
    itgs: Itgs,
    gd: GracefulDeath,
    generator: ShareImageGenerator,
    /,
    *,
    name_hint: str,
    prog: ProgressHelper,
    targets: List[ImageTarget],
    out_dir: str,
) -> List[GeneratedLocalImageFileExport]:
    """Uses the given share image generator"""
    if gd.received_term_signal:
        raise ShareImageBounceError()

    assert targets
    unique_resolutions_by_count: Dict[Tuple[int, int], int] = dict()
    for target in targets:
        unique_resolutions_by_count[(target.width, target.height)] = (
            unique_resolutions_by_count.get((target.width, target.height), 0) + 1
        )

    max_pool_size = multiprocessing.cpu_count() // 2
    # this is CPU intensive, so having more than one process per physical core
    # will just cause overhead

    max_useful_generator_processes = len(unique_resolutions_by_count)
    # given unlimited compute, it would be ideal to generate all the different resolutions
    # at once

    max_useful_separate_encoder_processes = sum(
        unique_resolutions_by_count.values()
    ) - len(targets)
    # given unlimited compute, it would be ideal for each generator process to
    # encode one of its targets while simultaneously offloading all remaining
    # targets for the same resolution to separate processes

    num_processes = max(
        min(
            max_useful_generator_processes + max_useful_separate_encoder_processes,
            max_pool_size,
        ),
        1,
    )

    await prog.push_progress(
        f"preparing to generate {len(targets)} target{'s' if len(targets) != 1 else ''} "
        f"for {name_hint} consisting of {len(unique_resolutions_by_count)} unique resolution{'s' if len(unique_resolutions_by_count) != 1 else ''} "
        f"using {num_processes} process{'es' if num_processes != 1 else ''} "
        + f"(there {'is' if max_pool_size == 1 else 'are'} {max_pool_size} physical core{'s' if max_pool_size != 1 else ''} available).",
        indicator={"type": "spinner"},
    )

    cancel_term_signal_checker = asyncio.Event()
    subprocess_shutdown_requested = multiprocessing.Event()
    term_signal_checker_task = asyncio.create_task(
        _check_term_signal(
            gd, cancel_term_signal_checker, subprocess_shutdown_requested
        )
    )

    if gd.received_term_signal:
        raise ShareImageBounceError()

    ordered_unique_resolutions = list(unique_resolutions_by_count.keys())
    targets_by_resolution: Dict[Tuple[int, int], List[ImageTarget]] = dict()
    for target in targets:
        arr = targets_by_resolution.get((target.width, target.height))
        if arr is None:
            arr = []
            targets_by_resolution[(target.width, target.height)] = arr
        arr.append(target)

    main_process_state = _MainProcessState(
        unique_resolutions=ordered_unique_resolutions,
        targets_by_resolution=targets_by_resolution,
    )

    del ordered_unique_resolutions
    del targets_by_resolution
    del unique_resolutions_by_count

    idle_workers_count = multiprocessing.Value("I", 0)
    tasks_queue = multiprocessing.Queue()
    task_completed_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    log_queue = multiprocessing.Queue()

    # queue tasks before starting processes to avoid unnecessary work redistribution
    for resolution_id, resolution in enumerate(main_process_state.unique_resolutions):
        os.makedirs(os.path.join(out_dir, str(resolution_id)), exist_ok=True)
        tasks_queue.put(
            _GenerateTask(
                resolution_id=resolution_id,
                type="task",
                action="generate",
                width=resolution[0],
                height=resolution[1],
                encode_subtasks=[
                    _GenerateEncodeSubtask(
                        target_id=target_id,
                        encode_format=target.format,
                        encode_quality_settings=target.quality_settings,
                        out_filepath=os.path.join(
                            out_dir, str(resolution_id), f"{target_id}.{target.format}"
                        ),
                    )
                    for target_id, target in enumerate(
                        main_process_state.targets_by_resolution[resolution]
                    )
                ],
            )
        )

    # avoid corrupting the log file
    old_prog_log = prog.log
    prog.log = False

    processes = [
        multiprocessing.Process(
            target=_worker_target,
            name=f"generate_share_image_worker_{i}",
            args=(
                i,
                subprocess_shutdown_requested,
                tasks_queue,
                idle_workers_count,
                task_completed_queue,
                error_queue,
                log_queue,
                generator,
            ),
            daemon=True,
        )
        for i in range(num_processes)
    ]
    processes.append(
        multiprocessing.Process(
            target=_log_target,
            name="generate_share_image_log_worker",
            args=(
                subprocess_shutdown_requested,
                log_queue,
            ),
        )
    )

    for p in processes:
        p.start()

    exports: List[GeneratedLocalImageFileExport] = []
    last_reported_completed = 0
    progress_message = (
        f"generating {len(targets)} target{'s' if len(targets) != 1 else ''} for {name_hint} in "
        f"{num_processes} process{'es' if num_processes != 1 else ''}"
    )
    progress_task = asyncio.create_task(
        prog.push_progress(
            progress_message, indicator={"type": "bar", "at": 0, "of": len(targets)}
        )
    )

    while len(exports) < len(targets):
        if gd.received_term_signal:
            subprocess_shutdown_requested.set()
            cancel_term_signal_checker.set()
            for process in processes:
                process.join()
            await progress_task
            await term_signal_checker_task
            prog.log = old_prog_log
            raise ShareImageBounceError()

        try:
            next_error = error_queue.get_nowait()
            subprocess_shutdown_requested.set()
            cancel_term_signal_checker.set()
            for process in processes:
                process.join()
            await progress_task
            await term_signal_checker_task
            prog.log = old_prog_log
            raise next_error
        except QueueEmpty:
            pass

        if any(not p.is_alive() for p in processes):
            subprocess_shutdown_requested.set()
            cancel_term_signal_checker.set()
            for process in processes:
                process.join()
            await progress_task
            await term_signal_checker_task
            prog.log = old_prog_log
            raise RuntimeError("A worker process died unexpectedly")

        try:
            export = task_completed_queue.get_nowait()
            assert isinstance(export, GeneratedLocalImageFileExport), export
            _log(
                log_queue,
                logging.INFO,
                f"main process received finished export message: {export!r}",
            )
            exports.append(export)
        except QueueEmpty:
            await graceful_sleep(gd, 1)

        if progress_task.done() and len(exports) > last_reported_completed:
            progress_task = asyncio.create_task(
                prog.push_progress(
                    progress_message,
                    indicator={"type": "bar", "at": len(exports), "of": len(targets)},
                )
            )
            last_reported_completed = len(exports)

    subprocess_shutdown_requested.set()
    cancel_term_signal_checker.set()
    for process in processes:
        process.join()

    await progress_task
    await term_signal_checker_task

    prog.log = old_prog_log
    return exports


async def _check_term_signal(
    gd: GracefulDeath,
    cancel_event: asyncio.Event,
    detected_event: MPEvent,
) -> None:
    while not cancel_event.is_set():
        if gd.received_term_signal:
            detected_event.set()
            return

        try:
            await asyncio.wait_for(cancel_event.wait(), timeout=0.1)
        except asyncio.TimeoutError:
            pass
        except (InterruptedError, KeyboardInterrupt):
            gd.received_term_signal = True
            detected_event.set()
            return


class _LogMessage(BaseModel):
    level: int = Field()
    message: str = Field()


def _log(log_queue: multiprocessing.Queue, level: int, message: str) -> None:
    log_queue.put(_LogMessage(level=level, message=message))


def _worker_target(
    id: int,
    shutdown_requested: MPEvent,
    tasks_queue: multiprocessing.Queue,
    idle_workers_count: Synchronized,
    task_completed_queue: multiprocessing.Queue,
    error_queue: multiprocessing.Queue,
    log_queue: multiprocessing.Queue,
    generator: ShareImageGenerator,
):
    _log(log_queue, logging.INFO, f"worker {id} started")

    prepared = False
    marked_idle = False

    while not shutdown_requested.is_set():
        try:
            task = tasks_queue.get(timeout=0.1)
        except QueueEmpty:
            if not marked_idle:
                _log(log_queue, logging.DEBUG, f"worker {id} is now idle")
                with idle_workers_count.get_lock():
                    idle_workers_count.value += 1
                marked_idle = True
            continue

        if marked_idle:
            _log(log_queue, logging.DEBUG, f"worker {id} is no longer idle")
            with idle_workers_count.get_lock():
                idle_workers_count.value -= 1
            marked_idle = False

        _log(log_queue, logging.DEBUG, f"worker {id} starting {task!r}")
        if isinstance(task, _GenerateTask) and not prepared:
            _log(log_queue, logging.INFO, f"worker {id} preparing generator")
            start_at = time.perf_counter()
            try:
                generator.prepare_process(shutdown_requested)
            except BaseException as e:
                if shutdown_requested.is_set():
                    return
                _log(
                    log_queue,
                    logging.ERROR,
                    f"worker {id} failed to prepare generator:\n\n{traceback.format_exc()}",
                )
                error_queue.put(e)
                shutdown_requested.wait()
                return
            prepared = True

            prepare_time = time.perf_counter() - start_at
            if not shutdown_requested.is_set():
                _log(
                    log_queue,
                    logging.INFO,
                    f"worker {id} prepared generator in {prepare_time:.3f}s",
                )

        if shutdown_requested.is_set():
            return

        if isinstance(task, _GenerateTask):
            _log(
                log_queue,
                logging.INFO,
                f"worker {id} generating image @ {task.width}x{task.height}",
            )
            start_at = time.perf_counter()
            try:
                img = generator.generate(
                    task.width, task.height, exit_requested=shutdown_requested
                )
            except BaseException as e:
                if shutdown_requested.is_set():
                    return
                _log(
                    log_queue,
                    logging.ERROR,
                    f"worker {id} failed to generate image:\n\n{traceback.format_exc()}",
                )
                error_queue.put(e)
                shutdown_requested.wait()
                return
            generate_time = time.perf_counter() - start_at
            _log(
                log_queue,
                logging.INFO,
                f"worker {id} generated image @ {task.width}x{task.height} in {generate_time:.3f}s",
            )
            subtasks = task.encode_subtasks
            img_as_bytes = None
        else:
            try:
                assert isinstance(task, _EncodeTask), task
            except AssertionError as e:
                _log(
                    log_queue,
                    logging.ERROR,
                    f"worker {id} received unexpected task: {task!r}\n{traceback.format_exc()}",
                )
                error_queue.put(e)
                shutdown_requested.wait()
                return
            img_as_bytes = task.img
            img = Image.frombytes("RGB", (task.width, task.height), task.img)
            subtasks = task.subtasks

        if len(subtasks) > 1:
            with idle_workers_count.get_lock():
                should_relinquish = idle_workers_count.value > 0
            if should_relinquish:
                _log(
                    log_queue,
                    logging.INFO,
                    f"worker {id} relinquishing extra {len(subtasks) - 1} subtasks to idle workers",
                )
                relinquished = subtasks[1:]
                subtasks = subtasks[:1]

                if img_as_bytes is None:
                    img_as_bytes = img.tobytes()

                for relinquished_subtask in relinquished:
                    tasks_queue.put(
                        _EncodeTask(
                            type="task",
                            action="encode",
                            id=relinquished_subtask.target_id,
                            resolution_id=task.resolution_id,
                            img=img_as_bytes,
                            width=task.width,
                            height=task.height,
                            subtasks=[relinquished_subtask],
                        )
                    )

        current_task: Optional[_GenerateEncodeSubtask] = subtasks.pop()
        subtasks_lock = threading.Lock()

        task = cast(Union[_GenerateTask, _EncodeTask], task)

        def _relinquish_watcher():

            _log(
                log_queue,
                logging.DEBUG,
                f"worker {id} relinquish watcher starting up",
            )
            while not shutdown_requested.is_set():
                with subtasks_lock:
                    if shutdown_requested.is_set():
                        return
                    if not subtasks:
                        _log(
                            log_queue,
                            logging.DEBUG,
                            f"worker {id} relinquish watcher shutting down (no more tasks to relinquish)",
                        )
                        return

                    with idle_workers_count.get_lock():
                        should_relinquish = idle_workers_count.value > 0

                    if not should_relinquish:
                        try:
                            time.sleep(0.1)
                        except (InterruptedError, KeyboardInterrupt):
                            return

                        continue

                    relinquished = subtasks[:]
                    subtasks.clear()

                _log(
                    log_queue,
                    logging.INFO,
                    f"worker {id} detected idle workers, relinquishing remaining subtasks",
                )

                real_img_as_bytes = (
                    img_as_bytes if img_as_bytes is not None else img.tobytes()
                )
                for relinquished_subtask in relinquished:
                    tasks_queue.put(
                        _EncodeTask(
                            type="task",
                            action="encode",
                            id=relinquished_subtask.target_id,
                            resolution_id=task.resolution_id,
                            img=real_img_as_bytes,
                            width=task.width,
                            height=task.height,
                            subtasks=[relinquished_subtask],
                        )
                    )
                return

        relinquish_watcher_thread = threading.Thread(
            target=_relinquish_watcher, name=f"relinquish_watcher_{id}"
        )
        relinquish_watcher_thread.start()

        while current_task is not None:
            _log(
                log_queue,
                logging.INFO,
                f"worker {id} encoding image @ {task.width}x{task.height} to {current_task.encode_format}",
            )
            start_at = time.perf_counter()
            try:
                img.save(
                    current_task.out_filepath,
                    format=current_task.encode_format,
                    **current_task.encode_quality_settings,
                )
            except BaseException as e:
                if shutdown_requested.is_set():
                    relinquish_watcher_thread.join()
                    return
                _log(
                    log_queue,
                    logging.ERROR,
                    f"worker {id} failed to encode image:\n\n{traceback.format_exc()}",
                )
                error_queue.put(e)
                shutdown_requested.wait()
                relinquish_watcher_thread.join()
                return

            if shutdown_requested.is_set():
                relinquish_watcher_thread.join()
                return

            encode_time = time.perf_counter() - start_at
            _log(
                log_queue,
                logging.INFO,
                f"worker {id} encoded image @ {task.width}x{task.height} to {current_task.encode_format} in {encode_time:.3f}s",
            )

            start_at = time.perf_counter()
            try:
                thumbhash_bytes_as_list = image_to_thumb_hash(current_task.out_filepath)
                thumbhash_bytes = bytes(thumbhash_bytes_as_list)
                thumbhash_b64url = base64.urlsafe_b64encode(thumbhash_bytes).decode(
                    "ascii"
                )
            except BaseException as e:
                if shutdown_requested.is_set():
                    return
                _log(
                    log_queue,
                    logging.ERROR,
                    f"worker {id} failed to compute thumbhash:\n\n{traceback.format_exc()}",
                )
                error_queue.put(e)
                shutdown_requested.wait()
                relinquish_watcher_thread.join()
                return

            if shutdown_requested.is_set():
                relinquish_watcher_thread.join()
                return

            thumbhash_time = time.perf_counter() - start_at
            _log(
                log_queue,
                logging.INFO,
                f"worker {id} computed thumbhash for {current_task.out_filepath} in {thumbhash_time:.3f}s",
            )

            filesize = os.path.getsize(current_task.out_filepath)
            task_completed_queue.put(
                GeneratedLocalImageFileExport(
                    uid=f"oseh_ife_{secrets.token_urlsafe(16)}",
                    width=task.width,
                    height=task.height,
                    filepath=current_task.out_filepath,
                    format=current_task.encode_format,
                    quality_settings=current_task.encode_quality_settings,
                    thumbhash=thumbhash_b64url,
                    file_size=filesize,
                )
            )

            with subtasks_lock:
                if subtasks:
                    current_task = subtasks.pop()
                else:
                    current_task = None

        _log(
            log_queue,
            logging.DEBUG,
            f"worker {id} finished {task!r}, waiting for watcher thread to finish",
        )
        relinquish_watcher_thread.join()
        if not shutdown_requested.is_set():
            _log(
                log_queue,
                logging.DEBUG,
                f"worker {id} watcher thread finished",
            )


def _log_target(
    shutdown_requested: MPEvent,
    log_queue: multiprocessing.Queue,
):
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)
    logging.info("log worker started")

    while not shutdown_requested.is_set():
        try:
            message = log_queue.get(timeout=0.1)
            assert isinstance(message, _LogMessage), message
            logging.log(message.level, message.message)

            for _ in range(100):
                message = log_queue.get_nowait()
                assert isinstance(message, _LogMessage), message
                logging.log(message.level, message.message)
        except QueueEmpty:
            pass
