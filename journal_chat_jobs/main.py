"""
Module responsible for dispatching journal chat related jobs, which come from
a different queue than normal jobs and are assumed to be mostly network io,
so they can be run in parallel with other jobs and themselves (up to a limit)
"""

import secrets
import threading
from typing import Iterable, List, Optional, Union, cast

from error_middleware import handle_error, handle_warning
from itgs import Itgs
from journal_chat_jobs.dispatcher import handle_journal_chat_job
from lib.journals.journal_chat_redis_packet import (
    EventBatchPacketDataItemDataThinkingSpinner,
    JournalChatRedisPacketPassthrough,
)
from lib.journals.journal_chat_task import JournalChatTask
from lib.journals.master_keys import (
    get_journal_master_key_for_decryption,
    get_journal_master_key_for_encryption,
)
from lib.journals.publish_journal_chat_event import publish_journal_chat_event
from lib.journals.serialize_journal_chat_event import serialize_journal_chat_event
from mp_helper import adapt_threading_event_to_asyncio
import asyncio
import logging
import time
import socket

from redis_helpers.journal_chat_job_try_lock import safe_journal_chat_job_try_lock


MAX_CONCURRENT_JOURNAL_CHAT_JOBS = 10


async def _run_forever():
    logging.info("journal chat jobs _run_forever starting")

    failures_times: List[float] = []
    hostname_bytes = socket.gethostname().encode("utf-8")

    while True:
        logging.info("journal chat jobs _run_forever outer loop starting")
        try:
            async with Itgs() as itgs:
                running_tasks: Iterable[Union[asyncio.Task, asyncio.Future]] = []
                want_reset_itgs: bool = cast(bool, False)

                async def sweep_running():
                    nonlocal running_tasks, want_reset_itgs

                    found_done = False
                    for t in running_tasks:
                        if t.done():
                            found_done = True

                            exc = t.exception()
                            if exc is not None:
                                await handle_error(exc)
                                want_reset_itgs = True

                    if found_done:
                        running_tasks = [t for t in running_tasks if not t.done()]

                try:
                    redis = await itgs.redis()
                    awake = True
                    next_awake_from_time: Optional[float] = None

                    pubsub = redis.pubsub()
                    try:
                        await pubsub.subscribe(b"ps:journal_chat_jobs:queued")
                        waken_message_task = asyncio.create_task(
                            pubsub.get_message(
                                ignore_subscribe_messages=True, timeout=5
                            )
                        )

                        while not want_reset_itgs:
                            if not awake:
                                # good opportunity to sweep
                                await sweep_running()

                                now = time.time()
                                if next_awake_from_time is None:
                                    next_awake_from_time = now + 60
                                if now > next_awake_from_time:
                                    awake = True
                                    next_awake_from_time = None
                                    continue

                                try:
                                    waken_message = await asyncio.wait_for(
                                        waken_message_task,
                                        timeout=max(1, next_awake_from_time - now),
                                    )
                                    if waken_message is not None:
                                        awake = True
                                        next_awake_from_time = None
                                    waken_message_task = asyncio.create_task(
                                        pubsub.get_message(
                                            ignore_subscribe_messages=True, timeout=5
                                        )
                                    )
                                except asyncio.TimeoutError:
                                    if not waken_message_task.done():
                                        waken_message_task.cancel()
                                        try:
                                            await waken_message_task
                                        except (
                                            asyncio.TimeoutError,
                                            asyncio.CancelledError,
                                        ):
                                            ...
                                    waken_message_task = asyncio.create_task(
                                        pubsub.get_message(
                                            ignore_subscribe_messages=True, timeout=5
                                        )
                                    )
                                continue

                            if len(running_tasks) >= MAX_CONCURRENT_JOURNAL_CHAT_JOBS:
                                fin, running_tasks = await asyncio.wait(
                                    running_tasks, return_when=asyncio.FIRST_COMPLETED
                                )
                                for t in fin:
                                    exc = t.exception()
                                    if exc is not None:
                                        await handle_error(exc)
                                        want_reset_itgs = True
                                continue

                            log_id = secrets.token_urlsafe(16)
                            task_result = await safe_journal_chat_job_try_lock(
                                itgs,
                                queue_key=b"journals:journal_chat_jobs:priority",
                                now=int(time.time()),
                                log_id=log_id.encode("ascii"),
                                hostname=hostname_bytes,
                            )
                            if task_result.type == "empty_queue":
                                task_result = await safe_journal_chat_job_try_lock(
                                    itgs,
                                    queue_key=b"journals:journal_chat_jobs:normal",
                                    now=int(time.time()),
                                    log_id=log_id.encode("ascii"),
                                    hostname=hostname_bytes,
                                )

                            if task_result.type != "success":
                                awake = False
                                continue

                            awake = True
                            encryption_journal_master_key = (
                                await get_journal_master_key_for_encryption(
                                    itgs, user_sub=task_result.user_sub, now=time.time()
                                )
                            )
                            if encryption_journal_master_key.type != "success":
                                await handle_warning(
                                    f"{__name__}:journal_master_key:{encryption_journal_master_key.type}",
                                    f"failed to fetch journal master key for encryption for `{task_result.user_sub=}`",
                                    is_urgent=True,
                                )
                                continue

                            if (
                                encryption_journal_master_key.journal_master_key_uid
                                == task_result.journal_master_key_uid
                            ):
                                task = JournalChatTask.model_validate_json(
                                    encryption_journal_master_key.journal_master_key.decrypt(
                                        task_result.encrypted_task
                                    )
                                )
                            else:
                                decryption_journal_master_key = await get_journal_master_key_for_decryption(
                                    itgs,
                                    user_sub=task_result.user_sub,
                                    journal_master_key_uid=task_result.journal_master_key_uid,
                                )
                                if decryption_journal_master_key.type != "success":
                                    await handle_warning(
                                        f"{__name__}:journal_master_key:{decryption_journal_master_key.type}",
                                        f"failed to fetch journal master key for decryption for `{task_result.user_sub=}`",
                                        is_urgent=True,
                                    )
                                    continue
                                task = JournalChatTask.model_validate_json(
                                    decryption_journal_master_key.journal_master_key.decrypt(
                                        task_result.encrypted_task
                                    )
                                )

                            await publish_journal_chat_event(
                                itgs,
                                journal_chat_uid=task_result.journal_chat_uid,
                                event=serialize_journal_chat_event(
                                    journal_master_key=encryption_journal_master_key,
                                    event=JournalChatRedisPacketPassthrough(
                                        counter=task_result.num_events,
                                        type="passthrough",
                                        event=EventBatchPacketDataItemDataThinkingSpinner(
                                            type="thinking-spinner",
                                            message="worker assigned; initializing...",
                                            detail=None,
                                        ),
                                    ),
                                    now=time.time(),
                                ),
                            )
                            new_task = asyncio.create_task(
                                handle_journal_chat_job(
                                    itgs,
                                    journal_chat_uid=task_result.journal_chat_uid,
                                    journal_master_key=encryption_journal_master_key,
                                    starts=task_result.starts,
                                    start_time=task_result.start_time,
                                    log_id=log_id,
                                    queued_at=task_result.queued_at,
                                    user_sub=task_result.user_sub,
                                    journal_entry_uid=task_result.journal_entry_uid,
                                    next_event_counter=task_result.num_events + 1,
                                    task=task,
                                )
                            )
                            await new_task
                            if isinstance(running_tasks, list):
                                running_tasks.append(new_task)
                            elif isinstance(running_tasks, set):
                                running_tasks.add(new_task)
                            else:
                                running_tasks = list(running_tasks)
                                running_tasks.append(new_task)

                        # let jobs finish cleanly
                        if running_tasks:
                            _, running_tasks = await asyncio.wait(
                                running_tasks, return_when=asyncio.ALL_COMPLETED
                            )
                    finally:
                        logging.debug("journal chat jobs closing pubsub client")
                        await pubsub.close()
                finally:
                    logging.debug(
                        f"journal chat jobs inner loop ended with {len(running_tasks)=} running tasks; cancelling them"
                    )
                    if running_tasks:
                        for t in running_tasks:
                            t.cancel()
                        await asyncio.wait(
                            running_tasks, return_when=asyncio.ALL_COMPLETED
                        )

                logging.debug(
                    "journal chat jobs is restarting inner loop, presumably because we want to reset itgs"
                )
        except asyncio.CancelledError:
            logging.warning(
                "journal_chat_jobs _run_forever detected cancellation in outer loop, reraising",
                exc_info=True,
            )

            raise
        except Exception as e:
            await handle_error(e)

            now = time.time()
            failures_times = [t for t in failures_times if t > now - 60]
            failures_times.append(now)

            if len(failures_times) >= 5:
                async with Itgs() as itgs:
                    slack = await itgs.slack()
                    await slack.send_ops_message(
                        "jobs journal_chat_jobs _run_forever is failing too often. Exiting"
                    )
                    raise


async def run_forever(stop_event: threading.Event, stopping_event: threading.Event):
    asyncio_event = adapt_threading_event_to_asyncio(stop_event)
    asyncio_stopping_event = adapt_threading_event_to_asyncio(stopping_event)

    try:
        run_task = asyncio.create_task(_run_forever())
        stopping_event_task = asyncio.create_task(asyncio_stopping_event.wait())
        stopped_task = asyncio.create_task(asyncio_event.wait())
        _, pending = await asyncio.wait(
            [
                run_task,
                stopping_event_task,
                stopped_task,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # not generally safe to use logging after the stopping event

        print(
            "Journal chat jobs detected stop requested and is cancelling pending tasks"
        )

        for t in pending:
            t.cancel()

        print(
            "Journal chat jobs detected stop requested and cancelled pending tasks, waiting for cleanup to finish"
        )
        try:
            await run_task
        except asyncio.CancelledError:
            ...

        if asyncio_stopping_event.is_set():
            print("Journal chat jobs stopped on stopping event, waiting for real stop")
            await asyncio.wait_for(asyncio_event.wait(), timeout=300)
    except Exception as e:
        await handle_error(e)
    finally:
        stop_event.set()
        print("Journal chat jobs stopped")


def run_forever_sync(stop_event: threading.Event, stopping_event: threading.Event):
    """Handles recurring jobs, queuing them to the hot queue as it's time for
    them to run. Recurring jobs stop immediately upon an update being requested
    rather than waiting our turn, in case the recurring jobs change
    """
    asyncio.run(run_forever(stop_event, stopping_event))
