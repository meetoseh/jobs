import journal_chat_jobs.main
from mp_helper import adapt_threading_event_to_asyncio
import mpfix  # type: ignore
import threading
import time
from typing import List
from graceful_death import GracefulDeath, graceful_sleep
import updater
import recurring_jobs
import asyncio
from itgs import Itgs
import importlib
from error_middleware import handle_error
import yaml
import logging.config
import logging
import os
import perpetual_pub_sub
import lib.transcripts.cache
import lib.users.entitlements
import lib.users.stripe_prices
import lib.journeys.read_one_external
import stability


async def _main(gd: GracefulDeath):
    stop_event: threading.Event = threading.Event()
    stopping_event: threading.Event = threading.Event()
    threads: List[threading.Thread] = []

    assert perpetual_pub_sub.instance is None
    perpetual_pub_sub.instance = perpetual_pub_sub.PerpetualPubSub()

    print(f"{os.getpid()=}")
    threads.append(
        threading.Thread(
            target=updater.listen_forever_sync,
            args=[stop_event, stopping_event],
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=recurring_jobs.run_forever_sync,
            args=[stop_event, stopping_event],
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=journal_chat_jobs.main.run_forever_sync,
            args=[stop_event, stopping_event],
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=stability.run_forever_sync,
            args=[stop_event, stopping_event],
            daemon=True,
        )
    )

    background_tasks = set()
    background_tasks.add(
        asyncio.create_task(perpetual_pub_sub.instance.run_in_background_async())
    )
    background_tasks.add(
        asyncio.create_task(lib.transcripts.cache.actively_sync_local_cache())
    )
    background_tasks.add(
        asyncio.create_task(lib.users.entitlements.purge_cache_loop_async())
    )
    background_tasks.add(
        asyncio.create_task(lib.users.stripe_prices.subscribe_to_stripe_price_updates())
    )
    background_tasks.add(
        asyncio.create_task(lib.journeys.read_one_external.cache_push_loop())
    )

    for t in threads:
        t.start()

    while not gd.received_term_signal and not stop_event.is_set():
        try:
            async with Itgs() as itgs:
                jobs = await itgs.jobs()
                while not gd.received_term_signal and not stop_event.is_set():
                    jobs = await itgs.jobs()
                    try:
                        job = await jobs.retrieve(timeout=5)
                    except Exception as e:
                        await handle_error(e)
                        await graceful_sleep(gd, 10)
                        break
                    if job is None:
                        continue

                    logging.info(f"starting {job['name']}")
                    logging.debug(f"{job=}")
                    try:
                        mod = importlib.import_module(job["name"])
                        started_at = time.perf_counter()
                        await mod.execute(itgs, gd, **job["kwargs"])
                        time_taken = time.perf_counter() - started_at
                        logging.info(f"finished in {time_taken:.3f} seconds")

                        if time_taken > 5:
                            await itgs.ensure_redis_liveliness()
                    except Exception as e:
                        await handle_error(e)
                        break
        except Exception as e:
            await handle_error(e)
            await graceful_sleep(gd, 30)

    stop_event.set()
    perpetual_pub_sub.instance.exit_event.set()

    for t in threads:
        t.join(15)

    await adapt_threading_event_to_asyncio(
        perpetual_pub_sub.instance.exitted_event
    ).wait()

    await asyncio.wait(background_tasks, return_when=asyncio.ALL_COMPLETED)


def main():
    gd = GracefulDeath()
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)
    asyncio.run(_main(gd))


if __name__ == "__main__":
    main()
