import threading
import time
from typing import List
from graceful_death import GracefulDeath
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


async def _main(gd: GracefulDeath):
    stop_event: threading.Event = threading.Event()
    threads: List[threading.Thread] = []

    print(f"{os.getpid()=}")
    threads.append(
        threading.Thread(
            target=updater.listen_forever_sync, args=[stop_event], daemon=True
        )
    )
    threads.append(
        threading.Thread(
            target=recurring_jobs.run_forever_sync,
            args=[stop_event],
            daemon=True,
        )
    )

    for t in threads:
        t.start()

    async with Itgs() as itgs:
        jobs = await itgs.jobs()
        while not gd.received_term_signal and not stop_event.is_set():
            job = await jobs.retrieve(timeout=5)
            if job is None:
                continue
            try:
                mod = importlib.import_module(job["name"])
                started_at = time.perf_counter()
                await mod.execute(itgs, gd, **job["kwargs"])
                logging.info(
                    f"finished in {time.perf_counter() - started_at:.3f} seconds"
                )
            except Exception as e:
                await handle_error(e)
                continue

    stop_event.set()

    for t in threads:
        t.join(15)


def main():
    gd = GracefulDeath()
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)

    logging.config.dictConfig(logging_config)
    asyncio.run(_main(gd))


if __name__ == "__main__":
    main()
