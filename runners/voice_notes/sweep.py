import time
from typing import List, Optional, Tuple, cast
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory
from lib.shared.redis_hash import RedisHash

category = JobCategory.LOW_RESOURCE_COST

MAX_JOB_TIME_SECONDS = 10
"""The maximum duration in seconds this job can run before stopping itself to allow
other jobs to run
"""

WARN_STUCK_LONGER_THAN_S = 60
"""If a voice note has been processing for this duration in seconds, we will emit
a warning to slack (once per note)
"""

REMOVE_STUCK_LONGER_THAN_S = 60 * 60 * 24
"""If a voice note has been processing for this duration in seconds, we delete it from
redis and delete the stiched file from s3
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Sweeps over the voice note processing pseudoset, cleaning up entries that got
    stuck in the processing state for too long.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """

    started_at = time.time()
    last_score = cast(Optional[int], None)

    while True:
        if gd.received_term_signal:
            logging.info(f"{__name__} stopping early due to signal")
            break
        if time.time() - started_at > MAX_JOB_TIME_SECONDS:
            logging.info(f"{__name__} stopping early due to time")
            break

        redis = await itgs.redis()
        result = cast(
            List[Tuple[bytes, float]],
            await redis.zrangebyscore(
                b"voice_notes:processing",
                "-inf" if last_score is None else last_score,
                started_at - WARN_STUCK_LONGER_THAN_S,
                start=0,
                num=10,
                withscores=True,
            ),
        )
        if not result:
            logging.info(
                f"{__name__} stopping as there are no more voice notes to sweep"
            )
            break

        last_score = result[-1][1]

        for voice_note_uid_bytes, score in result:
            if score < started_at - REMOVE_STUCK_LONGER_THAN_S:
                logging.info(
                    f"{__name__} removing voice note {voice_note_uid_bytes.decode('utf-8')} as it has been stuck for too long"
                )

                old_data = RedisHash(await redis.hgetall(b"voice_notes:processing:" + voice_note_uid_bytes))  # type: ignore
                stitched_s3_key = old_data.get_str(b"stitched_s3_key")
                if stitched_s3_key != b"not_yet":
                    jobs = await itgs.jobs()
                    await jobs.enqueue("runners.delete_s3_file", key=stitched_s3_key)

                async with redis.pipeline() as pipe:
                    pipe.multi()
                    await pipe.delete(b"voice_notes:processing:" + voice_note_uid_bytes)
                    await pipe.zrem(b"voice_notes:processing", voice_note_uid_bytes)
                    await pipe.execute()

                await handle_warning(
                    f"{__name__}:cleanup_stuck",
                    f"Removed voice note `{voice_note_uid_bytes.decode('utf-8')}` as it was stuck for too long\n"
                    f"```\n{list(old_data.items_bytes())}\n```",
                )
            else:
                locked_for_warning = await redis.set(
                    b"voice_notes:warned_stuck:" + voice_note_uid_bytes,
                    b"1",
                    nx=True,
                    ex=REMOVE_STUCK_LONGER_THAN_S * 2,
                )
                if not locked_for_warning:
                    logging.debug(
                        f"{__name__} not warning for still stuck voice note `{voice_note_uid_bytes.decode('utf-8')}` as it's already been warned about"
                    )
                    continue

                async with redis.pipeline() as pipe:
                    pipe.multi()
                    await pipe.incr(b"voice_notes:stuck_recently")
                    await pipe.expire(b"voice_notes:stuck_recently", 60 * 60 * 24)
                    await pipe.execute()

                old_data = RedisHash(await redis.hgetall(b"voice_notes:processing:" + voice_note_uid_bytes))  # type: ignore
                await handle_warning(
                    f"{__name__}:stuck",
                    f"Voice note `{voice_note_uid_bytes.decode('utf-8')}` has been processing for more than {WARN_STUCK_LONGER_THAN_S}s\n"
                    f"```\n{list(old_data.items_bytes())}\n```",
                )


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.voice_notes.sweep")

    asyncio.run(main())
