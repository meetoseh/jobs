from itgs import Itgs
from lib.touch.touch_info import TouchLog
import redis.asyncio


async def queue_touch_db_write(itgs: Itgs, event: TouchLog):
    """Queues the given database write to be written to the database. This
    should only be used internally by the touch system.

    See Also: `queue_touch_db_write_in_pipe`
    """
    redis = await itgs.redis()
    await queue_touch_db_write_in_pipe(redis, event)


async def queue_touch_db_write_in_pipe(redis: redis.asyncio.Redis, event: TouchLog):
    """Queues the given database write to be written to the database within the
    given redis instance. Typically this is only invoked directly if you're
    using a pipeline, hence the name, though that is not required.

    Args:
        redis (redis.Redis): the redis instance to use
        event (TouchLog): the event to queue
    """
    await redis.rpush(b"touch:to_log", event.json().encode("utf-8"))
