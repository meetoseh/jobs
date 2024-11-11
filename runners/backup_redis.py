"""Redis database backups"""

import time
from typing import List, Optional, Tuple, cast
from file_service import SyncWritableBytesIO
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import hashlib
import redis.asyncio
from redis.exceptions import NoScriptError
from temp_files import temp_file

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Scans the redis database and backups all non-expiring keys.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    logging.info("Starting redis backup...")
    started_at = time.time()

    started_at = time.time()
    backup_key = f"s3_files/backup/redis/{int(started_at)}.bak"
    with temp_file() as backup_file:
        with open(backup_file, "wb") as f:
            await create_redis_dump(itgs, f)

        backup_created_at = time.time()
        logging.info(
            f"Redis backup created in {backup_created_at - started_at:.3f} seconds"
        )

        files = await itgs.files()
        with open(backup_file, "rb") as f:
            await files.upload(
                f, bucket=files.default_bucket, key=backup_key, sync=True
            )

        backup_uploaded_at = time.time()
        logging.info(
            f"Redis backup uploaded in {backup_uploaded_at - backup_created_at:.3f} seconds"
        )

    logging.info(
        f"Redis backup completed in {backup_uploaded_at - started_at:.3f} seconds -> {backup_key}"
    )


async def create_redis_dump(itgs: Itgs, out: SyncWritableBytesIO) -> None:
    """Writes a redis dump file to the given output stream.

    The dump file is formatted as a repeated sequence of
    (key length as an uint32, key, value length as a uint32, value)

    Args:
        itgs (Itgs): The integrations to (re)use
        out (io.BytesIO): The stream to write the dump to
    """
    redis = await itgs.redis()
    cursor: Optional[int] = None
    while cursor != 0:
        cursor, keys_and_dumps = await scan_and_filter_expiring_and_dump(
            redis, cursor if cursor is not None else 0
        )

        for i in range(0, len(keys_and_dumps), 2):
            key = keys_and_dumps[i]
            dump = keys_and_dumps[i + 1]

            out.write(len(key).to_bytes(4, "big", signed=False))
            out.write(key)
            out.write(len(dump).to_bytes(4, "big", signed=False))
            out.write(dump)


REDIS_SCAN_AND_FILTER_EXPIRING_SCRIPT = """
local cursor = ARGV[1]

local scan_response = redis.call('SCAN', cursor)
local new_cursor = scan_response[1]
local keys = scan_response[2]
local result = {}

if keys ~= nil then
    for i, key in ipairs(keys) do
        local expires_at = redis.call('EXPIRETIME', key)
        if expires_at == -1 then
            result[#result + 1] = key
            result[#result + 1] = redis.call('DUMP', key)
        end
    end
end

return {new_cursor, result}
"""

REDIS_SCAN_AND_FILTER_EXPIRING_SCRIPT_SHA = hashlib.sha1(
    REDIS_SCAN_AND_FILTER_EXPIRING_SCRIPT.encode("utf-8")
).hexdigest()


async def scan_and_filter_expiring_and_dump(
    redis: redis.asyncio.Redis, cursor: int
) -> Tuple[int, List[bytes]]:
    try:
        result = await redis.evalsha(  # type: ignore
            REDIS_SCAN_AND_FILTER_EXPIRING_SCRIPT_SHA, 0, cursor  # type: ignore
        )
    except NoScriptError:
        correct_sha = await redis.script_load(REDIS_SCAN_AND_FILTER_EXPIRING_SCRIPT)
        assert correct_sha == REDIS_SCAN_AND_FILTER_EXPIRING_SCRIPT_SHA
        result = await redis.evalsha(
            REDIS_SCAN_AND_FILTER_EXPIRING_SCRIPT_SHA, 0, cursor  # type: ignore
        )

    return int(result[0]), cast(List[bytes], result[1])


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.backup_redis")

    asyncio.run(main())
