import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from error_middleware import handle_warning
from graceful_death import GracefulDeath
from PIL import Image
from itgs import Itgs
import multiprocessing
import multiprocessing.pool
import aiofiles
import secrets
import hashlib
import shutil
import time
import json
import os


@dataclass
class S3File:
    """An s3 file, see backend/docs/db/s3_files.md for more info"""

    uid: str
    bucket: str
    key: str
    file_size: int
    content_type: str
    created_at: float


@dataclass
class ImageFileExport:
    """An image file export, see backend/docs/db/image_file_exports.md for more info"""

    uid: str
    s3_file: S3File
    width: int
    height: int
    crop: Tuple[int, int, int, int]  # top, right, bottom, left
    format: str
    quality_settings: Dict[str, Any]
    created_at: float


@dataclass
class LocalImageFileExport:
    """An image file export we haven't necessarily uploaded to the s3_files table/file service"""

    uid: str
    width: int
    height: int
    filepath: str
    crop: Tuple[int, int, int, int]  # top, right, bottom, left
    format: str
    quality_settings: Dict[str, Any]
    file_size: int


@dataclass
class UploadedImageFileExport:
    """An image file we have uploaded to the s3_files table and to the file service,
    but not necessarily to image_file_exports
    """

    local_image_file_export: LocalImageFileExport
    s3_file: S3File


@dataclass
class ImageFile:
    """An image file, see backend/docs/db/image_files.md for more info"""

    uid: str
    name: str
    original_s3_file: S3File
    original_sha512: str
    original_width: int
    original_height: int
    created_at: float
    exports: List[ImageFileExport]


@dataclass
class ImageTarget:
    """Describes a target export for a given image"""

    required: bool
    """True if the image target must be generated. This primarily means that
    an exception will be raised if the original image is too small to generate
    the target
    """

    width: int
    """The width of the target"""

    height: int
    """The height of the target"""

    format: str
    """The format of the target, e.g., png. Acts as both the file extension and
    the format hint to pillow
    """

    quality_settings: Dict[str, Any]
    """Forwarded to the save function - see e.g.
    https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html#webp-saving
    for more info.

    Depends on the format, but generally dictates the quality vs size vs time tradeoff
    """


class ProcessImageException(Exception):
    pass


class ProcessImageAbortedException(ProcessImageException):
    pass


async def process_image(
    local_filepath: str,
    targets: List[ImageTarget],
    *,
    itgs: Itgs,
    gd: GracefulDeath,
    max_width: int,
    max_height: int,
    max_area: int,
    max_file_size: int,
    name_hint: str,
) -> ImageFile:
    """Processes the user-provided image which we have available at the given filepath
    and generates the given targets.

    This will deduplicate the image; if the original image has already been processed
    (based on it's sha512), then this only needs to generate the missing targets (if
    any). This means if we add additional targets, we should be able to just re-run
    this function on all existing images and they will be updated--this can often be done
    lazily based off an appropriate timestamp field.

    The returned image file may not include an export corresponding to each of the
    specified targets. If this is the case, this function guarrantees that such an
    export existed in the database at some point during the duration of this function,
    though not necessarily at the start or end. If you need an ImageFile with more
    exports than were returned, retry this function until such a result is received -
    this will return all exports that were either created by this function or were
    in the database for the entire duration of this function call.

    Args:
        local_file (str): Where the file is available on disk
        targets (List[ImageTarget]): The targets to generate
        itgs (Itgs): The integrations to use to connect to networked services, such as
            the database
        gd (GracefulDeath): This process can take a long time; this is used to check if
            we should abort. When aborting, a ProcessImageAbortedException will be raised,
            in which case this function should be called again on a clean instance.
        max_width (int): The maximum width of the image; if the image at that path is
            wider, an exception will be raised
        max_height (int): The maximum height of the image; if the image at that path is
            taller, an exception will be raised
        max_area (int): The maximum area of the image; if the image at that path is
            larger, an exception will be raised
        max_file_size (int): The maximum file size of the image; if the image at that path
            is larger, an exception will be raised. This is generally NOT the primary way
            that file size should be restricted, since the image has already been downloaded,
            however it's a good sanity check when the image comes from a trusted party.
        name_hint (str): A hint for the name of the image file.

    Returns:
        ImageFile: The image file that was processed

    Raises:
        ProcessImageAbortedException: when we want this function to be called again. this
            can occur because a term signal was received (checked via the GracefulDeath
            instance), or because we are using an optimistic concurrency control strategy
            and we got contention, so we'll have to restart.
        ProcessImageException: when the image does not meet the sanity checks, i.e., it's
            too big
        Exception: when something else goes wrong, such as the image not being in a known
            format (or not being an image at all)
    """
    _sanity_check_image(local_filepath, max_width, max_height, max_area, max_file_size)
    _verify_required_targets_possible(local_filepath, targets)
    name = _name_from_name_hint(name_hint)
    sha512 = await _hash_image(local_filepath)

    conn = await itgs.conn()
    cursor = conn.cursor()
    response = await cursor.execute(
        "SELECT uid FROM image_files WHERE original_sha512 = ?", (sha512,)
    )
    if response.results:
        return await _add_missing_targets(
            local_filepath, targets, uid=response.results[0][0], itgs=itgs, gd=gd
        )

    return await _make_new_image(
        local_filepath, targets, name=name, sha512=sha512, itgs=itgs, gd=gd
    )


async def _add_missing_targets(
    local_filepath: str,
    targets: List[ImageTarget],
    *,
    uid: str,
    itgs: Itgs,
    gd: GracefulDeath,
) -> ImageFile:
    """Adds any missing targets to the image file exports of the given image file
    and returns the updated image file. The returned image file will have any
    exports which were in the image file for the entire duration of this
    function plus any exports we added. Image file exports that were in the
    image file for only some of the duration of this function may or may not be
    in the result.

    Note that this means that the returned image file may not have an export
    corresponding to each specified target, as it may have been created by
    someone else during the runtime of this function. If this matters to you,
    you should keep retrying the parent function until you get an image file
    with all the exports you want.
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    image_file = await get_image_file(uid=uid, itgs=itgs)
    if image_file is None:
        raise ProcessImageException(f"Image file {uid=} does not exist")

    exports_lookup: Dict[Tuple[int, int, str, str], ImageFileExport] = dict(
        (
            (
                e.width,
                e.height,
                e.format,
                json.dumps(e.quality_settings, sort_keys=True),
            ),
            e,
        )
        for e in image_file.exports
    )

    missing_targets: List[ImageTarget] = [
        target
        for target in targets
        if (
            target.width,
            target.height,
            target.format,
            json.dumps(target.quality_settings, sort_keys=True),
        )
        not in exports_lookup
        and (
            target.width <= image_file.original_width
            and target.height <= image_file.original_height
        )
    ]

    if not missing_targets:
        return image_file

    if gd.received_term_signal:
        raise ProcessImageAbortedException("Received term signal")

    tmp_folder = os.path.join("tmp", secrets.token_hex(8))
    now = time.time()
    os.makedirs(tmp_folder)
    try:
        local_image_file_exports = await _make_targets(
            local_filepath, missing_targets, tmp_folder=tmp_folder, gd=gd
        )
        if gd.received_term_signal:
            raise ProcessImageAbortedException("Received term signal")

        uploaded = await _upload_many(local_image_file_exports, itgs=itgs, gd=gd)
        if gd.received_term_signal:
            await _delete_s3_files([export.s3_file for export in uploaded], itgs=itgs)
            raise ProcessImageAbortedException("Received term signal")

        response = await cursor.executemany3(
            [
                (
                    """
                    INSERT INTO image_file_exports (
                        uid, image_file_id, s3_file_id, width, height,
                        left_cut_px, right_cut_px, top_cut_px, bottom_cut_px,
                        format, quality_settings, created_at
                    )
                    SELECT
                        ?, image_files.id, s3_files.id, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?
                    FROM image_files
                    JOIN s3_files ON s3_files.uid = ?
                    WHERE
                        image_files.uid = ?
                    """,
                    (
                        export.local_image_file_export.uid,
                        export.local_image_file_export.width,
                        export.local_image_file_export.height,
                        *export.local_image_file_export.crop,
                        export.local_image_file_export.format,
                        json.dumps(
                            export.local_image_file_export.quality_settings,
                            sort_keys=True,
                        ),
                        now,
                        export.s3_file.uid,
                        uid,
                    ),
                )
                for export in uploaded
            ]
        )

        to_delete: List[S3File] = []
        for item, export in zip(response.items, uploaded):
            if item.rows_affected is None or item.rows_affected < 1:
                to_delete.append(export.s3_file)
                continue

            image_file.exports.append(
                ImageFileExport(
                    uid=export.local_image_file_export.uid,
                    s3_file=export.s3_file,
                    width=export.local_image_file_export.width,
                    height=export.local_image_file_export.height,
                    crop=export.local_image_file_export.crop,
                    format=export.local_image_file_export.format,
                    quality_settings=export.local_image_file_export.quality_settings,
                    created_at=now,
                )
            )

        await _delete_s3_files(to_delete, itgs=itgs)
        return image_file
    finally:
        shutil.rmtree(tmp_folder)


async def get_image_file(itgs: Itgs, uid: str) -> Optional[ImageFile]:
    """Loads the image file with the given uid, if it exists."""
    conn = await itgs.conn()
    cursor = conn.cursor()
    response = await cursor.execute(
        """
        SELECT
            s3_files.uid,
            s3_files.key,
            s3_files.file_size,
            s3_files.content_type,
            s3_files.created_at,
            image_files.name,
            image_files.original_sha512,
            image_files.original_width,
            image_files.original_height,
            image_files.created_at
        FROM image_files
        JOIN s3_files ON s3_files.id = image_files.original_s3_file_id
        WHERE
            image_files.uid = ?
        """,
        (uid,),
    )
    if not response.results:
        return None

    files = await itgs.files()
    original = S3File(
        uid=response.results[0][0],
        bucket=files.default_bucket,
        key=response.results[0][1],
        file_size=response.results[0][2],
        content_type=response.results[0][3],
        created_at=response.results[0][4],
    )

    image_file = ImageFile(
        uid=uid,
        name=response.results[0][5],
        original_s3_file=original,
        original_sha512=response.results[0][6],
        original_width=response.results[0][7],
        original_height=response.results[0][8],
        created_at=response.results[0][9],
        exports=[],
    )

    response = await cursor.execute(
        """
        SELECT
            image_file_exports.uid,
            s3_files.uid,
            s3_files.key,
            s3_files.file_size,
            s3_files.content_type,
            s3_files.created_at,
            image_file_exports.width,
            image_file_exports.height,
            image_file_exports.top_cut_px,
            image_file_exports.right_cut_px,
            image_file_exports.bottom_cut_px,
            image_file_exports.left_cut_px,
            image_file_exports.format,
            image_file_exports.quality_settings,
            image_file_exports.created_at
        FROM image_file_exports
        JOIN s3_files ON s3_files.id = image_file_exports.s3_file_id
        WHERE
            EXISTS (
                SELECT 1 FROM image_files
                WHERE image_files.id = image_file_exports.image_file_id
                  AND image_files.uid = ?
            )
        """,
        (uid,),
    )

    for row in response.results:
        export = ImageFileExport(
            uid=row[0],
            s3_file=S3File(
                uid=row[1],
                bucket=files.default_bucket,
                key=row[2],
                file_size=row[3],
                content_type=row[4],
                created_at=row[5],
            ),
            width=row[6],
            height=row[7],
            crop=tuple(row[8:12]),
            format=row[12],
            quality_settings=json.loads(row[13]),
            created_at=row[14],
        )
        image_file.exports.append(export)

    if not image_file.exports:
        return None

    return image_file


async def _make_new_image(
    local_filepath: str,
    targets: List[ImageTarget],
    *,
    name: str,
    sha512: str,
    itgs: Itgs,
    gd: GracefulDeath,
) -> ImageFile:
    with Image.open(local_filepath) as img:
        original_width, original_height = img.size

    image_file_uid = f"oseh_if_{secrets.token_urlsafe(16)}"
    now = time.time()
    tmp_folder = os.path.join("tmp", secrets.token_hex(8))
    os.makedirs(tmp_folder)
    try:
        local_image_file_exports, original = await asyncio.gather(
            _make_targets(local_filepath, targets, tmp_folder=tmp_folder, gd=gd),
            _upload_original(
                local_filepath, image_file_uid=image_file_uid, now=now, itgs=itgs
            ),
        )
        if gd.received_term_signal:
            await _delete_s3_files([original], itgs=itgs)
            raise ProcessImageAbortedException("Received term signal")

        uploaded = await _upload_many(local_image_file_exports, itgs=itgs, gd=gd)
        if gd.received_term_signal:
            await _delete_s3_files(
                [original, *[export.s3_file for export in uploaded]], itgs=itgs
            )
            raise ProcessImageAbortedException("Received term signal")

        now = time.time()
        conn = await itgs.conn()
        cursor = conn.cursor()

        response = await cursor.executemany3(
            (
                (
                    """
                    INSERT INTO image_files (
                        uid, name, original_s3_file_id, original_sha512,
                        original_width, original_height, created_at
                    )
                    SELECT
                        ?, ?, s3_files.id, ?, ?, ?, ?
                    FROM s3_files
                    WHERE
                        s3_files.uid = ?
                        AND NOT EXISTS (SELECT 1 FROM image_files WHERE original_sha512 = ?)
                    """,
                    (
                        image_file_uid,
                        name,
                        sha512,
                        original_width,
                        original_height,
                        now,
                        original.uid,
                        sha512,
                    ),
                ),
                *[
                    (
                        """
                        INSERT INTO image_file_exports (
                            uid, image_file_id, s3_file_id, width, height,
                            left_cut_px, right_cut_px, top_cut_px, bottom_cut_px,
                            format, quality_settings, created_at
                        )
                        SELECT
                            ?, image_files.id, s3_files.id, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?
                        FROM image_files
                        JOIN s3_files ON s3_files.uid = ?
                        WHERE
                            image_files.uid = ?
                        """,
                        (
                            export.local_image_file_export.uid,
                            export.local_image_file_export.width,
                            export.local_image_file_export.height,
                            *export.local_image_file_export.crop,
                            export.local_image_file_export.format,
                            json.dumps(
                                export.local_image_file_export.quality_settings,
                                sort_keys=True,
                            ),
                            now,
                            export.s3_file.uid,
                            image_file_uid,
                        ),
                    )
                    for export in uploaded
                ],
            )
        )

        if response[0].rows_affected is None or response[0].rows_affected <= 0:
            handle_warning(
                f"{__name__}:optimistic_insert_failed",
                f"optimistic insert for {sha512=} failed - inserted during create",
            )
            await _delete_s3_files(
                [
                    *[export.s3_file for export in uploaded],
                    original,
                ],
                itgs=itgs,
            )
            raise ProcessImageAbortedException("Optimistic concurrency control failed")

        for item in response.items[1:]:
            assert (
                item.rows_affected == 1
            ), f"expected 1 row affected, got {item.rows_affected=}"

        return ImageFile(
            uid=image_file_uid,
            name=name,
            original_s3_file=original,
            original_sha512=sha512,
            original_width=original_width,
            original_height=original_height,
            created_at=now,
            exports=[
                ImageFileExport(
                    uid=export.local_image_file_export.uid,
                    s3_file=export.s3_file,
                    width=export.local_image_file_export.width,
                    height=export.local_image_file_export.height,
                    crop=export.local_image_file_export.crop,
                    format=export.local_image_file_export.format,
                    quality_settings=export.local_image_file_export.quality_settings,
                    created_at=now,
                )
                for export in uploaded
            ],
        )
    finally:
        shutil.rmtree(tmp_folder)


async def _delete_s3_files(s3_files: List[S3File], *, itgs: Itgs) -> None:
    if not s3_files:
        return

    conn = await itgs.conn()
    cursor = conn.cursor("strong")

    s3_file_uids = [s3_file.uid for s3_file in s3_files]
    conn = await itgs.conn()
    cursor = conn.cursor("strong")
    await cursor.execute(
        "DELETE FROM s3_files WHERE uid IN ({})".format(
            ",".join("?" * len(s3_file_uids))
        ),
        s3_file_uids,
    )

    purg_mapping: Dict[str, float] = {}
    now = time.time()
    for s3_file in s3_files:
        purgatory_key = json.dumps(
            {
                "key": s3_file.key,
                "bucket": s3_file.bucket,
                "hint": "jobs/images#_delete_s3_files",
                "expected": True,
            },
            sort_keys=True,
        )
        purg_mapping[purgatory_key] = now

    redis = await itgs.redis()
    await redis.zadd("files:purgatory", mapping=purg_mapping)


async def _upload_original(
    local_filepath: str,
    *,
    image_file_uid: str,
    now: float,
    itgs: Itgs,
) -> S3File:
    files = await itgs.files()
    image_file_uid = f"oseh_if_{secrets.token_urlsafe(16)}"
    original = S3File(
        uid=f"oseh_s3f_{secrets.token_urlsafe(16)}",
        bucket=files.default_bucket,
        key=f"s3_files/images/originals/{image_file_uid}/{secrets.token_urlsafe(8)}",
        file_size=os.path.getsize(local_filepath),
        content_type="octet-stream",
        created_at=now,
    )

    redis = await itgs.redis()
    original_purgatory_key = json.dumps(
        {"key": original.key, "bucket": original.bucket}, sort_keys=True
    )
    await redis.zadd("files:purgatory", mapping={original_purgatory_key: now + 60 * 60})
    async with aiofiles.open(local_filepath, "rb") as f:
        await files.upload(f, bucket=original.bucket, key=original.key, sync=False)
    conn = await itgs.conn()
    cursor = conn.cursor("strong")
    await cursor.execute(
        """
        INSERT INTO s3_files (
            uid, key, file_size, content_type, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            original.uid,
            original.key,
            original.file_size,
            original.content_type,
            now,
        ),
    )
    await redis.zrem("files:purgatory", original_purgatory_key)
    return original


async def _upload_many(
    local_image_file_exports: List[LocalImageFileExport],
    itgs: Itgs,
    gd: GracefulDeath,
) -> List[UploadedImageFileExport]:
    """Uploads the given local image file exports asynchronously. The resulting list is
    already in the s3_files table.

    The returned items will be in the same order as provided, but there may be fewer of
    them if the upload was aborted.
    """
    max_concurrent_uploads = 8

    local_image_file_exports = sorted(
        local_image_file_exports, key=lambda x: x.file_size, reverse=True
    )

    mapped_exports: Dict[int, UploadedImageFileExport] = dict()
    running: Set[asyncio.Task] = set()
    remaining: List[int] = list(range(len(local_image_file_exports)))

    while True:
        while (
            not gd.received_term_signal
            and remaining
            and len(running) < max_concurrent_uploads
        ):
            idx = remaining.pop()
            local_image_file_export = local_image_file_exports[idx]
            running.add(
                asyncio.create_task(
                    _upload_one_with_idx(local_image_file_export, itgs, idx)
                )
            )

        if not running:
            break

        done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            idx, uploaded_image_file_export = task.result()
            mapped_exports[idx] = uploaded_image_file_export

    return [
        mapped_exports[idx]
        for idx in range(len(local_image_file_exports))
        if idx in mapped_exports
    ]


async def _upload_one_with_idx(
    local_image_file_export: LocalImageFileExport, itgs: Itgs, idx: int
) -> Tuple[int, UploadedImageFileExport]:
    """Uploads the given local image file export, returning the index and the uploaded
    image file export"""
    return idx, await _upload_one(local_image_file_export, itgs)


async def _upload_one(
    local_image_file_export: LocalImageFileExport, itgs: Itgs
) -> UploadedImageFileExport:
    redis = await itgs.redis()
    files = await itgs.files()
    conn = await itgs.conn()
    cursor = conn.cursor("strong")

    bucket = files.default_bucket
    key = f"s3_files/images/exports/{local_image_file_export.uid}/{secrets.token_urlsafe(8)}.{local_image_file_export.format}"

    now = time.time()
    uid = f"oseh_s3f_{secrets.token_urlsafe(16)}"
    content_type = f"image/{local_image_file_export.format.lower()}"
    times_out_in = 60 * 10
    times_out_at = now + times_out_in  # 10 minutes

    purgatory_key = json.dumps({"bucket": bucket, "key": key}, sort_keys=True)

    await redis.zadd("files:purgatory", {purgatory_key: times_out_at})
    async with aiofiles.open(local_image_file_export.filepath, "rb") as f:
        await files.upload(f, bucket=bucket, key=key, sync=False)
    await cursor.execute(
        """
        INSERT INTO s3_files(
            uid, key, file_size, content_type, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (uid, key, local_image_file_export.file_size, content_type, now),
    )
    await redis.zrem("files:purgatory", purgatory_key)
    return UploadedImageFileExport(
        local_image_file_export=local_image_file_export,
        s3_file=S3File(
            uid=uid,
            bucket=bucket,
            key=key,
            file_size=local_image_file_export.file_size,
            content_type=content_type,
            created_at=now,
        ),
    )


def _crops_to_pil_box(
    top: int, right: int, bottom: int, left: int, width: int, height: int
) -> Tuple[int, int, int, int]:
    """Returns the box achieved by cropping the given amount from
    the given dimensions. The returned box is pil-style, i.e.,
    (left, top, right, bottom)
    """
    return (
        left,
        top,
        width - right,
        height - bottom,
    )


async def _make_targets(
    local_filepath: str,
    targets: List[ImageTarget],
    *,
    tmp_folder: str,
    gd: GracefulDeath,
) -> List[LocalImageFileExport]:
    mapped_targets: Dict[int, Optional[LocalImageFileExport]] = dict()
    nprocesses = multiprocessing.cpu_count() // 2
    max_queued_jobs = nprocesses * 2
    with multiprocessing.Pool(processes=nprocesses) as pool:
        # we use apply_async instead of starmap_async because we want to not queue
        # all the jobs if we're terminated (esp if there are a lot of jobs)
        running: List[multiprocessing.pool.ApplyResult] = []
        remaining: List[int] = list(range(len(targets)))

        while True:
            while (
                not gd.received_term_signal
                and remaining
                and len(running) < max_queued_jobs
            ):
                target_idx = remaining.pop()
                target = targets[target_idx]
                running.append(
                    pool.apply_async(
                        _make_target_with_idx,
                        args=(
                            local_filepath,
                            target,
                            os.path.join(tmp_folder, f"{target_idx}.{target.format}"),
                            target_idx,
                        ),
                    )
                )

            if not running:
                break

            done: List[multiprocessing.pool.ApplyResult] = []
            while not done:
                new_running = []
                failed = False
                for result in running:
                    if result.ready():
                        if not result.successful():
                            failed = True
                            break
                        done.append(result)
                    else:
                        new_running.append(result)

                if failed:
                    for result in running:
                        result.wait()

                    for result in running:
                        if not result.successful():
                            raise result.get()

                running = new_running

                if not done:
                    running[0].wait(1)

            for task in done:
                idx, local_image_file_export = task.get()
                mapped_targets[idx] = local_image_file_export

    return list(v for v in mapped_targets.values() if v is not None)


def _make_target_with_idx(
    local_filepath: str,
    target: ImageTarget,
    tmp_filepath: str,
    idx: int,
) -> Tuple[int, Optional[LocalImageFileExport]]:
    return (idx, _make_target(local_filepath, target, tmp_filepath))


def _make_target(
    local_filepath: str,
    target: ImageTarget,
    target_filepath: str,  # where to store the target
) -> Optional[LocalImageFileExport]:  # None = not possible
    with Image.open(local_filepath) as img:
        if img.width < target.width or img.height < target.height:
            return None

        cropped_width: int = img.width
        cropped_height: int = img.height

        too_widedness = target.width * img.height - img.width * target.height
        if too_widedness > 0:
            # equivalent to target.width / target.height > img.width / img.height
            # but without floating point math
            # implies the target is too wide, so we need to crop some from the left
            # and right

            cropped_width = (img.height * target.width) // target.height
        elif too_widedness < 0:
            cropped_height = (img.width * target.height) // target.width

        required_x_crop = img.width - cropped_width
        required_y_crop = img.height - cropped_height
        crops: Tuple[int, int, int, int] = (  # top/right/bottom/left
            required_y_crop // 2,
            required_x_crop - (required_x_crop // 2),
            required_y_crop - (required_y_crop // 2),
            required_x_crop // 2,
        )

        if any(crop > 0 for crop in crops):
            img = img.crop(_crops_to_pil_box(*crops, img.width, img.height))

        if img.width != target.width or img.height != target.height:
            img = img.resize((target.width, target.height), Image.LANCZOS)

        img.save(target_filepath, format=target.format, **target.quality_settings)
        file_size = os.path.getsize(target_filepath)
        return LocalImageFileExport(
            uid=f"oseh_ife_{secrets.token_urlsafe(16)}",
            width=img.width,
            height=img.height,
            filepath=target_filepath,
            crop=crops,
            format=target.format,
            quality_settings=target.quality_settings,
            file_size=file_size,
        )


def _sanity_check_image(
    local_filepath: str,
    max_width: int,
    max_height: int,
    max_area: int,
    max_file_size: int,
):
    file_size_bytes = os.path.getsize(local_filepath)
    if file_size_bytes > max_file_size:
        raise ProcessImageException(
            f"File too large ({file_size_bytes} > {max_file_size})"
        )

    with Image.open(local_filepath) as img:
        if img.width > max_width:
            raise ProcessImageException(
                f"Image width {img.width=} is too large ({img.height=})"
            )

        if img.height > max_height:
            raise ProcessImageException(
                f"Image height {img.height=} is too large ({img.width=})"
            )

        area = img.width * img.height
        if area > max_area:
            raise ProcessImageException(
                f"Image area {area=} is too large ({img.width=}x{img.height=})"
            )


def _verify_required_targets_possible(
    local_filepath: str,
    targets: List[ImageTarget],
) -> None:
    with Image.open(local_filepath) as img:
        width, height = img.size

    for target in targets:
        if target.required and (target.width > width or target.height > height):
            raise ProcessImageException(
                f"Target {target=} is required, but image is too small ({width=}x{height=})"
            )


def _name_from_name_hint(name_hint: str) -> str:
    res = "".join(c for c in name_hint[:255] if c.isalnum() or c in "._-").lower()

    if len(res) < 5:
        res += secrets.token_urlsafe(5)

    return res


async def _hash_image(local_filepath: str) -> str:
    """Hashes the image at the given filepath using sha512"""
    sha512 = hashlib.sha512()
    async with aiofiles.open(local_filepath, mode="rb") as f:
        while True:
            chunk = await f.read(8192)
            if not chunk:
                break
            sha512.update(chunk)
    return sha512.hexdigest()
