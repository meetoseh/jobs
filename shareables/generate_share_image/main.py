import hashlib
import io
import json
import secrets
import time
from typing import Iterable, List, Optional, Tuple, Union, Any, cast
from content import hash_content
from error_middleware import handle_warning
from images import (
    ImageFile,
    ImageFileExport,
    ImageTarget,
    LocalImageFileExport,
    delete_s3_files,
    get_image_file,
    name_from_name_hint,
    upload_many_image_targets,
    upload_original,
)
from itgs import Itgs
from graceful_death import GracefulDeath
from lib.progressutils.progress_helper import ProgressHelper
from shareables.generate_share_image.exceptions import ShareImageBounceError
from shareables.generate_share_image.generator import ShareImageGenerator
import logging

from shareables.generate_share_image.parallelize import generate_targets
from shareables.generate_share_image.targets import TARGETS

from temp_files import temp_dir, temp_file


async def run_pipeline(
    itgs: Itgs,
    gd: GracefulDeath,
    generator: ShareImageGenerator,
    /,
    *,
    name_hint: str,
    job_progress_uid: Optional[str] = None,
) -> ImageFile:
    """Generates an image which has a custom pipeline depending on the resolution of
    the target. This will handle the glue code required to start child processes,
    generate the images in parallel, and upload/merge the result into a single
    ImageFile.

    Args:
        itgs (Itgs): the integrations to (re)use.
        gd (GracefulDeath): the graceful death handler.
        generator (ShareImageGenerator): the generator to use for actually generating
            the images.
        name_hint (str): the prefix for the name of the image file to generate; this is
            just metadata we associate with the image
        job_progress_uid (str, None): If specified, we report progress to the job with
            this uid. If None, we don't report progress.
    """
    try:
        if gd.received_term_signal:
            raise ShareImageBounceError()

        prog = ProgressHelper(itgs, job_progress_uid, log=True)
        await prog.push_progress(
            f"detecting if {name_hint} is already generated",
            indicator={"type": "spinner"},
        )

        logging.info(f"Getting configuration for {name_hint}...")
        custom_configuration = await generator.get_configuration(itgs, gd, prog)
        if gd.received_term_signal:
            raise ShareImageBounceError()

        configuration = {
            "pipeline": "shareables.generator_share_image.main.run_pipeline",
            "pipeline_version": "1.0.2",
            "generator": custom_configuration,
        }

        configuration_json = json.dumps(configuration, sort_keys=True, indent=2) + "\n"
        logging.info(f"Generating share image: {configuration_json}")

        _sha512 = hashlib.sha512()
        _sha512.update(configuration_json.encode("utf-8"))
        configuration_sha512 = _sha512.hexdigest()
        del _sha512

        conn = await itgs.conn()
        cursor = conn.cursor("weak")
        response = await cursor.execute(
            "SELECT uid FROM image_files WHERE original_sha512 = ?",
            (configuration_sha512,),
        )
        if gd.received_term_signal:
            raise ShareImageBounceError()

        existing_image_file_uid = (
            None if not response.results else cast(str, response.results[0][0])
        )
        targets = list(TARGETS)

        if existing_image_file_uid is not None:
            existing_image_file = await get_image_file(
                itgs, uid=existing_image_file_uid
            )
            if existing_image_file is None:
                existing_image_file_uid = None
            else:
                remaining_targets = dict(
                    (_make_target_key(target), target) for target in targets
                )
                for target in existing_image_file.exports:
                    remaining_targets.pop(_make_target_key(target), None)
                targets = list(remaining_targets.values())

                if not targets:
                    await prog.push_progress(
                        f"all targets already generated for {name_hint}"
                    )
                    return existing_image_file

        if gd.received_term_signal:
            raise ShareImageBounceError()

        logging.info(f"Preparing generator for {name_hint}...")
        await generator.prepare(itgs, gd, prog)

        with temp_dir() as out_dir:
            local_exports = await generate_targets(
                itgs,
                gd,
                generator,
                name_hint=name_hint,
                prog=prog,
                targets=targets,
                out_dir=out_dir,
            )

            # ensures that redis is reconnected
            await prog.push_progress(
                f"preparing to upload new exports for {name_hint} to s3",
                indicator={"type": "spinner"},
            )

            uploaded_exports = await upload_many_image_targets(
                [
                    LocalImageFileExport(
                        uid=export.uid,
                        width=export.width,
                        height=export.height,
                        filepath=export.filepath,
                        crop=(0, 0, 0, 0),
                        format=export.format,
                        quality_settings=export.quality_settings,
                        thumbhash=export.thumbhash,
                        file_size=export.file_size,
                    )
                    for export in local_exports
                ],
                itgs=itgs,
                gd=gd,
                job_progress_uid=prog.job_progress_uid,
                name_hint=name_hint,
            )

            queries: List[Tuple[str, Iterable[Any]]] = []
            if existing_image_file_uid is None:
                image_file_uid = f"oseh_if_{secrets.token_urlsafe(16)}"
                now = time.time()

                with temp_file(".json") as original_filepath:
                    with open(original_filepath, "w", newline="\n") as f:
                        f.write(configuration_json)

                    # verify we didn't corrupt the file now, rather than getting an error later
                    hashed_original = await hash_content(original_filepath)
                    assert (
                        hashed_original == configuration_sha512
                    ), f"{hashed_original=} != {configuration_sha512=}"

                    original = await upload_original(
                        original_filepath,
                        image_file_uid=image_file_uid,
                        now=now,
                        itgs=itgs,
                    )

                queries.append(
                    (
                        """
INSERT INTO image_files (
    uid,
    name,
    original_s3_file_id,
    original_sha512,
    original_width,
    original_height,
    created_at
)
SELECT
    ?, ?, s3_files.id, ?, ?, ?, ?
FROM s3_files
WHERE s3_files.uid = ?

                        """,
                        (
                            image_file_uid,
                            name_from_name_hint(name_hint),
                            configuration_sha512,
                            0,
                            0,
                            now,
                            original.uid,
                        ),
                    )
                )
            else:
                image_file_uid = existing_image_file_uid
                original = None

            query = io.StringIO()
            qargs = []
            query.write(
                "WITH new_exports(uid, s3_file_uid, width, height, format, quality_settings, thumbhash) "
                "AS (VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            for idx, exp in enumerate(uploaded_exports):
                if idx != 0:
                    query.write(", (?, ?, ?, ?, ?, ?, ?)")
                qargs.extend(
                    (
                        exp.local_image_file_export.uid,
                        exp.s3_file.uid,
                        exp.local_image_file_export.width,
                        exp.local_image_file_export.height,
                        exp.local_image_file_export.format,
                        json.dumps(
                            uploaded_exports[
                                idx
                            ].local_image_file_export.quality_settings,
                            sort_keys=True,
                        ),
                        exp.local_image_file_export.thumbhash,
                    )
                )
            query.write(
                """)
INSERT INTO image_file_exports (
    uid,
    image_file_id,
    s3_file_id,
    width,
    height,
    left_cut_px, right_cut_px, top_cut_px, bottom_cut_px,
    format,
    quality_settings,
    thumbhash,
    created_at
)
SELECT
    new_exports.uid,
    (SELECT image_files.id FROM image_files WHERE image_files.uid = ?),
    s3_files.id,
    new_exports.width,
    new_exports.height,
    0, 0, 0, 0,
    new_exports.format,
    new_exports.quality_settings,
    new_exports.thumbhash,
    ?
FROM new_exports, s3_files
WHERE
    s3_files.uid = new_exports.s3_file_uid
            """
            )
            qargs.append(image_file_uid)
            qargs.append(time.time())
            queries.append((query.getvalue(), qargs))
            del query
            del qargs

            queries.append(
                (
                    "SELECT name, created_at FROM image_files WHERE uid=?",
                    (image_file_uid,),
                )
            )

            response = await cursor.executeunified3(queries)

            if existing_image_file_uid is None:
                assert response[0].rows_affected == 1, response
                assert response[1].rows_affected == len(uploaded_exports), response

            if not response[-1].results:
                assert existing_image_file_uid is not None, response
                await handle_warning(
                    f"{__name__}:raced_delete",
                    f"While we were adding targets to {image_file_uid=}, it was deleted",
                )
                await delete_s3_files(
                    [export.s3_file for export in uploaded_exports], itgs=itgs
                )
                raise ShareImageBounceError()

            name = cast(str, response[-1].results[0][0])
            created_at = cast(float, response[-1].results[0][1])

            result = ImageFile(
                uid=image_file_uid,
                name=name,
                original_s3_file=original,
                original_sha512=configuration_sha512,
                original_width=0,
                original_height=0,
                created_at=created_at,
                exports=[],
            )

            await prog.push_progress(
                f"finalizing {name_hint}...",
                indicator={"type": "spinner"},
            )
    except ShareImageBounceError:
        raise
    except Exception:
        if gd.received_term_signal:
            raise ShareImageBounceError()
        raise

    await generator.finish(itgs, gd, prog, result)
    return result


def _make_target_key(
    target: Union[ImageTarget, ImageFileExport]
) -> Tuple[int, int, str, str]:
    return (
        target.width,
        target.height,
        target.format,
        json.dumps(target.quality_settings, sort_keys=True),
    )
