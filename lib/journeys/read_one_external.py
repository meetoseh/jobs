import asyncio
import json
import secrets
import time
from typing import (
    AsyncIterator,
    Dict,
    List,
    NoReturn,
    Optional,
    Tuple,
    cast as typing_cast,
)
from lib.content_files.models import ContentFileRef
import lib.content_files.auth
from error_middleware import handle_error
from file_service import SyncReadableBytesIO
import lib.image_files.auth
from pydantic import BaseModel, Field
from lib.image_files.image_file_ref import ImageFileRef
from lib.journeys.external_journey import (
    ExternalJourney,
    ExternalJourneyCategory,
    ExternalJourneyDescription,
    ExternalJourneyInstructor,
)
from lib.transcripts.transcript_ref import TranscriptRef
import perpetual_pub_sub as pps
from itgs import Itgs
import io
import logging
import lib.transcripts.auth


async def read_one_external(
    itgs: Itgs, *, journey_uid: str
) -> Optional[ExternalJourney]:
    """Reads the required information about the journey with the given UID to return
    the appropriate ExternalJourney. Due to collaborative caching, this is often
    achievable with no network calls

    This uses a similar flow to the webserver backend implementation which is focused
    on quickly providing the serialized representation with JWTs embedded, but instead
    this is focused on quickly returning the parsed model with blank JWTs.

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The UID of the journey to read
    """
    req_id = secrets.token_urlsafe(4)
    logging.debug(f"{req_id=} {journey_uid=}")
    locally_cached = await read_local_cache(itgs, journey_uid)
    if locally_cached is not None:
        logging.debug(f"{req_id=} {journey_uid=} LOCAL CACHE HIT")
        return ExternalJourney.model_validate_json(locally_cached)

    logging.debug(f"{req_id=} {journey_uid=} LOCAL CACHE MISS")

    redis = await itgs.redis()
    got_lock = await redis.set(
        f"journeys:external:cache_lock:{journey_uid}", "1", ex=3, nx=True
    )
    if not got_lock:
        logging.debug(f"{req_id=} {journey_uid=} REMOTE CACHE LOCKED")
        received_data_event = asyncio.Event()
        received_data_task = asyncio.create_task(received_data_event.wait())
        arr = waiting_for_cache.get(journey_uid)
        if arr is None:
            arr = []
            waiting_for_cache[journey_uid] = arr

        received_data_loop_event_tuple = (
            asyncio.get_running_loop(),
            received_data_event,
        )
        arr.append(received_data_loop_event_tuple)

        try:
            await asyncio.wait_for(received_data_task, timeout=3)
            logging.debug(f"{req_id=} {journey_uid=} received data event")
            locally_cached = await read_local_cache(itgs, journey_uid)
            if locally_cached is not None:
                logging.debug(f"{req_id=} {journey_uid=} PUSHED CACHE HIT")
                return ExternalJourney.model_validate_json(locally_cached)

            try:
                raise Exception("shouldn't happen")
            except Exception as e:
                await handle_error(
                    e,
                    extra_info="received data event but no data in cache (external journey)",
                )
            # fall down to assuming we got the lock
        except asyncio.TimeoutError as e:
            received_data_task.cancel()
            await handle_error(
                e,
                extra_info=(
                    "timed out waiting for external journey, either instance died (in which "
                    "case this will safely recover), or it's taking way too long (check db health). "
                    "going to assume control over the lock"
                ),
            )
            try:
                arr.remove(received_data_loop_event_tuple)
            except ValueError:
                await handle_error(e)  # i think this shouldn't happen

            # fall down to assuming we got the lock

    logging.debug(f"{req_id=} {journey_uid=} reading from source")

    now = time.time()
    journey = await read_from_db(itgs, journey_uid)
    if journey is None:
        logging.warning(f"{req_id=} {journey_uid=} NOT FOUND")
        return None

    logging.debug(
        f"{req_id=} {journey_uid=} READ FROM SOURCE...pushing to other instances"
    )

    remotely_cacheable = io.BytesIO()
    convert_to_cacheable(journey, remotely_cacheable)
    remotely_cacheable.seek(0)

    await push_to_caches(itgs, journey_uid, remotely_cacheable.getvalue(), now)
    logging.debug(f"{req_id=} {journey_uid=} RELEASING LOCK")
    await redis.delete(f"journeys:external:cache_lock:{journey_uid}")
    logging.debug(f"{req_id=} {journey_uid=} returning")
    return journey


async def read_local_cache(
    itgs: Itgs, journey_uid: str
) -> Optional[bytes]:
    """Reads the raw data available in the local cache for the journey with the
    given UID. If data is available, it's returned either completely in memory or via
    a file-like object, depending on its size and hardware characteristics. If
    no data is available, None is returned.

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The UID of the journey to read
    """
    local_cache = await itgs.local_cache()
    return typing_cast(
        Optional[bytes],
        local_cache.get(f"journeys:external:{journey_uid}".encode("utf-8"), read=False),
    )


async def inject_from_cached(
    itgs: Itgs, cached: SyncReadableBytesIO, jwt: str
) -> AsyncIterator[bytes]:
    """Injects the required information into the cached journey data to return
    the appropriate dynamic ExternalJourney response object, already serialized.
    This will inject the specified journey jwt, as well as generating
    any needed image file or content file jwts on the fly.

    Args:
        itgs (Itgs): The integrations to (re)use for creating image file and content file jwts
        cached (io.BytesIO): The cached journey data
        jwt (str): The JWT which provides the user access to the journey, which is inserted
            into the response

    Yields:
        bytes: The next part of the response
    """
    while True:
        value_length_bytes = cached.read(4)
        if not value_length_bytes:
            break
        value_length = int.from_bytes(value_length_bytes, "big", signed=False)

        part_type = cached.read(1)
        value = cached.read(value_length)

        if part_type == b"\x01":
            yield value
        elif part_type == b"\x02":
            yield jwt.encode("ascii")
        elif part_type == b"\x03":
            image_file_uid = value.decode("ascii")
            image_file_jwt = await lib.image_files.auth.create_jwt(itgs, image_file_uid)
            yield image_file_jwt.encode("ascii")
        elif part_type == b"\x04":
            content_file_uid = value.decode("ascii")
            content_file_jwt = await lib.content_files.auth.create_jwt(
                itgs, content_file_uid
            )
            yield content_file_jwt.encode("ascii")
        elif part_type == b"\x05":
            transcript_uid = value.decode("ascii")
            transcript_jwt = await lib.transcripts.auth.create_jwt(itgs, transcript_uid)
            yield transcript_jwt.encode("ascii")
        else:
            raise ValueError(f"Unknown part type {part_type}")


def parse_cached_with_blank_jwts(cached: SyncReadableBytesIO) -> ExternalJourney:
    """Parses the cached journey data into an ExternalJourney model, with all the
    jwts left blank.
    """
    valid_json = io.BytesIO()
    while True:
        value_length_bytes = cached.read(4)
        if not value_length_bytes:
            break
        value_length = int.from_bytes(value_length_bytes, "big", signed=False)

        part_type = cached.read(1)
        value = cached.read(value_length)

        if part_type == b"\x01":
            valid_json.write(value)
        elif part_type == b"\x02":
            continue
        elif part_type == b"\x03":
            continue
        elif part_type == b"\x04":
            continue
        elif part_type == b"\x05":
            continue
        else:
            raise ValueError(f"Unknown part type {part_type}")

    return ExternalJourney.model_validate_json(valid_json.getvalue())


def convert_to_cacheable(journey: ExternalJourney, f: io.BytesIO) -> None:
    """Serializes the given journey in the format required for caching, and writes
    it to the given file-like object. This writes in parts, so it can benefit from
    buffering.

    This ignores the session uid and any jwts that may be present, as they are
    dynamic and should not be cached.

    Args:
        journey (ExternalJourney): The journey to serialize
        f (io.BytesIO): The file-like object to write to. Must be seekable
    """
    mark_start = f.tell()
    f.write(b"\x00\x00\x00\x00\x01")

    def finish_mark():
        nonlocal mark_start

        curr = f.tell()
        f.seek(mark_start)
        f.write((curr - mark_start - 5).to_bytes(4, "big", signed=False))
        f.seek(curr)
        mark_start = curr

    f.write(b'{"uid":"')
    f.write(journey.uid.encode("ascii"))
    f.write(b'","jwt":"')
    finish_mark()
    f.write(b'\x00\x00\x00\x00\x02\x00\x00\x00\x00\x01","background_image":{"uid":"')
    mark_start += 5
    f.write(journey.background_image.uid.encode("ascii"))
    f.write(b'","jwt":"')
    finish_mark()
    f.write(b"\x00\x00\x00\x00\x03")
    f.write(journey.background_image.uid.encode("ascii"))
    finish_mark()
    f.write(b'\x00\x00\x00\x00\x01"},"blurred_background_image":{"uid":"')
    f.write(journey.blurred_background_image.uid.encode("ascii"))
    f.write(b'","jwt":"')
    finish_mark()
    f.write(b"\x00\x00\x00\x00\x03")
    f.write(journey.blurred_background_image.uid.encode("ascii"))
    finish_mark()
    f.write(b'\x00\x00\x00\x00\x01"},"darkened_background_image":{"uid":"')
    f.write(journey.darkened_background_image.uid.encode("ascii"))
    f.write(b'","jwt":"')
    finish_mark()
    f.write(b"\x00\x00\x00\x00\x03")
    f.write(journey.darkened_background_image.uid.encode("ascii"))
    finish_mark()
    f.write(b'\x00\x00\x00\x00\x01"},"audio_content":{"uid":"')
    f.write(journey.audio_content.uid.encode("ascii"))
    f.write(b'","jwt":"')
    finish_mark()
    f.write(b"\x00\x00\x00\x00\x04")
    f.write(journey.audio_content.uid.encode("ascii"))
    finish_mark()
    f.write(b'\x00\x00\x00\x00\x01"},"category":{"external_name":')
    f.write(json.dumps(journey.category.external_name).encode("utf-8"))
    f.write(b'},"title":')
    f.write(json.dumps(journey.title).encode("utf-8"))
    f.write(b',"instructor":{"name":')
    f.write(json.dumps(journey.instructor.name).encode("utf-8"))
    f.write(b'},"description":{"text":')
    f.write(json.dumps(journey.description.text).encode("utf-8"))
    f.write(b'},"duration_seconds":')
    f.write(str(journey.duration_seconds).encode("ascii"))
    f.write(b',"interactive_prompt_uid":"')
    f.write(journey.interactive_prompt_uid.encode("ascii"))
    if journey.sample is None:
        f.write(b'","sample":null')
    else:
        f.write(b'","sample":{"uid":"')
        f.write(journey.sample.uid.encode("ascii"))
        f.write(b'","jwt":"')
        finish_mark()
        f.write(b"\x00\x00\x00\x00\x04")
        f.write(journey.sample.uid.encode("ascii"))
        finish_mark()
        f.write(b'\x00\x00\x00\x00\x01"}')
    if journey.transcript is None:
        f.write(b',"transcript":null')
    else:
        f.write(b',"transcript":{"uid":"')
        f.write(journey.transcript.uid.encode("ascii"))
        f.write(b'","jwt":"')
        finish_mark()
        f.write(b"\x00\x00\x00\x00\x05")
        f.write(journey.transcript.uid.encode("ascii"))
        finish_mark()
        f.write(b'\x00\x00\x00\x00\x01"}')
    f.write(b"}")
    finish_mark()


async def write_to_local_cache(itgs: Itgs, journey_uid: str, f: io.BytesIO) -> None:
    """Writes the given file-like object to the local cache for the journey with
    the given UID. This must be in the cacheable representation described under the
    diskcache key `journeys:external:{uid}`. This will not write to other instances,
    and will automatically expire the cache after 2 days.

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The UID of the journey to write to
        f (io.BytesIO): The file-like object to write
    """
    local_cache = await itgs.local_cache()
    local_cache.set(
        f"journeys:external:{journey_uid}".encode("utf-8"),
        f,
        expire=60 * 60 * 24 * 2,
        read=True,
        tag="collab",
    )


async def delete_from_local_cache(itgs: Itgs, journey_uid: str) -> None:
    """Deletes the local cache for the journey with the given UID, if it exists.
    This will not delete from other instances.

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The UID of the journey to delete
    """
    local_cache = await itgs.local_cache()
    local_cache.delete(f"journeys:external:{journey_uid}".encode("utf-8"))


async def read_from_db(itgs: Itgs, journey_uid: str) -> Optional[ExternalJourney]:
    """Reads the journey with the given UID from the database, and returns it
    as an ExternalJourney model. The session uid and any jwts are set to empty
    strings.

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The UID of the journey to read

    Returns:
        ExternalJourney, None: The journey, if it exists, otherwise None.
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            image_files.uid,
            content_files.uid,
            content_files.duration_seconds,
            journey_subcategories.external_name,
            journeys.title,
            instructors.name,
            journeys.description,
            blurred_image_files.uid,
            darkened_image_files.uid,
            samples.uid,
            transcripts.uid,
            interactive_prompts.uid
        FROM journeys
        JOIN image_files ON image_files.id = journeys.background_image_file_id
        JOIN image_files AS blurred_image_files ON blurred_image_files.id = journeys.blurred_background_image_file_id
        JOIN image_files AS darkened_image_files ON darkened_image_files.id = journeys.darkened_background_image_file_id
        JOIN content_files ON content_files.id = journeys.audio_content_file_id
        JOIN journey_subcategories ON journey_subcategories.id = journeys.journey_subcategory_id
        JOIN instructors ON instructors.id = journeys.instructor_id
        JOIN interactive_prompts ON interactive_prompts.id = journeys.interactive_prompt_id
        LEFT OUTER JOIN content_files AS samples ON samples.id = journeys.sample_content_file_id
        LEFT OUTER JOIN transcripts ON transcripts.id = (
            SELECT content_file_transcripts.transcript_id 
            FROM content_file_transcripts 
            WHERE 
                content_file_transcripts.content_file_id = journeys.audio_content_file_id 
            ORDER BY content_file_transcripts.created_at ASC, content_file_transcripts.uid
        )
        WHERE
            journeys.uid = ?
        """,
        (journey_uid,),
    )

    if not response.results:
        return None

    row = response.results[0]

    return ExternalJourney(
        uid=journey_uid,
        jwt="",
        duration_seconds=row[2],
        background_image=ImageFileRef(uid=row[0], jwt=""),
        audio_content=ContentFileRef(uid=row[1], jwt=""),
        category=ExternalJourneyCategory(external_name=row[3]),
        title=row[4],
        instructor=ExternalJourneyInstructor(name=row[5]),
        description=ExternalJourneyDescription(text=row[6]),
        blurred_background_image=ImageFileRef(uid=row[7], jwt=""),
        darkened_background_image=ImageFileRef(uid=row[8], jwt=""),
        sample=ContentFileRef(uid=row[9], jwt="") if row[9] is not None else None,
        transcript=TranscriptRef(uid=row[10], jwt="") if row[10] is not None else None,
        interactive_prompt_uid=row[11],
    )


async def push_to_caches(
    itgs: Itgs, journey_uid: str, cached: bytes, fetched_at: float
) -> None:
    """Shares a cached representation of the journey with the given uid to other
    instances, so they don't have to fill their cache separately. This will also
    update our own cache after a short delay.

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The UID of the journey that has been updated
        cached (bytes): The cached representation of the journey
        fetched_at (float): The timestamp at which the journey was fetched
    """
    initial_part = (
        JourneysExternalPushCachePubSubMessage(
            uid=journey_uid, min_checked_at=time.time(), have_updated=True
        )
        .model_dump_json()
        .encode("utf-8")
    )

    message = io.BytesIO(bytearray(4 + len(initial_part) + len(cached)))
    message.write(len(initial_part).to_bytes(4, "big", signed=False))
    message.write(initial_part)
    message.write(cached)

    redis = await itgs.redis()
    await redis.publish(
        b"ps:journeys:external:push_cache",
        message.getvalue(),
    )


async def evict_external_journey(itgs: Itgs, uid: str) -> None:
    """Purges the cached representation of the journey with the given uid from
    all instances, including our own. This should be called when the journey is
    modified (or deleted).

    Args:
        itgs (Itgs): The integrations to (re)use
        journey_uid (str): The UID of the journey that has been updated
    """
    initial_part = (
        JourneysExternalPushCachePubSubMessage(
            uid=uid, min_checked_at=time.time(), have_updated=False
        )
        .model_dump_json()
        .encode("utf-8")
    )

    redis = await itgs.redis()
    await redis.publish(
        b"ps:journeys:external:push_cache",
        len(initial_part).to_bytes(4, "big", signed=False) + initial_part,
    )


waiting_for_cache: Dict[str, List[Tuple[asyncio.AbstractEventLoop, asyncio.Event]]] = {}
"""This mutable dictionary maps from keys of journey uids to a list of events
which should be set when we recieve a message from another instance about that
journey, after it's been updated. The events are set once and then the list is
removed from the dictionary. This isn't cleaned by the cache push loop unless
a relevant message is received, so those adding to this dictionary should
have timeouts to clean up after themselves if they don't receive a message
"""


async def cache_push_loop() -> NoReturn:
    """Loops until the perpetual pub sub connection is closed, constantly listening
    for messages from (other) instances about journeys that have been updated, and
    purging or updating our cache appropriately. This should be a background task
    that is started when the server starts, as it will mostly idle.
    """
    assert pps.instance is not None
    running_loop = asyncio.get_running_loop()
    try:
        async with pps.PPSSubscription(
            pps.instance, "ps:journeys:external:push_cache", "je-cpl"
        ) as sub:
            async for raw_message_bytes in sub:
                raw_message = io.BytesIO(raw_message_bytes)
                initial_part_length = int.from_bytes(
                    raw_message.read(4), "big", signed=False
                )
                message = JourneysExternalPushCachePubSubMessage.model_validate_json(
                    raw_message.read(initial_part_length)
                )

                async with Itgs() as itgs:
                    if not message.have_updated:
                        await delete_from_local_cache(itgs, message.uid)
                        continue

                    parsed_journey = parse_cached_with_blank_jwts(raw_message)
                    locally_cacheable = parsed_journey.__pydantic_serializer__.to_json(
                        parsed_journey
                    )
                    await write_to_local_cache(
                        itgs, message.uid, io.BytesIO(locally_cacheable)
                    )
                    to_notify = waiting_for_cache.pop(message.uid, [])
                    for loop, event in to_notify:
                        if loop is running_loop:
                            event.set()
                        else:
                            loop.call_soon_threadsafe(event.set)

    except Exception as e:
        if pps.instance.exit_event.is_set() and isinstance(e, pps.PPSShutdownException):
            return  # type: ignore
        await handle_error(e)
    finally:
        print("read_one_external cache_push_loop exiting")


class JourneysExternalPushCachePubSubMessage(BaseModel):
    uid: str = Field(description="The UID of the journey updated")
    min_checked_at: float = Field(description="When the journey updated")
    have_updated: bool = Field(
        description="True if this message is followed by the updated journey data, False if it is not"
    )
