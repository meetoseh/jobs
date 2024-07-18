from itgs import Itgs


async def publish_journal_chat_event(
    itgs: Itgs, /, *, journal_chat_uid: str, event: bytes
) -> None:
    """Pushes the event serialized as if by serialize_journal_chat_event to the
    event list of the given journal chat job, ensuring that the event list still
    has an expiration set and also alerting websocket workers that may be listening
    for new events
    """
    event_list_key = f"journal_chats:{journal_chat_uid}:events".encode("utf-8")
    redis = await itgs.redis()
    async with redis.pipeline() as pipe:
        pipe.multi()
        await pipe.rpush(event_list_key, event)  # type: ignore
        await pipe.expire(event_list_key, 60 * 60)  # type: ignore
        await pipe.publish(b"ps:" + event_list_key, event)  # type: ignore
        await pipe.execute()
