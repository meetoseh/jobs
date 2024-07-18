from lib.journals.journal_chat_redis_packet import JournalChatRedisPacket
from lib.journals.master_keys import GetJournalMasterKeyForEncryptionResultSuccess


def serialize_journal_chat_event(
    *,
    journal_master_key: GetJournalMasterKeyForEncryptionResultSuccess,
    event: JournalChatRedisPacket,
    now: float
) -> bytes:
    """Serializes the journal chat event that can go into either
    `journal_chats:{uid}:events` or `ps:journal_chats:{uid}:events`
    to the form that is stored in Redis.
    """
    encrypted_event = journal_master_key.journal_master_key.encrypt_at_time(
        event.__pydantic_serializer__.to_json(event), int(now)
    )

    key_uid_bytes = journal_master_key.journal_master_key_uid.encode("utf-8")
    return (
        len(key_uid_bytes).to_bytes(4, "big")
        + key_uid_bytes
        + len(encrypted_event).to_bytes(8, "big")
        + encrypted_event
    )
