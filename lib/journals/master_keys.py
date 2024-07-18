"""Module for working with journal master keys"""

import io
import secrets
from itgs import Itgs
from typing import Literal, Union, cast
from dataclasses import dataclass
import cryptography.fernet


@dataclass
class GetJournalMasterKeyForEncryptionResultSuccess:
    type: Literal["success"]
    """
    - `success`: We found a journal master key and were able to download it or
      we created the key and uploaded it to s3
    """
    fresh: bool
    """True if we created the key, false if we just downloaded it"""
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we fetched"""
    journal_master_key: cryptography.fernet.Fernet
    """The journal master key we fetched"""
    journal_master_key_data: bytes
    """The raw journal client key data"""


@dataclass
class GetJournalMasterKeyForEncryptionResultS3Error:
    type: Literal["s3_error"]
    """
    - `s3_error`: We found a journal master key, but there was an error fetching 
      the key from s3
    """
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we fetched"""
    exc: Exception
    """The exception that occurred while fetching the journal master key"""


@dataclass
class GetJournalMasterKeyForEncryptionResultLost:
    type: Literal["lost"]
    """
    - `lost`: We know about the journal master key, but there is no file at the indicated
      key in s3
    """
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we fetched"""


@dataclass
class GetJournalMasterKeyForEncryptionResultParseError:
    type: Literal["parse_error"]
    """
    - `parse_error`: There was an error parsing the journal master key
    """
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we fetched"""
    exc: Exception
    """The exception that occurred while parsing the journal master key"""


GetJournalMasterKeyForEncryptionResult = Union[
    GetJournalMasterKeyForEncryptionResultSuccess,
    GetJournalMasterKeyForEncryptionResultS3Error,
    GetJournalMasterKeyForEncryptionResultParseError,
    GetJournalMasterKeyForEncryptionResultLost,
]


async def get_journal_master_key_for_encryption(
    itgs: Itgs,
    /,
    *,
    user_sub: str,
    now: float,
) -> GetJournalMasterKeyForEncryptionResult:
    """Fetches a journal master key for encrypting journal entries for the user with
    the given sub. We support having multiple such keys in use simultaneously in order
    to facilitate key rotation.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str): the sub of the user for whom we want a journal master key
        now (float): the current time in seconds since the epoch
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    minimum_key_created_at = now - 60 * 60 * 24 * 30
    response = await cursor.execute(
        """
SELECT
    user_journal_master_keys.uid,
    s3_files.key
FROM user_journal_master_keys, s3_files
WHERE
    user_journal_master_keys.user_id = (SELECT users.id FROM users WHERE users.sub = ?)
    AND s3_files.id = user_journal_master_keys.s3_file_id
    AND user_journal_master_keys.created_at > ?
ORDER BY user_journal_master_keys.created_at DESC
LIMIT 1
        """,
        (user_sub, minimum_key_created_at),
    )

    if response.results:
        master_key_uid = cast(str, response.results[0][0])
        s3_key = cast(str, response.results[0][1])
        result = await get_journal_master_key_from_s3(
            itgs,
            user_journal_master_key_uid=master_key_uid,
            user_sub=user_sub,
            s3_key=s3_key,
        )
        if result.type != "lost":
            return result

    new_key_data = cryptography.fernet.Fernet.generate_key()
    new_key_uid = f"oseh_ujmk_{secrets.token_urlsafe(16)}"
    new_s3_key = f"s3_files/journals/keys/master/{new_key_uid}"
    new_s3_uid = f"oseh_s3f_{secrets.token_urlsafe(16)}"

    files = await itgs.files()
    try:
        await files.upload(
            io.BytesIO(new_key_data),
            bucket=files.default_bucket,
            key=new_s3_key,
            sync=True,
        )
    except Exception as e:
        return GetJournalMasterKeyForEncryptionResultS3Error(
            type="s3_error",
            user_sub=user_sub,
            journal_master_key_uid=new_key_uid,
            exc=e,
        )

    response = await cursor.executeunified3(
        (
            (
                """
SELECT
    user_journal_master_keys.uid,
    s3_files.key
FROM user_journal_master_keys, s3_files
WHERE
    user_journal_master_keys.user_id = (SELECT users.id FROM users WHERE users.sub = ?)
    AND s3_files.id = user_journal_master_keys.s3_file_id
    AND user_journal_master_keys.created_at > ?
ORDER BY user_journal_master_keys.created_at DESC
LIMIT 1
                """,
                (user_sub, minimum_key_created_at),
            ),
            (
                """
INSERT INTO s3_files (
    uid, key, file_size, content_type, created_at
)
SELECT
    ?, ?, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM users, user_journal_master_keys
    WHERE
        users.sub = ?
        AND users.id = user_journal_master_keys.user_id
        AND user_journal_master_keys.created_at > ?
)
                """,
                (
                    new_s3_uid,
                    new_s3_key,
                    len(new_key_data),
                    "application/octet-stream",
                    now,
                    user_sub,
                    minimum_key_created_at,
                ),
            ),
            (
                """
INSERT INTO user_journal_master_keys (
    uid, user_id, s3_file_id, created_at
)
SELECT
    ?, users.id, s3_files.id, ?
FROM users, s3_files
WHERE
    users.sub = ?
    AND s3_files.uid = ?
    AND NOT EXISTS (
        SELECT 1 FROM user_journal_master_keys AS ujmk
        WHERE
            ujmk.user_id = (SELECT u.id FROM users AS u WHERE u.sub = ?)
            AND ujmk.created_at > ?
    )
                """,
                (
                    new_key_uid,
                    now,
                    user_sub,
                    new_s3_uid,
                    user_sub,
                    minimum_key_created_at,
                ),
            ),
        )
    )

    if response[2].rows_affected is None or response[2].rows_affected < 1:
        assert (
            response[1].rows_affected is None or response[1].rows_affected < 1
        ), response
        await files.delete(bucket=files.default_bucket, key=new_s3_key)

    if response[0].results:
        assert (
            response[2].rows_affected is None or response[2].rows_affected < 1
        ), response
        return await get_journal_master_key_from_s3(
            itgs,
            user_journal_master_key_uid=new_key_uid,
            user_sub=user_sub,
            s3_key=new_s3_key,
        )

    assert (
        response[1].rows_affected is not None and response[1].rows_affected > 0
    ), response
    return GetJournalMasterKeyForEncryptionResultSuccess(
        type="success",
        fresh=True,
        user_sub=user_sub,
        journal_master_key_uid=new_key_uid,
        journal_master_key=cryptography.fernet.Fernet(new_key_data),
        journal_master_key_data=new_key_data,
    )


async def get_journal_master_key_from_s3(
    itgs: Itgs,
    /,
    *,
    user_journal_master_key_uid: str,
    user_sub: str,
    s3_key: str,
) -> Union[
    GetJournalMasterKeyForEncryptionResultSuccess,
    GetJournalMasterKeyForEncryptionResultS3Error,
    GetJournalMasterKeyForEncryptionResultParseError,
    GetJournalMasterKeyForEncryptionResultLost,
]:
    """Fetches the journal master key with the given uid from s3

    Args:
        itgs (Itgs): the integrations to (re)use
        user_journal_master_key_uid (str): the uid of the journal master key that we know is
            stored in the s3 file at the given key
        user_sub (str): the sub of the user that owns the master key at the given s3 file
        s3_key (str): the key in s3 where the journal master key is stored
    """
    files = await itgs.files()
    key_reader = io.BytesIO()
    try:
        found = await files.download(
            key_reader, bucket=files.default_bucket, key=s3_key, sync=True
        )
    except Exception as e:
        return GetJournalMasterKeyForEncryptionResultS3Error(
            type="s3_error",
            user_sub=user_sub,
            journal_master_key_uid=user_journal_master_key_uid,
            exc=e,
        )

    if not found:
        return GetJournalMasterKeyForEncryptionResultLost(
            type="lost",
            user_sub=user_sub,
            journal_master_key_uid=user_journal_master_key_uid,
        )

    key_data = key_reader.getvalue()
    try:
        key = cryptography.fernet.Fernet(key_data)
    except Exception as e:
        return GetJournalMasterKeyForEncryptionResultParseError(
            type="parse_error",
            user_sub=user_sub,
            journal_master_key_uid=user_journal_master_key_uid,
            exc=e,
        )

    return GetJournalMasterKeyForEncryptionResultSuccess(
        type="success",
        fresh=False,
        user_sub=user_sub,
        journal_master_key_uid=user_journal_master_key_uid,
        journal_master_key=key,
        journal_master_key_data=key_data,
    )


@dataclass
class GetJournalMasterKeyForDecryptionResultSuccess:
    type: Literal["success"]
    """
    - `success`: We found the journal master key and were able to download it
    """
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we fetched"""
    journal_master_key: cryptography.fernet.Fernet
    """The journal master key we fetched"""
    journal_master_key_data: bytes
    """The raw journal client key data"""


@dataclass
class GetJournalMasterKeyForDecryptionResultS3Error:
    type: Literal["s3_error"]
    """
    - `s3_error`: We found the journal master key, but there was an error fetching 
      the key from s3
    """
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we failed to fetch"""
    exc: Exception
    """The exception that occurred while fetching the journal master key"""


@dataclass
class GetJournalMasterKeyForDecryptionResultLost:
    type: Literal["lost"]
    """
    - `lost`: We know about the journal master key, but there is no file at the indicated
      key in s3
    """
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we failed to fetch"""


@dataclass
class GetJournalMasterKeyForDecryptionResultNotFound:
    type: Literal["not_found"]
    """
    - `not_found`: There is no journal master key with that uid for that user
    """
    user_sub: str
    """The sub of the user for whom we wanted the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we wanted"""


@dataclass
class GetJournalMasterKeyForDecryptionResultParseError:
    type: Literal["parse_error"]
    """
    - `parse_error`: There was an error parsing the journal master key
    """
    user_sub: str
    """The sub of the user for whom we fetched the journal master key"""
    journal_master_key_uid: str
    """The uid of the journal master key we failed to parse"""
    exc: Exception
    """The exception that occurred while parsing the journal master key"""


GetJournalMasterKeyForDecryptionResult = Union[
    GetJournalMasterKeyForDecryptionResultSuccess,
    GetJournalMasterKeyForDecryptionResultS3Error,
    GetJournalMasterKeyForDecryptionResultParseError,
    GetJournalMasterKeyForDecryptionResultLost,
    GetJournalMasterKeyForDecryptionResultNotFound,
]


async def get_journal_master_key_for_decryption(
    itgs: Itgs,
    /,
    *,
    user_sub: str,
    journal_master_key_uid: str,
) -> GetJournalMasterKeyForDecryptionResult:
    """Fetches the journal master key with the given uid for decrypting journal
    entries for the user with the given sub.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str): the sub of the user for whom we want a journal master key
        journal_master_key_uid (str): the uid of the journal master key we want
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    response = await cursor.execute(
        """
SELECT
    s3_files.key
FROM user_journal_master_keys, s3_files
WHERE
    user_journal_master_keys.user_id = (SELECT users.id FROM users WHERE users.sub = ?)
    AND user_journal_master_keys.uid = ?
    AND s3_files.id = user_journal_master_keys.s3_file_id
        """,
        (user_sub, journal_master_key_uid),
    )

    if not response.results:
        return GetJournalMasterKeyForDecryptionResultNotFound(
            type="not_found",
            user_sub=user_sub,
            journal_master_key_uid=journal_master_key_uid,
        )

    s3_key = cast(str, response.results[0][0])
    result = await get_journal_master_key_from_s3(
        itgs,
        user_journal_master_key_uid=journal_master_key_uid,
        user_sub=user_sub,
        s3_key=s3_key,
    )

    if result.type == "s3_error":
        return GetJournalMasterKeyForDecryptionResultS3Error(
            type=result.type,
            user_sub=user_sub,
            journal_master_key_uid=journal_master_key_uid,
            exc=result.exc,
        )

    if result.type == "parse_error":
        return GetJournalMasterKeyForDecryptionResultParseError(
            type=result.type,
            user_sub=user_sub,
            journal_master_key_uid=journal_master_key_uid,
            exc=result.exc,
        )

    if result.type == "lost":
        return GetJournalMasterKeyForDecryptionResultLost(
            type=result.type,
            user_sub=user_sub,
            journal_master_key_uid=journal_master_key_uid,
        )

    return GetJournalMasterKeyForDecryptionResultSuccess(
        type=result.type,
        user_sub=user_sub,
        journal_master_key_uid=journal_master_key_uid,
        journal_master_key=result.journal_master_key,
        journal_master_key_data=result.journal_master_key_data,
    )
