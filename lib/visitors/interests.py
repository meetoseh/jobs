import json
import secrets
import time
from typing import List
from error_middleware import handle_contextless_error
from itgs import Itgs
import logging
from dataclasses import dataclass


@dataclass
class _VisInterest:
    uid: str
    slug: str
    add_reason: str
    is_primary: bool
    created_at: float


async def copy_interests_from_visitor_to_user(
    itgs: Itgs, visitor_uid: str, user_sub: str
) -> None:
    """If there are any interests for the given visitor which
    are more recent than all active interests for the given user,
    then the users interests are replaced with the interests from
    the visitor.

    This operation generally only makes sense when the visitor and
    user are first related, to handle the standard signup flow:

    - Person clicks on an ad with directed copy (e.g., sleep copy)
    - A visitor is created with the person and associated with an interest (e.g., sleep)
    - The person signs up, which creates a user for the person
    - The user is associated with the visitor, and now we want to copy the interest
      from the visitor to the user

    This operation is not concurrency safe / idempotent. However, when called as
    described above weaving won't occur as we've already determined we "won"
    the competition to associate the user with the visitor.

    Args:
        itgs (Itgs): the integrations to (re)use
        visitor_uid (str): the visitor which was presumably just associated
            with the user
        user_sub (str): the user which was presumably just associated
            with the visitor
    """
    logging.info(f"Copying interests from {visitor_uid=} to {user_sub=}")
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            visitor_interests.uid,
            interests.slug,
            visitor_interests.add_reason,
            visitor_interests.is_primary,
            visitor_interests.created_at
        FROM visitor_interests, visitors, interests
        WHERE
            visitor_interests.visitor_id = visitors.id
            AND visitor_interests.interest_id = interests.id
            AND visitors.uid = ?
        """,
        (visitor_uid,),
    )

    if not response.results:
        logging.info(f"There are no interests to copy from {visitor_uid=}")
        return

    interests: List[_VisInterest] = [
        _VisInterest(
            uid=uid,
            slug=slug,
            add_reason=add_reason,
            is_primary=bool(is_primary),
            created_at=created_at,
        )
        for uid, slug, add_reason, is_primary, created_at in response.results
    ]

    logging.debug(
        f"Found {len(interests)} interests to potentially copy from {visitor_uid=} to {user_sub=}: {interests}"
    )
    latest_visitor_interest_created_at = max(
        interest.created_at for interest in interests
    )

    new_interests_qmark_list = ",".join(["(?,?,?,?)"] * len(interests))
    new_interests_values = [
        v
        for interest in interests
        for v in (
            interest.slug,
            interest.uid,
            interest.is_primary,
            f"oseh_uint_{secrets.token_urlsafe(16)}",
        )
    ]

    swapped_at = time.time()
    response = await cursor.executemany3(
        (
            (
                """
                UPDATE user_interests
                SET
                    deleted_reason=?, deleted_at=?
                WHERE
                    EXISTS (
                        SELECT 1 FROM users
                        WHERE
                            users.id = user_interests.user_id
                            AND users.sub = ?
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM user_interests AS ui, users
                        WHERE
                            ui.user_id = users.id
                            AND users.sub = ?
                            AND ui.created_at >= ?
                            AND ui.deleted_at IS NULL
                    )
                    AND user_interests.deleted_at IS NULL
                """,
                (
                    json.dumps({"type": "replaced"}),
                    swapped_at,
                    user_sub,
                    user_sub,
                    latest_visitor_interest_created_at,
                ),
            ),
            (
                f"""
                WITH new_interest_slugs (slug, vint_uid, is_primary, uint_uid) AS (
                    VALUES {new_interests_qmark_list}
                )
                INSERT INTO user_interests (
                    uid, user_id, interest_id, is_primary, add_reason, created_at
                )
                SELECT
                    new_interest_slugs.uint_uid,
                    users.id,
                    interests.id,
                    new_interest_slugs.is_primary,
                    json_insert(?, '$.visitor_interest_uid', new_interest_slugs.vint_uid),
                    ?
                FROM new_interest_slugs, users, interests
                WHERE
                    users.sub = ?
                    AND interests.slug = new_interest_slugs.slug
                    AND (
                        EXISTS (
                            SELECT 1 FROM user_interests AS ui, new_interest_slugs AS nis
                            WHERE ui.uid = nis.uint_uid
                        )
                        OR NOT EXISTS (
                            SELECT 1 FROM user_interests AS ui
                            WHERE 
                                ui.user_id = users.id
                                AND ui.deleted_at IS NULL
                        )
                    )
                """,
                (
                    *new_interests_values,
                    json.dumps({"type": "copy_visitor"}),
                    swapped_at,
                    user_sub,
                ),
            ),
        )
    )

    if response[0].rows_affected is not None and response[0].rows_affected > 0:
        logging.info(
            f"Deleted {response[0].rows_affected} user_interests for {user_sub=} which were replaced by interests from {visitor_uid=}"
        )

    if response[1].rows_affected is not None and response[1].rows_affected > 0:
        if response[1].rows_affected != len(interests):
            await handle_contextless_error(
                extra_info=f"Expected to insert {len(interests)} user_interests for {user_sub=} but inserted {response[1].rows_affected}"
            )
        logging.info(
            f"Inserted {response[1].rows_affected} user_interests for {user_sub=} from {visitor_uid=}"
        )
    else:
        if response[0].rows_affected is not None and response[0].rows_affected > 0:
            await handle_contextless_error(
                extra_info=f"Deleted {response[0].rows_affected} user_interests for {user_sub=} but inserted no user_interests for {user_sub=} from {visitor_uid=}"
            )

        logging.info(f"Inserted no user_interests for {user_sub=} from {visitor_uid=}")
