import secrets
from typing import Optional, cast
from error_middleware import handle_contextless_error
from itgs import Itgs


async def get_or_create_latest_revenue_cat_id(
    itgs: Itgs, *, user_sub: str, now: float
) -> Optional[str]:
    """Attempts to fetch the revenue cat id from the database for the user with
    the given sub; if none exists, a new one is created and fetched (to ensure it
    exists) before being returned.

    If the user with the given sub does not exist, returns None.

    Args:
        itgs (Itgs): The itgs instance.
        user_sub (str): The user's sub.
        now (float): canonical current time for the request

    Returns:
        Optional[str]: The revenue cat id, or None if the user does not exist.
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")
    response = await cursor.execute(
        """
            SELECT
                user_revenue_cat_ids.revenue_cat_id
            FROM users
            LEFT OUTER JOIN user_revenue_cat_ids ON user_revenue_cat_ids.user_id = users.id
            WHERE users.sub = ?
            ORDER BY user_revenue_cat_ids.created_at DESC, user_revenue_cat_ids.uid ASC
            LIMIT 1
            """,
        (user_sub,),
    )
    if not response.results:
        return None

    latest_revenue_cat_id = cast(Optional[str], response.results[0][0])
    if latest_revenue_cat_id is not None:
        return latest_revenue_cat_id

    new_revenue_cat_uid = f"oseh_iurc_{secrets.token_urlsafe(16)}"
    new_revenue_cat_id = f"oseh_u_rc_{secrets.token_urlsafe(16)}"
    rc = await itgs.revenue_cat()
    await rc.get_customer_info(revenue_cat_id=new_revenue_cat_id)
    response = await cursor.execute(
        """
        INSERT INTO user_revenue_cat_ids (
            uid, user_id, revenue_cat_id, revenue_cat_attributes, created_at, checked_at
        )
        SELECT
            ?, users.id, ?, '{}', ?, ?
        FROM users
        WHERE
            users.sub = ?
            AND NOT EXISTS (
                SELECT 1 FROM user_revenue_cat_ids AS urci
                WHERE urci.user_id = users.id
            )
        """,
        (
            new_revenue_cat_uid,
            new_revenue_cat_id,
            now,
            now,
            user_sub,
        ),
    )
    if response.rows_affected != 1:
        await handle_contextless_error(
            extra_info=f"for user `{user_sub}` raced creating revenue cat id"
        )
        return None
    return new_revenue_cat_id
