"""Ensures a users revenue cat customer actually exists"""
from typing import Dict
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import os


async def execute(itgs: Itgs, gd: GracefulDeath, *, user_sub: str):
    """Ensures the given user exists in RevenueCat and sets their attributes,
    to ease the burden of customer support.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): the sub of the user to ensure is synced with revenue cat
    """
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    response = await cursor.execute(
        """
        SELECT
            users.email,
            users.given_name,
            users.family_name,
            users.revenue_cat_id
        FROM users
        WHERE users.sub = ?
        """,
        (user_sub,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}.not_found", f"User with sub {user_sub=} not found"
        )
        return

    email: str = response.results[0][0]
    given_name: str = response.results[0][1]
    family_name: str = response.results[0][2]
    revenue_cat_id: str = response.results[0][3]

    name = f"{given_name} {family_name}".strip()

    expected_attributes = {
        "$displayName": name,
        "$email": email,
        "environment": os.environ["ENVIRONMENT"],
    }

    rcat = await itgs.revenue_cat()

    # this ensures the user exists in revenue cat
    customer_info = await rcat.get_customer_info(revenue_cat_id=revenue_cat_id)

    curr_attrs = customer_info.subscriber.subscriber_attributes
    to_update: Dict[str, str] = dict()
    for key, exp_val in expected_attributes.items():
        if key not in curr_attrs or curr_attrs[key].value != exp_val:
            to_update[key] = exp_val

    if not to_update:
        return

    await rcat.set_customer_attributes(
        revenue_cat_id=revenue_cat_id, attributes=to_update
    )
    logging.debug(f"Updated {revenue_cat_id=} with {to_update=}")
