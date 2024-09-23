"""Ensures a users revenue cat customer actually exists"""

import json
from typing import Any, Dict, Optional, cast
from error_middleware import handle_warning
from pypika import Table, Query, Parameter, Not
from pypika.terms import ExistsCriterion
from lib.db.utils import ShieldFields
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import os
from jobs import JobCategory
import time
from lib.contact_methods.user_primary_email import primary_email_join_clause
from lib.users.revenue_cat import get_or_create_latest_revenue_cat_id
import aiohttp.client_exceptions

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, user_sub: str):
    """Ensures the given user exists in RevenueCat and sets their attributes,
    to ease the burden of customer support. Furthermore, if we are in a special
    period (e.g., the beta test), this may also be used to grant an initial
    entitlement to the user.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): the sub of the user to ensure is synced with revenue cat
    """
    executed_at = time.time()
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    users = Table("users")
    user_email_addresses = Table("user_email_addresses")
    user_revenue_cat_ids = Table("user_revenue_cat_ids")
    user_revenue_cat_ids_inner = user_revenue_cat_ids.as_("urci")
    response = await cursor.execute(
        Query.from_(users)
        .select(
            user_email_addresses.email,
            users.given_name,
            users.family_name,
            user_revenue_cat_ids.revenue_cat_id,
            user_revenue_cat_ids.revenue_cat_attributes,
        )
        .left_outer_join(user_email_addresses)
        .on(primary_email_join_clause())
        .left_outer_join(user_revenue_cat_ids)
        .on(
            (user_revenue_cat_ids.user_id == users.id)
            & (
                ShieldFields(
                    Not(
                        ExistsCriterion(
                            Query.from_(user_revenue_cat_ids_inner)
                            .select(1)
                            .where(
                                (
                                    user_revenue_cat_ids_inner.user_id
                                    == users.id
                                    & (
                                        (
                                            user_revenue_cat_ids_inner.created_at
                                            > user_revenue_cat_ids.created_at
                                        )
                                        | (
                                            user_revenue_cat_ids_inner.created_at
                                            == user_revenue_cat_ids.created_at
                                            & user_revenue_cat_ids_inner.uid
                                            < user_revenue_cat_ids.uid
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
        .where(users.sub == Parameter("?"))
        .get_sql(),
        (user_sub,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}.not_found", f"User with sub {user_sub=} not found"
        )
        return

    email = cast(Optional[str], response.results[0][0])
    given_name = cast(str, response.results[0][1])
    family_name = cast(str, response.results[0][2])
    revenue_cat_id = cast(Optional[str], response.results[0][3])
    revenue_cat_id_attributes_raw = cast(Optional[str], response.results[0][4])

    if revenue_cat_id is None:
        revenue_cat_id = await get_or_create_latest_revenue_cat_id(
            itgs, user_sub=user_sub, now=executed_at
        )
        assert revenue_cat_id is not None, response
        revenue_cat_id_attributes_raw = "{}"
    else:
        assert revenue_cat_id_attributes_raw is not None, response

    revenue_cat_id_attributes = json.loads(revenue_cat_id_attributes_raw)

    name = f"{given_name} {family_name}".strip()

    expected_attributes = {
        "$displayName": name,
        "environment": os.environ["ENVIRONMENT"],
    }
    if email is not None:
        expected_attributes["$email"] = email

    rcat = await itgs.revenue_cat()

    to_update: Dict[str, str] = dict()
    new_attributes: Dict[str, Dict[str, Any]] = dict(revenue_cat_id_attributes)
    for key, exp_val in expected_attributes.items():
        if (
            key not in revenue_cat_id_attributes_raw
            or revenue_cat_id_attributes[key]["value"] != exp_val
        ):
            to_update[key] = exp_val
            new_attributes[key] = {
                "value": exp_val,
                "updated_at_ms": executed_at * 1000,
            }

    if to_update:
        try:
            await rcat.set_customer_attributes(
                revenue_cat_id=revenue_cat_id, attributes=to_update
            )
        except aiohttp.client_exceptions.ClientResponseError as exc:
            if exc.status == 404:
                logging.info("Revenue cat user appears uninitialized, initializing")
                await rcat.get_customer_info(revenue_cat_id=revenue_cat_id)
                await rcat.set_customer_attributes(
                    revenue_cat_id=revenue_cat_id, attributes=to_update
                )

        logging.debug(
            f"Updated {revenue_cat_id=} with {to_update=}, now {new_attributes=}"
        )
        await cursor.execute(
            "UPDATE user_revenue_cat_ids SET revenue_cat_attributes = ? WHERE revenue_cat_id = ?",
            (json.dumps(new_attributes), revenue_cat_id),
        )


if __name__ == "__main__":
    import asyncio

    async def main():
        user_sub = input("User sub: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.revenue_cat.ensure_user",
                user_sub=user_sub,
            )

    asyncio.run(main())
