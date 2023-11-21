"""Ensures a users revenue cat customer actually exists"""
import json
from typing import Dict, Optional
from error_middleware import handle_warning
from pypika import Table, Query, Parameter
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import os
from jobs import JobCategory
import time
import datetime
from lib.contact_methods.user_primary_email import primary_email_join_clause
from lib.shared.describe_user import enqueue_send_described_user_slack_message

category = JobCategory.LOW_RESOURCE_COST

GIVING_BETA_ACCESS = os.environ.get("ENVIRONMENT") != "dev"


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
    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    users = Table("users")
    user_email_addresses = Table("user_email_addresses")
    response = await cursor.execute(
        Query.from_(users)
        .select(
            user_email_addresses.email,
            users.given_name,
            users.family_name,
            users.revenue_cat_id,
        )
        .left_outer_join(user_email_addresses)
        .on(primary_email_join_clause())
        .where(users.sub == Parameter("?"))
        .get_sql(),
        (user_sub,),
    )

    if not response.results:
        await handle_warning(
            f"{__name__}.not_found", f"User with sub {user_sub=} not found"
        )
        return

    email: Optional[str] = response.results[0][0]
    given_name: str = response.results[0][1]
    family_name: str = response.results[0][2]
    revenue_cat_id: str = response.results[0][3]

    name = f"{given_name} {family_name}".strip()

    expected_attributes = {
        "$displayName": name,
        "environment": os.environ["ENVIRONMENT"],
    }
    if email is not None:
        expected_attributes["$email"] = email
    else:
        await handle_warning(
            f"{__name__}:no_email",
            f"User with sub `{user_sub}` has no email address, not setting $email attribute",
        )

    rcat = await itgs.revenue_cat()

    # this ensures the user exists in revenue cat
    customer_info = await rcat.get_customer_info(revenue_cat_id=revenue_cat_id)

    curr_attrs = customer_info.subscriber.subscriber_attributes
    to_update: Dict[str, str] = dict()
    for key, exp_val in expected_attributes.items():
        if key not in curr_attrs or curr_attrs[key].value != exp_val:
            to_update[key] = exp_val

    if to_update:
        await rcat.set_customer_attributes(
            revenue_cat_id=revenue_cat_id, attributes=to_update
        )

        logging.debug(f"Updated {revenue_cat_id=} with {to_update=}")

    dnow = datetime.datetime.fromtimestamp(time.time(), tz=datetime.timezone.utc)
    pro = customer_info.subscriber.entitlements.get("pro")
    if (
        pro is None or (pro.expires_date is not None and pro.expires_date < dnow)
    ) and GIVING_BETA_ACCESS:
        # 1 month no-credit-card trial for now
        logging.debug(
            f"Granting 1 month of Oseh+ to {name} ({email=}, {revenue_cat_id=}; previous had? {pro is not None and pro.expires_date is not None})"
        )
        await rcat.grant_promotional_entitlement(
            revenue_cat_id=revenue_cat_id,
            entitlement_identifier="pro",
            duration="monthly",
        )

        now = time.time()
        redis = await itgs.redis()
        await redis.delete(f"entitlements:{user_sub}".encode("utf-8"))
        await redis.publish(
            b"ps:entitlements:purge",
            json.dumps({"user_sub": user_sub, "min_checked_at": now}).encode("utf-8"),
        )

        logging.info(
            f"Granted 1 month of Oseh+ to {name} ({email=}, {revenue_cat_id=})"
        )

        await enqueue_send_described_user_slack_message(
            itgs,
            message=f"{{name}} was granted 1 month of Oseh+",
            sub=user_sub,
            channel="oseh_bot",
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
