import json
import secrets
from typing import Optional, cast
from itgs import Itgs
import logging as logger
import stripe
import os
import time
from lib.users.revenue_cat import get_or_create_latest_revenue_cat_id


async def sync_user_stripe_revenue_cat(itgs: Itgs, *, user_sub: str) -> None:
    """Fetches all the stripe customers associated with a user and posts them
    to the latest revenue cat id for that user, which should ensure they are
    both known about and transferred correctly. Then purges any relevant
    caches for the user.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str): the sub of the user to update
    """
    request_id = secrets.token_urlsafe(6)
    logger.debug(
        f"Syncing user {user_sub} stripe<->revenue cat; assigned {request_id=} for logging"
    )

    conn = await itgs.conn()
    cursor = conn.cursor("weak")

    logger.debug(f"{request_id=} fetching stripe customers...")
    response = await cursor.execute(
        "SELECT"
        " stripe_customers.stripe_customer_id "
        "FROM users, stripe_customers "
        "WHERE"
        " users.sub = ? AND stripe_customers.user_id = users.id",
        (user_sub,),
    )
    if not response.results:
        logger.debug(f"{request_id=} no stripe customers found")
        return

    stripe_customer_ids = [cast(str, row[0]) for row in response.results]
    logger.debug(f"{request_id=} found {len(stripe_customer_ids)} stripe customers...")

    stripe_sk = os.environ["OSEH_STRIPE_SECRET_KEY"]
    customers = [
        stripe.Customer.retrieve(
            stripe_customer_id,
            api_key=stripe_sk,
            expand=["subscriptions"],
        )
        for stripe_customer_id in stripe_customer_ids
    ]
    logger.debug(f"{request_id=} fetched stripe customers...")

    now = time.time()
    livemode_expected = os.environ["ENVIRONMENT"] != "dev"
    revenue_cat_id: Optional[str] = None
    need_purge_caches: bool = False
    for customer in customers:
        if not customer.subscriptions:
            logger.debug(f"{request_id=} skipping {customer.id=}: no subscriptions")
            continue

        subs = customer.subscriptions
        while True:
            for subscription in subs.data:
                if subscription.livemode is not livemode_expected:
                    logger.warning(
                        f"{request_id=} {subscription.id=} has unexpected livemode {subscription.livemode=}, skipping"
                    )
                    continue

                if subscription.ended_at is not None and subscription.ended_at < now:
                    logger.debug(
                        f"{request_id=} {subscription.id=} has ended, skipping"
                    )
                    continue

                logger.debug(
                    f"{request_id=} want to post subscription {subscription.id} to revenuecat..."
                )
                if revenue_cat_id is None:
                    logger.debug(f"{request_id=} fetching latest revenue cat id...")
                    revenue_cat_id = await get_or_create_latest_revenue_cat_id(
                        itgs, user_sub=user_sub, now=now
                    )
                    if revenue_cat_id is None:
                        logger.warning(
                            f"{request_id=} failed to fetch latest revenue cat id"
                        )
                        raise ValueError(
                            "Failed to fetch latest revenue cat id (user deleted)"
                        )

                logger.info(
                    f"{request_id=} syncing {subscription.id=} to {revenue_cat_id=} for {user_sub=}.."
                )
                rc = await itgs.revenue_cat()
                await rc.create_stripe_purchase(
                    revenue_cat_id=revenue_cat_id,
                    stripe_checkout_session_id=subscription.id,
                    is_restore=True,
                )
                need_purge_caches = True

            if not subs.has_more:
                break

            subs = subs.next_page(api_key=stripe_sk)

    if need_purge_caches:
        logger.debug(
            f"{request_id=} purging caches for {user_sub=} and {revenue_cat_id=}..."
        )
        redis = await itgs.redis()
        await redis.delete(f"entitlements:{user_sub}")
        await redis.publish(
            b"ps:entitlements:purge",
            json.dumps(
                {
                    "user_sub": user_sub,
                    "min_checked_at": time.time(),
                }
            ),
        )
