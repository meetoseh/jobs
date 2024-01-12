"""Checks open stripe sessions which haven't been checked in a while to see if they
are complete
"""
import json
import secrets
from typing import Optional
from error_middleware import handle_warning
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future as ConcFuture
import stripe
import os
import asyncio
from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Sweeps the open stripe checkout sessions table for sessions which are due
    to be checked and checks them.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    now = time.time()
    with ThreadPoolExecutor(max_workers=1) as executor:
        while True:
            response = await cursor.execute(
                """
                SELECT
                    oscs.uid,
                    oscs.stripe_checkout_session_id,
                    users.sub,
                    user_revenue_cat_ids.revenue_cat_id,
                    oscs.created_at,
                    oscs.expires_at
                FROM open_stripe_checkout_sessions AS oscs
                JOIN users ON users.id = oscs.user_id
                LEFT OUTER JOIN user_revenue_cat_ids ON (
                    user_revenue_cat_ids.user_id = users.id
                    AND NOT EXISTS (
                        SELECT 1 FROM user_revenue_cat_ids AS urcids
                        WHERE
                            urcids.user_id = users.id
                            AND (
                                urcids.created_at > user_revenue_cat_ids.created_at
                                OR (urcids.created_at = user_revenue_cat_ids.created_at AND urcids.uid < user_revenue_cat_ids.uid)
                            )
                    )
                )
                WHERE
                    (
                        oscs.last_checked_at - oscs.created_at < 300
                        AND ? - oscs.created_at >= 300
                    )
                    OR (
                        oscs.last_checked_at - oscs.created_at < 3600
                        AND ? - oscs.created_at >= 3600
                    )
                    OR (
                        oscs.expires_at < ?
                        AND ? - oscs.last_checked_at >= 300
                    )
                LIMIT 100
                """,
                (now, now, now, now),
            )

            if not response.results:
                break

            for row in response.results:
                if gd.received_term_signal:
                    return

                oscs_uid: str = row[0]
                stripe_checkout_session_id: str = row[1]
                user_sub: str = row[2]
                revenue_cat_id: Optional[str] = row[3]
                created_at: float = row[4]
                expires_at: float = row[5]

                logging.debug(
                    f"checking {stripe_checkout_session_id=} for {user_sub=}, {oscs_uid=}; {created_at=}, {expires_at=}"
                )
                session = await get_session_in_executor(
                    executor, stripe_checkout_session_id
                )

                if session.status == "expired":
                    logging.debug(f"{stripe_checkout_session_id=} is expired")
                    await cursor.execute(
                        "DELETE FROM open_stripe_checkout_sessions WHERE uid=?",
                        (oscs_uid,),
                    )
                    continue

                if session.status != "complete":
                    logging.debug(
                        f"{stripe_checkout_session_id=} is not complete: {session.status=}"
                    )
                    await cursor.execute(
                        """
                        UPDATE open_stripe_checkout_sessions
                        SET last_checked_at = ?
                        WHERE uid = ?
                        """,
                        (now, oscs_uid),
                    )
                    continue

                logging.debug(
                    f"{stripe_checkout_session_id=} is complete, sending to revenue cat"
                )

                if revenue_cat_id is None:
                    logging.debug(f"{user_sub=} has no revenue cat id, creating one")
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
                        await handle_warning(
                            f"{__name__}:revenue_cat_raced",
                            f"Failed to get or initialize latest revenue cat id for {user_sub=}, will handle on next sweep",
                        )
                        return

                    revenue_cat_id = new_revenue_cat_id

                rc = await itgs.revenue_cat()
                await rc.create_stripe_purchase(
                    revenue_cat_id=revenue_cat_id,
                    stripe_checkout_session_id=stripe_checkout_session_id,
                )
                logging.debug(
                    f"sent {stripe_checkout_session_id=} to revenue cat, deleting from open_stripe_checkout_sessions"
                )
                await cursor.execute(
                    "DELETE FROM open_stripe_checkout_sessions WHERE uid = ?",
                    (oscs_uid,),
                )
                logging.debug(f"purging entitlements info for {user_sub=}")

                redis = await itgs.redis()
                await redis.delete(f"entitlements:{user_sub}")
                await redis.publish(
                    "ps:entitlements:purge",
                    json.dumps(
                        {"user_sub": user_sub, "min_checked_at": time.time()}
                    ).encode("utf-8"),
                )

                logging.debug(f"done for {stripe_checkout_session_id=}, {user_sub=}")


async def get_session_in_executor(
    executor: ThreadPoolExecutor, stripe_checkout_session_id: str
) -> stripe.checkout.Session:
    conc_future = executor.submit(
        stripe.checkout.Session.retrieve,
        stripe_checkout_session_id,
        api_key=os.environ["OSEH_STRIPE_SECRET_KEY"],
    )
    loop = asyncio.get_running_loop()
    async_future = loop.create_future()

    def callback(fut: ConcFuture):
        if fut.exception():
            loop.call_soon_threadsafe(async_future.set_exception, fut.exception())
        else:
            loop.call_soon_threadsafe(async_future.set_result, fut.result())

    conc_future.add_done_callback(callback)
    return await async_future


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.revenue_cat.sweep_open_stripe_checkout_sessions"
            )

    asyncio.run(main())
