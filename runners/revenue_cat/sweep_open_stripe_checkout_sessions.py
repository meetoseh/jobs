"""Checks open stripe sessions which haven't been checked in a while to see if they
are complete
"""
import json
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future as ConcFuture
import stripe
import os
import asyncio


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
                    users.revenue_cat_id,
                    oscs.created_at,
                    oscs.expires_at
                FROM open_stripe_checkout_sessions AS oscs
                JOIN users ON users.id = oscs.user_id
                WHERE
                    (
                        oscs.last_checked_at - oscs.created_at < 300
                        AND ? - oscs.created_at >= 300
                    )
                    OR (
                        oscs.last_checked_at - oscs.created_at < 3600
                        AND ? - oscs.created_at >= 3600
                    )
                    OR oscs.expires_at < ?
                LIMIT 100
                """,
                (now, now, now),
            )

            if not response.results:
                break

            for row in response.results:
                oscs_uid: str = row[0]
                stripe_checkout_session_id: str = row[1]
                user_sub: str = row[2]
                revenue_cat_id: str = row[3]
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
