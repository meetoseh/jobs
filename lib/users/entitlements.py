"""This module handles caching basic entitlement information on users, with the
ability to purge the cache.

This is a two-stage cache: RevenueCat -> redis -> diskcache
"""

import asyncio
import time
from typing import (
    Awaitable,
    Dict,
    List,
    Literal,
    Optional,
    NoReturn as Never,
    Union,
    cast as typing_cast,
)
import perpetual_pub_sub as pps
from pydantic import BaseModel, Field
from error_middleware import handle_error, handle_warning
from itgs import Itgs
import datetime
import logging as logger
from redis_helpers.zadd_exact_window import zadd_exact_window_safe
from redis_helpers.zcard_exact_window import zcard_exact_window_safe
from lib.users.prices import Period, get_localized_price


class CachedEntitlementRecurrenceLifetime(BaseModel):
    type: Literal["lifetime"] = Field(
        description="Indicates the entitlement lasts forever"
    )


class CachedEntitlementRecurrenceRecurring(BaseModel):
    type: Literal["recurring"] = Field(
        description="Indicates the entitlement ends unless renewed"
    )
    period: Period = Field(description="Time between renewals, normally")
    cycle_ends_at: float = Field(
        description="When the current cycle ends in seconds since the epoch"
    )
    auto_renews: bool = Field(description="If the entitlement will auto-renew")


CachedEntitlementRecurrence = Union[
    CachedEntitlementRecurrenceLifetime, CachedEntitlementRecurrenceRecurring
]

CachedEntitlementPlatform = Literal["stripe", "ios", "google", "promotional"]


class CachedEntitlementActiveInfo(BaseModel):
    recurrence: CachedEntitlementRecurrence = Field(
        description="The recurrence information for the entitlement"
    )
    platform: CachedEntitlementPlatform = Field(
        description="The platform that the entitlement was purchased on"
    )


class CachedEntitlement(BaseModel):
    """Describes information about an entitlement that is cached, either
    in redis or on disk. The identifier of the entitlement is generally
    clear from context.
    """

    is_active: bool = Field(description="If the user has this entitlement")
    active_info: Optional[CachedEntitlementActiveInfo] = Field(
        None,
        description=(
            "If the user has this entitlement, information about the "
            "subscription they have for information purposes only. This "
            "is not intended to be used for determining if the use has the "
            "entitlement, as we do not directly manage subscriptions. For "
            "stripe, for example, the customer portal shows authoritative "
            "info. For ios, the App Store. For google, Google Play."
        ),
    )
    expires_at: Optional[float] = Field(
        description=(
            "if the users entitlement is active, but will expire unless renewed, "
            "the earliest time at which it will expire in seconds since the epoch. This "
            "value may be in the past, but should never be used to determine "
            "whether the entitlement is active - it is only provided for "
            "informational purposes"
        )
    )
    checked_at: float = Field(
        description=(
            "The time that the entitlement was retrieved from the source of truth."
        )
    )


class LocalCachedEntitlements(BaseModel):
    """The format for locally cached entitlements on a given user"""

    entitlements: Dict[str, CachedEntitlement] = Field(
        default_factory=dict, description="The entitlements that are cached locally"
    )


async def get_entitlements_from_source(
    itgs: Itgs, *, user_sub: str, now: float
) -> Optional[LocalCachedEntitlements]:
    """Gets all entitlements that a user has ever had, from the source of
    truth. This information is not particularly slow but depends on revenuecat's
    response times, which we cannot control.

    This does not respect the `revenue_cat_errors` redis key, meaning that it
    will send a request even if we've detected a revenue cat outage, and will
    not report any errors that occur.

    Not generally used directly. Prefer `get_entitlement`

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlements we are getting
        now (float): the time the request is being made

    Returns:
        LocalCachedEntitlements: if the users entitlement information was fetched
            successfully, their entitlements, otherwise None
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    query = (
        "SELECT user_revenue_cat_ids.revenue_cat_id "
        "FROM users, user_revenue_cat_ids "
        "WHERE"
        " users.sub = ?"
        " AND users.id = user_revenue_cat_ids.user_id"
    )
    qargs = (user_sub,)

    response = await cursor.execute(query, qargs)
    if not response.results:
        response = await cursor.execute(query, qargs, read_consistency="strong")
        if not response.results:
            return None

    revenue_cat_ids: List[str] = [row[0] for row in response.results]
    rc = await itgs.revenue_cat()

    unjoined: List[LocalCachedEntitlements] = []
    dnow = datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc)

    for revenue_cat_id in revenue_cat_ids:
        truth = await rc.get_customer_info(
            revenue_cat_id=revenue_cat_id, handle_ratelimits=True
        )
        logger.info(
            f"got revenue cat customer information on {revenue_cat_id=}; {rc.is_sandbox=}: {truth=}"
        )

        entitlements: Dict[str, CachedEntitlement] = dict()
        for (
            entitlement_identifier,
            raw_entitlement,
        ) in truth.subscriber.entitlements.items():
            if (
                raw_entitlement.expires_date is not None
                and raw_entitlement.expires_date < dnow
            ):
                entitlements[entitlement_identifier] = CachedEntitlement(
                    is_active=False,
                    active_info=None,
                    expires_at=raw_entitlement.expires_date.timestamp(),
                    checked_at=now,
                )
                logger.debug(
                    f"checking {entitlement_identifier=} for {revenue_cat_id=} found expired: {entitlements[entitlement_identifier]!r}"
                )
                continue

            if raw_entitlement.expires_date is None:
                logger.debug(
                    f"checking {entitlement_identifier=} for {revenue_cat_id=} found lifetime, searching for platform in non_subscriptions.."
                )
                platform: Optional[CachedEntitlementPlatform] = None
                best_difference: Optional[float] = None
                for purchases in truth.subscriber.non_subscriptions.values():
                    for purchase in purchases:
                        if not rc.is_sandbox and purchase.is_sandbox:
                            logger.debug(
                                f"skipping purchase in wrong environment: {purchase=}"
                            )
                            continue

                        purchase_time_difference = abs(
                            (
                                purchase.purchase_date - raw_entitlement.purchase_date
                            ).total_seconds()
                        )
                        if (
                            best_difference is None
                            or best_difference > purchase_time_difference
                        ):
                            platform = await _store_to_platform(purchase.store)
                            best_difference = purchase_time_difference
                            logger.debug(
                                f"found a purchase at {purchase.purchase_date=} via {platform=} with time diff {best_difference=}"
                            )

                if platform is None:
                    await handle_warning(
                        f"{__name__}:no_lifetime_platform",
                        f"no lifetime platform for {revenue_cat_id=} via\n\n```json\n{truth.model_dump_json(indent=2)}\n```",
                    )
                    platform = "promotional"
                    logger.debug(
                        f"no lifetime platform for {revenue_cat_id=}, guessing {platform=}"
                    )

                entitlements[entitlement_identifier] = CachedEntitlement(
                    is_active=True,
                    active_info=CachedEntitlementActiveInfo(
                        recurrence=CachedEntitlementRecurrenceLifetime(type="lifetime"),
                        platform=platform,
                    ),
                    expires_at=None,
                    checked_at=now,
                )
                logger.debug(
                    f"checking {entitlement_identifier=} for {revenue_cat_id=} found lifetime: {entitlements[entitlement_identifier]!r}"
                )
                continue

            platform: Optional[CachedEntitlementPlatform] = None
            best_recurrence: Optional[CachedEntitlementRecurrenceRecurring] = None
            best_date: Optional[datetime.datetime] = None

            logger.debug(
                f"checking {entitlement_identifier=} for {revenue_cat_id=} found subscription, searching for platform and recurrence..."
            )

            for (
                subscription_type,
                subscription,
            ) in truth.subscriber.subscriptions.items():
                if subscription.refunded_at is not None:
                    logger.debug(f"skipping refunded subscription: {subscription=}")
                    continue

                if not rc.is_sandbox and subscription.is_sandbox:
                    logger.debug(
                        f"skipping subscription in wrong environment: {subscription=}"
                    )
                    continue

                if best_date is None or abs(
                    subscription.expires_date.timestamp()
                    - raw_entitlement.expires_date.timestamp()
                ) < abs(
                    best_date.timestamp() - raw_entitlement.expires_date.timestamp()
                ):
                    platform = await _store_to_platform(subscription.store)
                    best_date = subscription.expires_date

                    period = await _period_from_subscription_key(
                        itgs, subscription_type
                    )
                    if period is None:
                        cycle_time = (
                            subscription.expires_date - subscription.purchase_date
                        )
                        cycle_days_approx = round(
                            cycle_time.total_seconds() / (60 * 60 * 24)
                        )

                        if cycle_days_approx == 1:
                            period = Period(iso8601="P1D")
                        elif cycle_days_approx == 7:
                            period = Period(iso8601="P1W")
                        elif cycle_days_approx >= 25 and cycle_days_approx <= 35:
                            period = Period(iso8601="P1M")
                        elif cycle_days_approx >= 85 and cycle_days_approx <= 95:
                            period = Period(iso8601="P3M")
                        elif cycle_days_approx >= 175 and cycle_days_approx <= 185:
                            period = Period(iso8601="P6M")
                        elif cycle_days_approx >= 355 and cycle_days_approx <= 375:
                            period = Period(iso8601="P1Y")
                        elif cycle_days_approx == 73000:
                            period = Period(iso8601="P200Y")
                        elif cycle_days_approx == 0:
                            # test purchase?
                            cycle_minutes_approx = round(
                                cycle_time.total_seconds() / 60
                            )
                            if cycle_minutes_approx == 5:
                                # 1 week or 1 month
                                period = Period(iso8601="PT5M")
                            elif cycle_minutes_approx == 10:
                                # 3 months
                                period = Period(iso8601="PT10M")
                            elif cycle_minutes_approx == 15:
                                # 6 months
                                period = Period(iso8601="PT15M")
                            elif cycle_minutes_approx == 30:
                                # 1 year
                                period = Period(iso8601="PT30M")
                            else:
                                await handle_warning(
                                    f"{__name__}:bad_cycle_approx:test",
                                    f"bad cycle minutes approximation: {cycle_minutes_approx} minutes from {subscription_type} for {revenue_cat_id=}",
                                )
                                period = Period(iso8601=f"PT{cycle_minutes_approx}M")
                        else:
                            await handle_warning(
                                f"{__name__}:bad_cycle_approx",
                                f"bad cycle days approximation: {cycle_days_approx} from {subscription_type} for {revenue_cat_id=}",
                            )
                            period = Period(iso8601=f"P{cycle_days_approx}D")

                    best_recurrence = CachedEntitlementRecurrenceRecurring(
                        type="recurring",
                        period=period,
                        cycle_ends_at=subscription.expires_date.timestamp(),
                        auto_renews=subscription.unsubscribe_detected_at is None,
                    )
                    logger.debug(
                        f"found {platform=}, {best_recurrence=} via {subscription=}"
                    )

            if platform is None or best_recurrence is None:
                await handle_warning(
                    f"{__name__}:no_recurring_platform",
                    f"no recurring {platform=} or {best_recurrence=} for {revenue_cat_id=} via\n\n```json\n{truth.model_dump_json(indent=2)}\n```",
                )
                platform = "promotional"
                best_recurrence = CachedEntitlementRecurrenceRecurring(
                    type="recurring",
                    period=Period(iso8601="P1M"),
                    cycle_ends_at=raw_entitlement.expires_date.timestamp(),
                    auto_renews=False,
                )

            if best_recurrence.period.iso8601 == "P200Y":
                entitlements[entitlement_identifier] = CachedEntitlement(
                    is_active=True,
                    active_info=CachedEntitlementActiveInfo(
                        recurrence=CachedEntitlementRecurrenceLifetime(type="lifetime"),
                        platform=platform,
                    ),
                    expires_at=None,
                    checked_at=now,
                )
            else:
                entitlements[entitlement_identifier] = CachedEntitlement(
                    is_active=True,
                    active_info=CachedEntitlementActiveInfo(
                        recurrence=best_recurrence,
                        platform=platform,
                    ),
                    expires_at=raw_entitlement.expires_date.timestamp(),
                    checked_at=now,
                )
            logger.debug(
                f"final {entitlement_identifier=} for {revenue_cat_id=}: {entitlements[entitlement_identifier]!r}"
            )

        unjoined.append(LocalCachedEntitlements(entitlements=entitlements))

    return merge_revenue_cat_user_entitlements(unjoined)


def merge_revenue_cat_user_entitlements(
    arr: List[LocalCachedEntitlements],
) -> LocalCachedEntitlements:
    """Returns the maximally permissive union of the given entitlements."""
    if len(arr) == 1:
        return arr[0]

    result = LocalCachedEntitlements(entitlements={})
    for entitlements in arr:
        for key, value in entitlements.entitlements.items():
            if key not in result.entitlements:
                result.entitlements[key] = value
            else:
                current = result.entitlements[key]
                if _compare_entitlements(current, value) > 0:
                    result.entitlements[key] = value

    return result


async def get_entitlement_from_redis(
    itgs: Itgs, *, user_sub: str, identifier: str
) -> Optional[CachedEntitlement]:
    """Gets the entitlement information with the given identifier for the
    given user from redis, if it's available, otherwise returns None.

    Not generally used directly. Prefer `get_entitlement`

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlement we are getting
        identifier (str): the identifier of the entitlement we are getting

    Returns:
        CachedEntitlement: if the entitlement was found in redis for that user,
            the entitlement, otherwise None
    """
    redis = await itgs.redis()

    raw = await typing_cast(
        Awaitable[Optional[bytes]],
        redis.hget(
            f"entitlements:{user_sub}".encode("utf-8"), identifier.encode("utf-8")  # type: ignore
        ),
    )
    if raw is None:
        return None

    return CachedEntitlement.model_validate_json(raw)


async def upsert_entitlements_to_redis(
    itgs: Itgs, *, user_sub: str, entitlements: Dict[str, CachedEntitlement]
) -> None:
    """For each specified entitlement sets or replaces the cached
    entitlement information for the given user and identifier pair in redis.
    If there were entitlements already cached for the user which are not
    specified, they are kept as-is.

    All entitlements for a user are expired together. This operation resets the
    expiry time.

    Not generally used directly. Prefer `get_entitlement`

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlement we are setting
        entitlements (Dict[str, CachedEntitlement]): the entitlements to set
    """
    redis = await itgs.redis()

    async with redis.pipeline(transaction=True) as pipe:
        pipe.multi()
        await pipe.hset(
            f"entitlements:{user_sub}".encode("utf-8"),  # type: ignore
            mapping=dict(
                (key.encode("utf-8"), value.__pydantic_serializer__.to_json(value))
                for (key, value) in entitlements.items()
            ),
        )
        await pipe.expire(f"entitlements:{user_sub}", 60 * 60 * 24)
        await pipe.execute()


async def purge_entitlements_from_redis(itgs: Itgs, *, user_sub: str) -> None:
    """Removes any entitlements cached for the given user from redis.

    Prefer `get_entitlement` with `force=True` to calling this directly.

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlements we are purging
    """
    redis = await itgs.redis()

    await redis.delete(f"entitlements:{user_sub}")


async def get_entitlements_from_local(
    itgs: Itgs, *, user_sub: str
) -> Optional[LocalCachedEntitlements]:
    """Fetches the entitlements for the given user from local cache, if they
    are available, otherwise returns None.

    Not generally used directly. Prefer `get_entitlement`

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlements we are getting

    Returns:
        LocalCachedEntitlements, None: if the users entitlement information was
            available, the entitlements, otherwise None
    """
    local_cache = await itgs.local_cache()

    raw = typing_cast(
        Optional[bytes], local_cache.get(f"entitlements:{user_sub}".encode("utf-8"))
    )
    if raw is None:
        return None

    return LocalCachedEntitlements.model_validate_json(raw)


async def set_entitlements_to_local(
    itgs: Itgs, *, user_sub: str, entitlements: LocalCachedEntitlements
) -> None:
    """Replaces the locally cached entitlement information for the user
    with the given sub with the given entitlements. The entitlements are
    cached for a short duration.

    Not generally used directly. Prefer `get_entitlement`

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlements we are setting
        entitlements (LocalCachedEntitlements): the entitlements to set
    """
    local_cache = await itgs.local_cache()

    local_cache.set(
        f"entitlements:{user_sub}".encode("utf-8"),
        entitlements.__pydantic_serializer__.to_json(entitlements),
        expire=60 * 60 * 24,
        tag="collab",
    )


async def purge_entitlements_from_local(itgs: Itgs, *, user_sub: str) -> None:
    """Purges any entitlements stored about the given user from the local
    cache.

    This is typically called from the entitlements purging loop. Prefer
    `get_entitlement` with `force=True` to calling this directly.

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlements we are purging
    """
    local_cache = await itgs.local_cache()

    local_cache.delete(f"entitlements:{user_sub}".encode("utf-8"))


async def get_entitlement(
    itgs: Itgs, *, user_sub: str, identifier: str, force: bool = False
) -> Optional[CachedEntitlement]:
    """The main interface to the entitlements. This will fetch the entitlement
    information for the given user and identifier from the nearest available
    source, respecting the force flag and filling in any gaps in the cache.

    Despite the multilayer cache, this will very rapidly synchronize across
    instances due to an active purging mechanism.

    This will automatically detect and workaround any issue that prevents us
    from communicating with revenue cat - such as a revenue cat outage, a
    slowdown (excessively long response times), AWS networking issues, BGP
    issues, etc, in such a way that minimizes the impact.

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlement we are getting
        identifier (str): the identifier of the entitlement we are getting
        force (bool): if True, the entitlement will be fetched from the source
            of truth regardless of whether it is available in a nearer cache. If
            false, the nearest cache will be used if available before falling
            back to the source of truth. Regardless of this flag, the caches
            will be filled in with fresher information if available and fetched
            - so in particular if force is True, the local cache and redis cache
            will both be updated every time. However, fetching an entitlement
            with force=True is not necessarily sufficient to purge caches for
            other entitlement identifiers.

    Returns:
        CachedEntitlement, None: if the user exists, the entitlement, otherwise None
    """

    if not force:
        local_cached = await get_entitlements_from_local(itgs, user_sub=user_sub)
        if local_cached is not None and identifier in local_cached.entitlements:
            return local_cached.entitlements[identifier]

        redis_cached_ent = await get_entitlement_from_redis(
            itgs, user_sub=user_sub, identifier=identifier
        )
        if redis_cached_ent is not None:
            local_cached = local_cached or LocalCachedEntitlements()
            local_cached.entitlements[identifier] = redis_cached_ent
            await set_entitlements_to_local(
                itgs, user_sub=user_sub, entitlements=local_cached
            )
            return redis_cached_ent

    if await is_revenue_cat_outage(itgs):
        return await fail_open_entitlement(
            itgs, user_sub=user_sub, identifier=identifier
        )

    now = time.time()
    try:
        to_cache = await asyncio.wait_for(
            get_entitlements_from_source(itgs, user_sub=user_sub, now=now), timeout=5
        )
    except Exception as exc:
        await handle_error(exc)
        await record_revenue_cat_error(itgs, now=now)
        return await fail_open_entitlement(
            itgs, user_sub=user_sub, identifier=identifier
        )

    if to_cache is None:
        return None

    if identifier not in to_cache.entitlements:
        to_cache.entitlements[identifier] = CachedEntitlement(
            is_active=False, expires_at=None, checked_at=now, active_info=None
        )

    await set_entitlements_to_local(itgs, user_sub=user_sub, entitlements=to_cache)
    await upsert_entitlements_to_redis(
        itgs, user_sub=user_sub, entitlements=to_cache.entitlements
    )
    await publish_purge_message(itgs, user_sub=user_sub, min_checked_at=now)
    return to_cache.entitlements[identifier]


async def is_revenue_cat_outage(itgs: Itgs) -> bool:
    """Determines if there is a revenue cat outage, based on the number of
    recent errors in `revenue_cat_errors`, in redis. This will automatically
    clip the errors

    Not generally used directly. Prefer `get_entitlement`, which will handle
    revenue cat outages appropriately.

    Args:
        itgs (Itgs): the integrations for networked services

    Returns:
        bool: True if there is a revenue cat outage, False otherwise
    """
    num_recent_errors = await zcard_exact_window_safe(
        itgs, b"revenue_cat_errors", int(time.time() - 60 * 5)
    )
    return num_recent_errors >= 10


async def record_revenue_cat_error(itgs: Itgs, *, now: float) -> None:
    """Records an error in communicating with revenue cat in redis.

    Args:
        itgs (Itgs): the integrations for networked services
        now (float): the time the error occurred
    """
    await zadd_exact_window_safe(
        itgs, b"revenue_cat_errors", b"revenue_cat_errors:idcounter", int(now)
    )


async def fail_open_entitlement(
    itgs: Itgs, *, user_sub: str, identifier: str
) -> CachedEntitlement:
    """Creates and caches an active entitlement with the given identifier
    for the user with the given sub. This is intended to be used only if
    we can't communicate with the source of truth.

    Not generally used directly. Prefer `get_entitlement`, which will handle
    revenue cat outages appropriately.

    Args:
        itgs (Itgs): the integrations for networked services
        user_sub (str): the sub of the user whose entitlement we are getting
        identifier (str): the identifier of the entitlement we are getting

    Returns:
        CachedEntitlement: the fail open entitlement
    """
    now = time.time()
    fail_open_entitlement = CachedEntitlement(
        is_active=True,
        expires_at=now + 60 * 10,
        checked_at=now,
        active_info=CachedEntitlementActiveInfo(
            recurrence=CachedEntitlementRecurrenceRecurring(
                type="recurring",
                period=Period(iso8601="P1M"),
                cycle_ends_at=now + 60 * 10,
                auto_renews=False,
            ),
            platform="promotional",
        ),
    )
    await upsert_entitlements_to_redis(
        itgs, user_sub=user_sub, entitlements={identifier: fail_open_entitlement}
    )

    current_local = await get_entitlements_from_local(itgs, user_sub=user_sub)
    if current_local is None:
        current_local = LocalCachedEntitlements(entitlements={})
    current_local.entitlements[identifier] = fail_open_entitlement
    await set_entitlements_to_local(itgs, user_sub=user_sub, entitlements=current_local)
    await publish_purge_message(itgs, user_sub=user_sub, min_checked_at=now)
    return fail_open_entitlement


class EntitlementsPurgePubSubMessage(BaseModel):
    """The format of messages sent over the entitlements purge pubsub channel."""

    user_sub: str = Field()
    min_checked_at: float = Field(
        description="the cache should be purged of any entitlements checked before this unix time"
    )


async def publish_purge_message(
    itgs: Itgs, *, user_sub: str, min_checked_at: float
) -> None:
    """Notifies instances to purge any locally cached entitlements for the user with
    the given sub which were checked before the given time.
    """
    redis = await itgs.redis()
    await redis.publish(
        b"ps:entitlements:purge",
        EntitlementsPurgePubSubMessage(user_sub=user_sub, min_checked_at=min_checked_at)
        .model_dump_json()
        .encode("utf-8"),
    )


async def purge_cache_loop_async() -> Never:
    """The main function run to handle purging the cache when a notification
    is received on the appropriate redis channel
    """
    assert pps.instance is not None
    try:
        async with pps.PPSSubscription(
            pps.instance, "ps:entitlements:purge", "entitlements"
        ) as sub:
            while True:
                raw_data = await sub.read()
                data = EntitlementsPurgePubSubMessage.model_validate_json(raw_data)

                async with Itgs() as itgs:
                    local = await get_entitlements_from_local(
                        itgs, user_sub=data.user_sub
                    )
                    if local is None:
                        continue

                    old_len = len(local.entitlements)
                    local.entitlements = dict(
                        (k, v)
                        for k, v in local.entitlements.items()
                        if v.checked_at >= data.min_checked_at
                    )
                    if old_len > len(local.entitlements):
                        await set_entitlements_to_local(
                            itgs, user_sub=data.user_sub, entitlements=local
                        )
    except Exception as e:
        if pps.instance.exit_event.is_set() and isinstance(e, pps.PPSShutdownException):
            return  # type: ignore
        await handle_error(e)
    finally:
        print("entitlements purge loop exiting")


async def _store_to_platform(store: str) -> CachedEntitlementPlatform:
    if store in ("app_store", "App Store"):
        return "ios"
    if store in ("play_store", "Play Store"):
        return "google"
    if store in ("stripe", "Stripe"):
        return "stripe"
    if store in ("promotional", "Promotional"):
        return "promotional"

    guess: Optional[CachedEntitlementPlatform] = None
    store_lower_no_spaces = (
        store.lower().replace(" ", "").replace("-", "").replace("_", "")
    )
    if "stripe" in store_lower_no_spaces:
        guess = "stripe"
    elif "promo" in store_lower_no_spaces:
        guess = "promotional"
    elif "playstore" in store_lower_no_spaces:
        guess = "google"
    elif "appstore" in store_lower_no_spaces:
        guess = "ios"

    await handle_warning(
        f"{__name__}:unknown_store:{store}",
        f"Unknown store from revenue cat: {store!r}, guessing: {guess!r}",
        is_urgent=guess is None,
    )
    return "promotional" if guess is None else guess


async def _period_from_subscription_key(itgs: Itgs, key: str) -> Optional[Period]:
    """Attempts to guess the period of a subscription from the key"""
    if key.startswith("prod_"):
        return await _get_period_from_product_identifier(itgs, key)

    if key == "annual":
        # shows in UI
        return Period(iso8601="P1Y")
    if key == "onemonth":
        # shows in UI
        return Period(iso8601="P1M")

    if key == "oseh_pro_1":
        # android; subscription duration is not available without knowing the
        # base plan id
        return None

    if key.startswith("rc_promo_pro_"):
        frequency_str = key[len("rc_promo_pro_") :]
        if frequency_str.startswith("cat_"):
            frequency_str = frequency_str[len("cat_") :]
        if frequency_str == "monthly":
            # shows in UI
            return Period(iso8601="P1M")
        elif frequency_str == "yearly":
            # confirmed for some users
            return Period(iso8601="P1Y")
        elif frequency_str == "lifetime":
            # confirmed for some users
            return Period(iso8601="P100Y")

        await handle_warning(
            f"{__name__}:bad_promo_frequency",
            f"unknown promotional frequency: {frequency_str} from {key}",
        )

        # guessing from here based on the options in the ui
        if frequency_str in ("yearly", "oneyear", "one_year"):
            return Period(iso8601="P1Y")
        if frequency_str in ("weekly", "oneweek", "one_week"):
            return Period(iso8601="P1W")
        if frequency_str in ("daily", "oneday", "one_day"):
            return Period(iso8601="P1D")
        if frequency_str in ("threedays", "threeday", "three_days", "three_day"):
            return Period(iso8601="P3D")
        if frequency_str in (
            "threemonths",
            "threemonths",
            "three_months",
            "quarterly",
            "onequarter",
            "one_quarter",
        ):
            return Period(iso8601="P3M")
        if frequency_str in (
            "sixmonths",
            "sixmonth",
            "six_months",
            "halfyear",
            "half_year",
            "semiannual",
            "semiannually",
            "semi_annually",
        ):
            return Period(iso8601="P6M")
        if frequency_str in ("lifetime", "forever"):
            return Period(iso8601="P100Y")

        await handle_warning(
            f"{__name__}:bad_promo_frequency:no_guess",
            f"unknown and unguessable promotional frequency: {frequency_str} from {key}",
            is_urgent=True,
        )
        return None
    elif key.startswith("rc_promo_") and key.endswith("_lifetime"):
        return Period(iso8601="P200Y")

    if key.startswith("oseh_"):
        # ios keys are in the form oseh_{price}_{period}_{trialinfo}
        parts = key.split("_")
        if len(parts) == 4:
            period_key = parts[2]
            period_iso8601 = "P" + period_key.upper()
            if period_iso8601 in ("P1Y", "P1M", "P1W", "P1D", "P3M", "P6M", "P200Y"):
                return Period(iso8601=period_iso8601)

    await handle_warning(f"{__name__}:bad_period_key", f"unknown period key: {key}")

    # guessing
    if key in ("oneyear", "one_year", "year"):
        return Period(iso8601="P1Y")
    if key in ("one_month", "monthly", "month"):
        return Period(iso8601="P1M")
    if key in ("one_week", "weekly", "week"):
        return Period(iso8601="P1W")
    if key in ("one_day", "daily", "day"):
        return Period(iso8601="P1D")
    if key in ("bimonthly", "twomonths", "two_months"):
        return Period(iso8601="P2M")
    if key in ("quarterly", "threemonths", "three_months"):
        return Period(iso8601="P3M")
    if key in (
        "semiannual",
        "sixmonths",
        "six_months",
        "semiannually",
        "semi_annually",
        "semi_annual",
    ):
        return Period(iso8601="P6M")
    if key.endswith("_lifetime"):
        return Period(iso8601="P200Y")

    await handle_warning(
        f"{__name__}:bad_period_key:no_guess",
        f"unknown and unguessable period key: {key}",
        is_urgent=True,
    )
    return None


async def _get_period_from_product_identifier(
    itgs: Itgs, product_id: str
) -> Optional[Period]:
    """Attempts to determine the period for the subscription product with the
    given id
    """
    if not product_id.startswith("prod_"):
        await handle_warning(
            f"{__name__}:unknown_product_id",
            f"trying to get period from {product_id=} which is not in a recognized format",
        )
        return None

    price = await get_localized_price(
        itgs, user_sub=None, platform_product_identifier=product_id, platform="stripe"
    )
    if price is None or price.default_option is None:
        await handle_warning(
            f"{__name__}:no_default_option",
            f"trying to get period from stripe {product_id=} but it has no default option:\n\n```json\n{None if price is None else price.model_dump_json()}\n```",
        )
        return None
    return price.default_option.pricing_phases[-1].billing_period


def _compare_entitlements(a: CachedEntitlement, b: CachedEntitlement) -> int:
    if a.is_active is not b.is_active:
        # prefer active (larger)
        return int(b.is_active) - int(a.is_active)
    if (a.expires_at is None) is not (b.expires_at is None):
        # prefer non-expiring (larger)
        return int(b.expires_at is None) - int(a.expires_at is None)
    if a.expires_at is not None and b.expires_at is not None:
        # prefer later expiration (larger)
        return int(b.expires_at - a.expires_at)
    return 0
