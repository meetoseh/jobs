import asyncio
import gzip
import io
import os
import random
from typing import Any, Awaitable, Dict, Literal, Optional, cast
from pydantic import BaseModel, Field
from itgs import Itgs
from lib.users.prices import (
    Period,
    Price,
    PricingPhase,
    PurchasesStoreProduct,
    SubscriptionOption,
    get_localized_price_by_platform,
)
import stripe
import perpetual_pub_sub as pps
from lib.users.stripe_trials import is_user_stripe_trial_eligible
import time


async def get_localized_price(
    itgs: Itgs, user_sub: Optional[str], product_id: str, now: Optional[float] = None
) -> Optional[PurchasesStoreProduct]:
    """Determines the localized price for the user with the given sub purchasing
    the stripe product with the given id.

    Args:
      itgs (Itgs): the integrations to (re)use
      user_sub (str, None): the sub of the user whose trying to purchase the product, or
        None for no localization
      product_id (str): the id of the stripe product
    """
    # Supported price localization for stripe:
    #
    # This has to match what we do in `start_checkout_stripe`
    #
    # - If no user is provided, we assume a trial is not available
    # - Otherwise, if the user has not had Oseh+ in the last 60 days,
    #   and the metadata includes a `trial` key whose value is a ISO8601
    #   duration, we will use that as the trial period. Otherwise, we assume
    #   no trial is available

    # For now we don't do any regional pricing on stripe, but the idea
    # would be if we were going to do that, we would have multiple prices
    # on the products and use some kind of tag/metadata on them. Would have
    # to update get_default_stripe_price_id in start_checkout_stripe as well
    if now is None:
        now = time.time()

    trial_eligible_task = (
        None
        if user_sub is None
        else asyncio.create_task(
            is_user_stripe_trial_eligible(itgs, user_sub=user_sub, now=now)
        )
    )
    try:
        price = await get_stripe_price(itgs, product_id)
    finally:
        trial_eligible = (
            False if trial_eligible_task is None else await trial_eligible_task
        )

    if price is None:
        return None
    return await convert_stripe_price_to_purchases_store_product(
        itgs, user_sub, price, trial_eligible
    )


get_localized_price_by_platform["stripe"] = get_localized_price


class AbridgedStripeRecurringInfo(BaseModel):
    interval: Literal["day", "week", "month", "year"] = Field(
        description="the frequency with which a subscription should be billed"
    )
    interval_count: int = Field(
        description="the number of intervals between subscription billings"
    )
    usage_type: Literal["metered", "licensed"] = Field(
        description="describes how to compute the price per period"
    )


def convert_to_iso8601(
    interval: Literal["day", "week", "month", "year"], interval_count: int
) -> str:
    if interval == "day":
        return f"P{interval_count}D"
    if interval == "week":
        return f"P{interval_count}W"
    if interval == "month":
        return f"P{interval_count}M"
    if interval == "year":
        return f"P{interval_count}Y"
    raise ValueError(f"Unknown interval: {interval}")


class AbridgedStripePrice(BaseModel):
    """What we store about stripe prices, which we then convert to
    PurchasesStoreProduct objects. We store them as closely as possible to
    how stripe represents them so that we can update how we process them
    without, generally, needing to bust caches
    """

    id: str = Field(description="unique identifier for the object")
    currency: str = Field(description="three-letter ISO currency code, in lowercase")
    metadata: Dict[str, Any] = Field(
        description="set of key-value pairs that you can attach to an object"
    )
    type: Literal["one_time", "recurring"] = Field(
        description="type of the price, one_time or recurring"
    )
    livemode: bool = Field(
        description="has the value `true` if the object exists in live mode and `false` if the object exists in test mode"
    )
    tax_behavior: Optional[Literal["exclusive", "inclusive", "unspecified"]] = Field(
        description="determines the tax behavior for the price, if different from the default"
    )
    billing_scheme: Literal["per_unit", "tiered", "volume"] = Field(
        description="billing scheme for the price"
    )
    unit_amount: Optional[int] = Field(
        description="the amount in cents to be charged per unit, for per_unit pricing"
    )
    recurring: Optional[AbridgedStripeRecurringInfo] = Field(
        description="the recurring information for the price, if it is a recurring price"
    )


async def convert_stripe_price_to_purchases_store_product(
    itgs: Itgs,
    user_sub: Optional[str],
    stripe_price: AbridgedStripePrice,
    trial_eligible: bool,
) -> PurchasesStoreProduct:
    """Localizes the given stripe price to the user with the given sub, or the
    generic price if a user sub isn't available
    """
    assert stripe_price.billing_scheme == "per_unit", stripe_price
    assert stripe_price.currency == "usd", stripe_price
    assert stripe_price.unit_amount is not None, stripe_price
    assert stripe_price.livemode is (os.environ["ENVIRONMENT"] != "dev"), stripe_price

    trial_duration_raw = cast(Optional[str], stripe_price.metadata.get("trial"))
    trial_duration = Period(iso8601=trial_duration_raw) if trial_duration_raw else None

    exact_dollars = stripe_price.unit_amount // 100
    exact_extra_cents = stripe_price.unit_amount % 100
    price_string = (
        f"${exact_dollars}.{exact_extra_cents:02}"
        if exact_extra_cents != 0
        else f"${exact_dollars}"
    )

    if stripe_price.recurring is None:
        return PurchasesStoreProduct(
            price=stripe_price.unit_amount / 100.0,
            currency_code=stripe_price.currency.upper(),
            price_string=price_string,
            product_category="NON_SUBSCRIPTION",
            default_option=None,
        )

    return PurchasesStoreProduct(
        price=stripe_price.unit_amount / 100.0,
        currency_code=stripe_price.currency.upper(),
        price_string=price_string,
        product_category="SUBSCRIPTION",
        default_option=SubscriptionOption(
            pricing_phases=[
                *(
                    []
                    if trial_duration is None or not trial_eligible
                    else [
                        PricingPhase(
                            billing_period=trial_duration,
                            recurrence_mode=3,
                            billing_cycle_count=1,
                            price=Price(
                                formatted="free", amount_micros=0, currency_code="USD"
                            ),
                            offer_payment_mode="FREE_TRIAL",
                        )
                    ]
                ),
                PricingPhase(
                    billing_period=Period(
                        iso8601=convert_to_iso8601(stripe_price.recurring.interval, 1),
                    ),
                    recurrence_mode=1,
                    billing_cycle_count=stripe_price.recurring.interval_count,
                    price=Price(
                        formatted=price_string,
                        amount_micros=stripe_price.unit_amount * 10**4,
                        currency_code=stripe_price.currency.upper(),
                    ),
                    offer_payment_mode=None,
                ),
            ]
        ),
    )


async def get_stripe_price(
    itgs: Itgs,
    product_id: str,
) -> Optional[AbridgedStripePrice]:
    """Fetches the abridged stripe price information for the stripe product with
    the given id. This uses a 2-layer cooperative cache to reduce latency and
    the number of requests to stripe, but does not actively prevent stampeding.

    Args:
        itgs (Itgs): the integrations to (re)use
        product_id (str): the id of the stripe product

    Returns:
        (AbrigedStripePrice, None): the abridged stripe price information for the
            stripe product with the given id, or None if no such product exists
    """
    raw = await read_stripe_price_from_local_cache(itgs, product_id)
    if raw is not None:
        return _convert_from_stored(raw)
    raw = await read_stripe_price_from_remote_cache(itgs, product_id)
    if raw is not None:
        await write_stripe_price_to_local_cache(itgs, product_id, raw)
        return _convert_from_stored(raw)
    price = await read_stripe_price_from_source(itgs, product_id)
    if price is not None:
        raw = _convert_to_stored(price)
        await write_stripe_price_to_local_cache(itgs, product_id, raw)
        await write_stripe_price_to_remote_cache(itgs, product_id, raw)
        await write_stripe_price_to_all_local_caches(itgs, product_id, raw)
    return price


async def read_stripe_price_from_local_cache(
    itgs: Itgs, product_id: str
) -> Optional[bytes]:
    """Reads the serialized abridged stripe price information on the product with the given
    id from the local cache, if it's there
    """
    cache = await itgs.local_cache()
    return cast(Optional[bytes], cache.get(_cache_key(product_id)))


async def write_stripe_price_to_local_cache(
    itgs: Itgs, product_id: str, raw: bytes
) -> None:
    """Writes the serialized abridged stripe price information on the product with the
    given id to the local cache, with a slightly randomized expiration time to reduce
    stampeding effects
    """
    cache = await itgs.local_cache()
    cache.set(
        _cache_key(product_id),
        raw,
        expire=3600 + random.randint(2, 10),
        tag="collab",
    )


async def read_stripe_price_from_remote_cache(
    itgs: Itgs, product_id: str
) -> Optional[bytes]:
    """Reads the serialized abridged stripe price information on the product with the given
    id from the remote cache, if it's there
    """
    redis = await itgs.redis()
    return await cast(Awaitable[Optional[bytes]], redis.get(_cache_key(product_id)))


async def write_stripe_price_to_remote_cache(
    itgs: Itgs, product_id: str, raw: bytes
) -> None:
    """Writes the serialized abridged stripe price information on the product with the
    given id to the remote cache
    """
    redis = await itgs.redis()
    await redis.set(_cache_key(product_id), raw, ex=3600)


async def write_stripe_price_to_all_local_caches(
    itgs: Itgs, product_id: str, raw: bytes
) -> None:
    serd_product_id = product_id.encode("utf-8")

    message = io.BytesIO()
    message.write(len(serd_product_id).to_bytes(2, "big"))
    message.write(serd_product_id)
    message.write(len(raw).to_bytes(8, "big"))
    message.write(raw)

    full_message = message.getvalue()

    redis = await itgs.redis()
    await redis.publish(
        b"ps:stripe:products:prices",
        full_message,
    )


async def read_stripe_price_from_source(
    itgs: Itgs, product_id: str
) -> Optional[AbridgedStripePrice]:
    """Reads the abridged stripe price information from the actual source (stripe)
    for the product with the given id, if it exists
    """
    stripe_prices = await asyncio.to_thread(
        stripe.Price.list,
        api_key=os.environ["OSEH_STRIPE_SECRET_KEY"],
        product=product_id,
        active=True,
        limit=3,
    )

    if stripe_prices.is_empty:
        return None

    if len(stripe_prices) > 1:
        raise ValueError(f"More than one price found for product {product_id}")

    return _get_abridged(stripe_prices.data[0])


async def subscribe_to_stripe_price_updates():
    assert pps.instance is not None

    async with pps.PPSSubscription(
        pps.instance, "ps:stripe:products:prices", "sp_stspu"
    ) as sub:
        async for message_raw in sub:
            message = io.BytesIO(message_raw)
            serd_product_id_len = int.from_bytes(message.read(2), "big")
            product_id = message.read(serd_product_id_len).decode("utf-8")
            raw_len = int.from_bytes(message.read(8), "big")
            raw = message.read(raw_len)

            async with Itgs() as itgs:
                await write_stripe_price_to_local_cache(itgs, product_id, raw)


def _cache_key(product_id: str) -> bytes:
    return f"stripe:abridged_prices:{product_id}".encode("utf-8")


def _get_abridged(price: stripe.Price) -> AbridgedStripePrice:
    return AbridgedStripePrice(
        id=price.id,
        currency=price.currency,
        metadata=price.metadata,
        type=price.type,
        livemode=price.livemode,
        tax_behavior=price.tax_behavior,
        billing_scheme=price.billing_scheme,
        unit_amount=price.unit_amount,
        recurring=(
            AbridgedStripeRecurringInfo(
                interval=price.recurring.interval,
                interval_count=price.recurring.interval_count,
                usage_type=price.recurring.usage_type,
            )
            if price.recurring is not None
            else None
        ),
    )


def _convert_to_stored(price: AbridgedStripePrice) -> bytes:
    """Converts the given abridged stripe price to how we would store it in caches"""
    return gzip.compress(price.__pydantic_serializer__.to_json(price), mtime=0)


def _convert_from_stored(raw: bytes) -> AbridgedStripePrice:
    """Converts the given raw bytes to an abridged stripe price"""
    return AbridgedStripePrice.model_validate_json(gzip.decompress(raw))
