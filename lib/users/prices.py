"""This module helps going from a revenue cat platform_product_identifier to
a useful price.

Specifically, this module helps with the following flow:

1. Have a user (typically, the logged in user on a client via their id JWT)
   and you want to display what plans they can buy on a particular platform
2. Typically, switch to backend code via an HTTP API call to our servers
3. Verify the user doesn't already have the `pro` entitlement on any of their
   revenue cat identities
4. Get identifies for what Package's RevenueCat wants to display for their latest
   revenue cat id.
   (Package is `identifier: string` plus `platform_product_identifier: string`)
5. **THIS MODULE** Convert from `platform_product_identifier` to a price that
   can actually be displayed. For example, $49.99/year after 3 day free trial,
   then $99.99/year
6. Display the price to the user
7. User purchases the product, for native apps this is done via communication
   with the app store without our involvement. For stripe, we need to build them
   a checkout session to go to.

Note that for client apps, the step this module take needs to occur on the
device rather than on our server, because it needs to communicate with the
corresponding app store and do price localization. That's why RevenueCat 
does not include the price in the API response. For Stripe, however, any
localization needs to be done by us, hence the need for us to act as the
app store for returning prices

This is the platform-agnostic code, i.e., models and glue code. The platform
specific stuff is in e.g. stripe_prices.py

Note that this is meant to just fill in for PurchasesStoreProduct from the
revenue cat sdk where the revenue cat sdk is unavailable (e.g., the web).
The models are purposely made to be as swappable as possible.

RevenueCat has this annoying technique of using an internal libraries types in
their API boundaries

https://revenuecat.github.io/react-native-purchases-docs/7.18.0/classes/default.html#getOfferings

returns Promise<PurchasesOfferings>, but PurchasesOfferings is imported
from '@revenuecat/purchases-typescript-internal', whose index method
https://github.com/RevenueCat/purchases-hybrid-common/blob/main/typescript/src/index.ts
is just a bunch of `export * from ` statements. If we dig down, however, we can eventually
find the type 
https://github.com/RevenueCat/purchases-hybrid-common/blob/main/typescript/src/offerings.ts#L326


Via `offerings.current.availablePackages[n].product` there is actually useful price
information. Specifically, `priceString` generally relates to the formatted price in
the local curency, and `subscriptionPeriod` is how long the entitlement lasts.

So this clearly contains the right information, but the documentation has a lot to be
desired: is the price an integer? Whats an example of the price string? Why are there
potentially multiple options on Google Play for a single price, when prices are already
restricted to a particular product? How can you tell if they are eligible for an intro
price?

https://www.revenuecat.com/docs/getting-started/displaying-products

This clearly shows everything we want; it displays two options,
includes a price comparison (SAVE 38%), includes trial text. We
just need to figure out how to extract that information from the
above object, and then reverse how to construct that object from
that information! But they don't actually explain how to do that :(

They have an example app called MagicWeather, but it renders using their
native UI paywall:

https://github.com/RevenueCat/react-native-purchases/blob/main/examples/MagicWeather/src/screens/PaywallScreen/index.js

They use title, description, and priceString. Clearly this is missing
the discount, trial period, etc...

If we look over their github issues we can find examples of what the
objects used to look like and some useful discussion:

https://github.com/RevenueCat/react-native-purchases/issues/727

Best guess from that:

- Price to show: `priceString`, e.g., $3.49

Determining if its a subscription: `productCategory` == `SUBSCRIPTION`;
otherwise, assume lifetime

If subscription:

- Period to show: `subscriptionPeriod`, e.g., `P1M`, requires parsing ISO 8601
- Trial period: `introPrice.period` @ `introPrice.priceString` for `introPrice.cycles` cycles
  If the intro price is zero, otherwise, ignore it and treat it as a discount below

discounts:
- ios:
  in order, after the trial, before the infinitely recurring price from just `priceString`, show
  - `discounts[n].period` @ `discounts[n].priceString` for `discounts[n].cycles` cycles
- android:
  - `defaultOption.pricingPhases[n].price.formatted` @ `defaultOption.pricingPhases[n].billingPeriod.iso8601` 
     for `defaultOption.pricingPhases[n].billingCycleCount`
    SKIP the first one if it's free, as it is just the trial period

what we do -> adapt stripe to the android model, dropping all fields except those
required for the above. This should allow downcasting in typescript from the RC
model to the model returned here

NOTE:
    Google Play supports prepaid subscriptions via pricing phases `offer_payment_mode`,
    e.g., you can buy 1 year at a set price. We include enough to at least error out on
    the client if that's the type of offer, which is in the pricing phase offer_payment_mode
    and recurrence_mode
"""

from pydantic import BaseModel, Field
from typing import Awaitable, Callable, Dict, Literal, Optional, List
from itgs import Itgs
import importlib


ProductCategory = Literal["NON_SUBSCRIPTION", "SUBSCRIPTION", "UNKNOWN"]
RecurrenceMode = Literal[1, 2, 3]
"""1 - INFINITE_RECURRING, 2 - FINITE_RECURRING, 3 - NON_RECURRING"""
OfferPaymentMode = Literal[
    "FREE_TRIAL", "SINGLE_PAYMENT", "DISCOUNTED_RECURRING_PAYMENT"
]


class Period(BaseModel):
    iso8601: str = Field(
        description=("The ISO 8601 duration of the period, e.g., `P1M` for 1 month. ")
    )

    def in_days(self) -> int:
        """Converts this period to a number of days. Raises ValueError if the period
        is not in a convertible format.

        Uses 31 days for a month and 366 days for a year.
        """
        assert len(self.iso8601) > 2
        if self.iso8601[0] != "P":
            raise ValueError(f"Invalid period: {self.iso8601}")

        unit_specifier = self.iso8601[-1]
        assert unit_specifier in ("D", "W", "M", "Y")

        count_in_unit = int(self.iso8601[1:-1])
        if unit_specifier == "D":
            return count_in_unit
        elif unit_specifier == "W":
            return count_in_unit * 7
        elif unit_specifier == "M":
            return count_in_unit * 31
        elif unit_specifier == "Y":
            return count_in_unit * 366
        raise ValueError(f"Invalid period: {self.iso8601}")


class Price(BaseModel):
    formatted: str = Field(
        description="Formatted price of the item, including its currency sign, e.g., $3.49"
    )
    amount_micros: int = Field(
        description="The price in the currency, in micros, e.g., 3490000 for $3.49. This is a localized, rounded price in the currency"
    )
    currency_code: str = Field(
        description="The unique identifier for the currency in the price. Only prices in the same currency are comparable"
    )


class PricingPhase(BaseModel):
    billing_period: Period = Field(
        description=(
            "For DISCOUNTED_RECURRING_PAYMENT, this describes the duration of "
            "the cycles. For other recurrence modes, this describes how long "
            "the price is valid for before moving onto the next phase."
        )
    )
    recurrence_mode: RecurrenceMode = Field(
        description=(
            "How this phase recurs:\n"
            "1 - INFINITE_RECURRING: pricing phase repeats infinitely until cancellation\n"
            "2 - FINITE_RECURRING: repeats for a fixed number of billing periods\n"
            "3 - NON_RECURRING: this is a one-time payment\n"
        )
    )
    billing_cycle_count: Optional[int] = Field(
        description=(
            # The documentation in the RevenueCat API appears to be incorrect. It says
            # it would be null for INFINITE_RECURRING and FINITE_RECURRING, which makes
            # no sense.
            # https://developer.android.com/reference/com/android/billingclient/api/ProductDetails.PricingPhase#getBillingCycleCount()
            "For INFINITE_RECURRING and NON_RECURRING, this is `null`. Otherwise, this is "
            "the number of billing periods, between 1 and 52 inclusive, that the price "
            "repeats for."
        )
    )
    price: Price = Field(
        description=(
            "The price. For INFINITE_RECURRING and FINITE_RECURRING, this is the price "
            "per cycle. For NON_RECURRING, this is the one-time price. An exact price of "
            "0 indicates this phase is free, which is typically only for the first "
            "phase and is normally called a trial"
        )
    )
    # https://support.google.com/googleplay/android-developer/answer/12154973?hl=en#:~:text=prepaid%20base%20plan
    offer_payment_mode: Optional[OfferPaymentMode] = Field(
        description=(
            "How the user pays for this phase. If FREE_TRIAL, the user gets the "
            "product for free for the duration of the phase. If SINGLE_PAYMENT, the "
            "user up-front for the entire phase. If DISCOUNTED_RECURRING_PAYMENT, the "
            "user pays a discounted price for the duration of the phase. note that "
            "a FINITE_RECURRING phase can be prepaid"
        )
    )


class SubscriptionOption(BaseModel):
    """Describes how a subscription is priced in more detail"""

    pricing_phases: List[PricingPhase] = Field(
        description=(
            "The phases of the subscription in the order they occur. Once all "
            "the cycles for a phase are exhausted, the subscription moves on to the "
            "next phase."
        )
    )


class PurchasesStoreProduct(BaseModel):
    price: float = Field(
        description=(
            "The price in the currency; this should never be used for display: "
            "the only useful thing for this value is if you have two price values "
            "with the same currency code, then you can compare them to see the "
            "relative values, e.g, that one is 30% more expensive than the other. "
            "This value may be inexact. The only value guarranteed to be exact is `0`, "
            "for free"
        )
    )
    currency_code: str = Field(
        description=(
            "The unique identifier for the currency in the price. "
            "Only prices in the same currency are comparable"
        )
    )
    price_string: str = Field(
        description="Formatted price of the item, including its currency sign, e.g., $3.49"
    )
    product_category: ProductCategory = Field(
        description=(
            "The category of the product, either a subscription or a non-subscription. "
            "If the category is unknown, it should be treated as a non-subscription"
        )
    )
    default_option: Optional[SubscriptionOption] = Field(
        description=(
            "If this is subscription-like, this describes the subscription "
            "as a sequence of phases. The last phase is typically infinitely recurring.\n\n"
            "Through an initial phase with a $0 price, this can include a trial period."
        )
    )


get_localized_price_by_platform: Dict[
    Literal["stripe"],
    Callable[[Itgs, Optional[str], str], Awaitable[Optional[PurchasesStoreProduct]]],
] = dict()


async def get_localized_price(
    itgs: Itgs,
    /,
    *,
    user_sub: Optional[str],
    platform_product_identifier: str,
    platform: Literal["stripe"],
) -> Optional[PurchasesStoreProduct]:
    """Determines the localized price for the given product on the given platform.

    Args:
        itgs (Itgs): the integrations to (re)use
        user_sub (str, None): the sub of the user to ge the price for. This will return
            a price without necessarily verifying the user is not already subscribed.
            If the user sub is not provided, a generic locale is used
        platform_product_identifier (str): the platform specific identifier for the
            product they are trying to purchaser. For our purposes, the annual
            $99 subscription is a different product compared to the monthly $13
            subscription even though they give the same entitlement. For stripe,
            this is the product id, not the price id.
        platform ("stripe"): which platform to get the price for.

    Returns:
        PurchasesStoreProduct, None: price information on the given product, if
            localization was successful and the product could be found, None otherwise
    """
    handler = get_localized_price_by_platform.get(platform)
    if handler is None:
        importlib.import_module(f"lib.users.{platform}_prices")
        handler = get_localized_price_by_platform.get(platform)
        if handler is None:
            raise ValueError(
                f"module lib.users.{platform}_prices did not define a handler for get_localized_price_by_platform['{platform}']"
            )

    return await handler(itgs, user_sub, platform_product_identifier)
