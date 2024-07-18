"""This module assists with working with entitlements from RevenueCat"""

import os
from typing import Dict, List, Literal, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, TypeAdapter
import aiohttp
import logging as logger
import asyncio

from error_middleware import handle_error


class Entitlement(BaseModel):
    """https://www.revenuecat.com/reference/subscribers"""

    expires_date: Optional[datetime] = Field(None)
    grace_period_expires_date: Optional[datetime] = Field(None)
    purchase_date: datetime = Field()
    product_identifier: str = Field()


class Subscription(BaseModel):
    """https://www.revenuecat.com/reference/subscribers"""

    expires_date: datetime = Field()
    purchase_date: datetime = Field()
    original_purchase_date: datetime = Field()
    ownership_type: Optional[Literal["PURCHASED", "FAMILY_SHARED"]] = Field(None)
    store: Literal[
        "app_store", "mac_app_store", "play_store", "amazon", "stripe", "promotional"
    ] = Field()
    is_sandbox: bool = Field()
    unsubscribe_detected_at: Optional[datetime] = Field(None)
    billing_issues_detected_at: Optional[datetime] = Field(None)
    grace_period_expires_date: Optional[datetime] = Field(None)
    refunded_at: Optional[datetime] = Field(None)
    auto_resume_date: Optional[datetime] = Field(None)


class NonSubscription(BaseModel):
    """https://www.revenuecat.com/reference/subscribers"""

    id: str = Field()
    purchase_date: datetime = Field()
    store: Literal[
        "app_store", "mac_app_store", "play_store", "amazon", "stripe", "promotional"
    ] = Field()
    is_sandbox: bool = Field()


class SubscriberAttribute(BaseModel):
    """https://www.revenuecat.com/reference/subscribers"""

    value: str = Field()
    updated_at_ms: float = Field()


class Subscriber(BaseModel):
    """https://www.revenuecat.com/reference/subscribers"""

    original_app_user_id: str = Field()
    original_application_version: Optional[str] = Field(None)
    original_purchase_date: Optional[datetime] = Field(None)
    management_url: Optional[str] = Field(None)
    first_seen: datetime = Field()
    last_seen: datetime = Field()
    entitlements: Dict[str, Entitlement] = Field()
    subscriptions: Dict[str, Subscription] = Field()
    non_subscriptions: Dict[str, List[NonSubscription]] = Field()
    subscriber_attributes: Dict[str, SubscriberAttribute] = Field(default_factory=dict)


class CustomerInfo(BaseModel):
    """https://www.revenuecat.com/reference/subscribers"""

    request_date: datetime = Field()
    request_date_ms: float = Field()
    subscriber: Subscriber = Field()


class Package(BaseModel):
    # From https://www.revenuecat.com/docs/api-v1#tag/offerings/operation/get-offerings
    identifier: str = Field()
    platform_product_identifier: str = Field()

    # https://community.revenuecat.com/third-party-integrations-53/android-subscription-adding-base-plan-id-to-product-id-2710
    # used in android subscriptions, omitted from the docs
    platform_product_plan_identifier: Optional[str] = Field(None)


RCEnv = Literal["production", "dev"]


class OfferingMetadata(BaseModel):
    # We add this via the metadata section to add basic environment support
    environment: Literal[RCEnv] = Field()
    alternative: Dict[RCEnv, str] = Field()


class OfferingWithoutMetadata(BaseModel):
    # From https://www.revenuecat.com/docs/api-v1#tag/offerings/operation/get-offerings
    description: str = Field()
    identifier: str = Field()
    packages: List[Package] = Field()


class Offering(BaseModel):
    # From https://www.revenuecat.com/docs/api-v1#tag/offerings/operation/get-offerings
    description: str = Field()
    identifier: str = Field()
    metadata: OfferingMetadata = Field()
    packages: List[Package] = Field()

    def strip_metadata(self) -> OfferingWithoutMetadata:
        return OfferingWithoutMetadata(
            description=self.description,
            identifier=self.identifier,
            packages=self.packages,
        )


class OfferingsWithoutMetadata(BaseModel):
    # From https://www.revenuecat.com/docs/api-v1#tag/offerings/operation/get-offerings
    current_offering_id: str = Field()
    offerings: List[OfferingWithoutMetadata] = Field()


class Offerings(BaseModel):
    # From https://www.revenuecat.com/docs/api-v1#tag/offerings/operation/get-offerings
    current_offering_id: str = Field()
    offerings: List[Offering] = Field()

    def strip_metadata(self) -> OfferingsWithoutMetadata:
        return OfferingsWithoutMetadata(
            current_offering_id=self.current_offering_id,
            offerings=[off.strip_metadata() for off in self.offerings],
        )


class NoOfferings(BaseModel):
    current_offering_id: Literal[None] = Field()
    offerings: List[Literal[None]] = Field(max_length=0)


list_offerings_result_validator: TypeAdapter[Union[Offerings, NoOfferings]] = (
    TypeAdapter(Union[Offerings, NoOfferings])
)


class RevenueCat:
    """The interface for interacting with RevenueCat. Acts as a
    async context manager, so you can use it with `async with`."""

    def __init__(
        self, sk: str, stripe_pk: str, playstore_pk: str, appstore_pk: str
    ) -> None:
        self.sk: str = sk
        """The secret key for RevenueCat"""

        self.stripe_pk: str = stripe_pk
        """The public key for the Stripe app in RevenueCat"""

        self.playstore_pk: str = playstore_pk
        """The public key for the Play Store app in RevenueCat"""

        self.appstore_pk: str = appstore_pk
        """The public key for the App Store app in RevenueCat"""

        self.is_sandbox: bool = os.environ["ENVIRONMENT"] == "dev"
        """If we're accessing the sandbox environment on revenuecat"""

        self.session: Optional[aiohttp.ClientSession] = None
        """If this has been entered as an async context manager, this will be
        the aiohttp session
        """

    async def __aenter__(self) -> "RevenueCat":
        if self.session is not None:
            raise RuntimeError("RevenueCat is non-reentrant")

        self.session = aiohttp.ClientSession()
        await self.session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session is None:
            raise RuntimeError("not entered")

        sess = self.session
        self.session = None

        await sess.__aexit__(exc_type, exc_val, exc_tb)

    async def get_customer_info(
        self, *, revenue_cat_id: str, handle_ratelimits: bool = False
    ) -> CustomerInfo:
        """Gets the customer information for the given RevenueCat ID."""
        assert self.session is not None

        ratelimit_counter = 0
        text = None
        while True:
            async with self.session.get(
                f"https://api.revenuecat.com/v1/subscribers/{revenue_cat_id}",
                headers={
                    "Authorization": f"Bearer {self.sk}",
                    "Accept": "application/json",
                    "X-Is-Sandbox": "true" if self.is_sandbox else "false",
                },
            ) as resp:
                if handle_ratelimits and resp.status == 429:
                    ratelimit_counter += 1
                    if ratelimit_counter > 10:
                        resp.raise_for_status()

                    retry_after_suggestion_raw = resp.headers.get("Retry-After")
                    retry_after_suggestion_ms: Optional[int] = None
                    if retry_after_suggestion_raw is not None:
                        try:
                            retry_after_suggestion_ms = int(retry_after_suggestion_raw)
                        except Exception:
                            pass

                    retry_after_ms = 1000 * (2 ** (ratelimit_counter - 1))
                    if retry_after_suggestion_ms is not None:
                        retry_after_ms = max(retry_after_ms, retry_after_suggestion_ms)

                    retry_after_ms = min(retry_after_ms, 1000 * 60)
                    await asyncio.sleep(retry_after_ms / 1000)
                    continue

                resp.raise_for_status()
                text = await resp.text()
                break

        try:
            return CustomerInfo.model_validate_json(text)
        except Exception as e:
            await handle_error(
                e, extra_info=f"for {revenue_cat_id=} and response {text=}"
            )
            raise Exception("Error parsing response from RevenueCat")

    async def set_customer_attributes(
        self, *, revenue_cat_id: str, attributes: Dict[str, str]
    ) -> None:
        """Updates the customer attributes (also referred to as subscriber
        attributes) for the given RevenueCat ID."""
        assert self.session is not None

        formatted_attrs = dict((key, {"value": val}) for key, val in attributes.items())

        async with self.session.post(
            f"https://api.revenuecat.com/v1/subscribers/{revenue_cat_id}/attributes",
            json={
                "attributes": formatted_attrs,
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.stripe_pk}",
                "Accept": "application/json",
            },
        ) as resp:
            resp.raise_for_status()

    async def delete_subscriber(self, *, revenue_cat_id: str) -> None:
        """Deletes the subscriber from RevenueCat."""
        assert self.session is not None

        async with self.session.delete(
            f"https://api.revenuecat.com/v1/subscribers/{revenue_cat_id}",
            headers={
                "Authorization": f"Bearer {self.sk}",
                "Accept": "application/json",
            },
        ) as resp:
            resp.raise_for_status()

    async def refund_and_revoke_google_play_subscription(
        self, *, revenue_cat_id: str, product_id: str
    ) -> None:
        """Immediately revokes access to a Google Subscription and issues a refund for the last purchase.

        Args:
            revenue_cat_id (str): The RevenueCat ID of the user
            product_id (str): The product id within revenue cat of the subscription to cancel
        """
        assert self.session is not None
        async with self.session.post(
            f"https://api.revenuecat.com/v1/subscribers/{revenue_cat_id}/subscriptions/{product_id}/revoke",
            headers={
                "Authorization": f"Bearer {self.sk}",
                "Accept": "application/json",
            },
        ) as resp:
            resp.raise_for_status()

    async def create_stripe_purchase(
        self,
        *,
        revenue_cat_id: str,
        stripe_checkout_session_id: str,
        is_restore: bool = False,
    ) -> CustomerInfo:
        """Informs revenuecat that the user has finished a stripe checkout session.
        This should occur either after the checkout.session.completed event or
        after the user indicates they completed the flow.

        Specifying is_restore=True will cause the default restore behavior, usually
        meaning that if the checkout session was used to apply entitlements to another
        user already, those entitlements are removed and added to this user.

        Args:
            revenue_cat_id (str): The RevenueCat ID of the user
            stripe_checkout_session_id (str): The ID of the stripe checkout session
                or subscription ID
            is_restore (bool, optional): Whether this is a restore operation. Defaults to False.
        """
        assert self.session is not None

        async with self.session.post(
            "https://api.revenuecat.com/v1/receipts",
            json={
                "app_user_id": revenue_cat_id,
                "fetch_token": stripe_checkout_session_id,
                "is_restore": is_restore,
                "attributes": {},
            },
            headers={
                "Authorization": f"Bearer {self.stripe_pk}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Platform": "stripe",
            },
        ) as resp:
            if not resp.ok:
                text = await resp.text()
                logger.warning(
                    f"create_stripe_purchase failed; {revenue_cat_id=}, stripe_checkout_session_id={stripe_checkout_session_id}, {resp.status=}, {text=}"
                )
            resp.raise_for_status()
            data = await resp.text("utf-8")
            try:
                return CustomerInfo.model_validate_json(data)
            except Exception as e:
                logger.warning(
                    f"create_stripe_purchase failed; {revenue_cat_id=}, stripe_checkout_session_id={stripe_checkout_session_id}, {resp.status=}, {data=}"
                )
                raise e

    async def grant_promotional_entitlement(
        self,
        *,
        revenue_cat_id: str,
        entitlement_identifier: str,
        duration: Literal[
            "daily",
            "three_day",
            "weekly",
            "monthly",
            "two_month",
            "three_month",
            "six_month",
            "yearly",
            "lifetime",
        ],
    ):
        """Grants the user with the given revenue cat id a promotional entitlement for the given duration.
        See https://www.revenuecat.com/reference/grant-a-promotional-entitlement

        Args:
            revenue_cat_id (str): The RevenueCat ID of the user
            entitlement_identifier (str): The identifier of the entitlement to grant
            duration ("daily", "three_day", "weekly", "monthly", "two_month", "three_month", "six_month", "yearly", "lifetime"):
                The duration of the entitlement
        """
        assert self.session is not None
        async with self.session.post(
            f"https://api.revenuecat.com/v1/subscribers/{revenue_cat_id}/entitlements/{entitlement_identifier}/promotional",
            headers={
                "Authorization": f"Bearer {self.sk}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json={
                "duration": duration,
            },
        ) as response:
            response.raise_for_status()

    async def list_offerings(
        self,
        *,
        revenue_cat_id: str,
        platform: Literal["stripe", "playstore", "appstore"],
    ) -> Optional[Offerings]:
        """Fetches the offerings of the app for the particular user on the given
        platform. If the user does not exist, this will return as if for a generic
        user.

        Prefer using `users.lib.offerings.get_offerings` over this as it will reduce
        traffic to revenuecat and has functions for augmenting the result.

        WARN:
            This will return offerings from all environments. You should filter
            the result to the environment you want.

        Args:
            revenue_cat_id (str): The RevenueCat ID of the user
            platform (Literal["stripe"]): The platform to get
                offers on; effects the interpretation of `platform_product_identifier`

        Returns:
            Offerings: The offerings available to the user or None if there are no
                offerings available
        """
        assert self.session is not None

        if platform == "stripe":
            platform_public_key = self.stripe_pk
        elif platform == "playstore":
            platform_public_key = self.playstore_pk
        elif platform == "appstore":
            platform_public_key = self.appstore_pk
        else:
            raise ValueError(
                f"unsupported platform (no public key available): {platform=}"
            )

        async with self.session.get(
            f"https://api.revenuecat.com/v1/subscribers/{revenue_cat_id}/offerings",
            headers={
                "Authorization": f"Bearer {platform_public_key}",
                "Accept": "application/json",
            },
        ) as response:
            response.raise_for_status()
            data = await response.read()
            result = list_offerings_result_validator.validate_json(data)
            if result.current_offering_id is None:
                return None
            logger.debug(f"converted revenue_cat offerings list {data!r} to {result!r}")
            return result
