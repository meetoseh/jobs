"""This module assists with working with entitlements from RevenueCat"""
from typing import Dict, List, Literal, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import aiohttp

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
        "app_store", "mac_app_store", "play_store", "amazon", "stripe"
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


class RevenueCat:
    """The interface for interacting with RevenueCat. Acts as a
    async context manager, so you can use it with `async with`."""

    def __init__(self, sk: str, stripe_pk: str) -> None:
        self.sk: str = sk
        """The secret key for RevenueCat"""

        self.stripe_pk: str = stripe_pk
        """The public key for the Stripe app in RevenueCat"""

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

    async def get_customer_info(self, *, revenue_cat_id: str) -> CustomerInfo:
        """Gets the customer information for the given RevenueCat ID."""
        assert self.session is not None
        async with self.session.get(
            f"https://api.revenuecat.com/v1/subscribers/{revenue_cat_id}",
            headers={
                "Authorization": f"Bearer {self.sk}",
                "Accept": "application/json",
            },
        ) as resp:
            resp.raise_for_status()
            text = await resp.text()

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
            resp.raise_for_status()
            data = await resp.text("utf-8")
            return CustomerInfo.model_validate_json(data)

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
