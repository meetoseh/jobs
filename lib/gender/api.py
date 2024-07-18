"""This module is our adapter to gender-api.com and should not generally be used
directly; instead, use the `lib.gender.by_user` module which handles the 3-layer
cache (instance disk, redis, database).

The API key is usually via os.environ['OSEH_GENDER_API_KEY']
"""

from typing import Literal, Optional, Generic, TypeVar
from lib.gender.gender_source import (
    GenderByEmailAddressPayload,
    GenderByEmailAddressResponse,
    GenderByFirstNameResponse,
    GenderByFirstNamePayload,
    GenderByFullNamePayload,
    GenderByFullNameResponse,
)
from pydantic import BaseModel, Field
import logging as logger

import aiohttp


PayloadT = TypeVar("PayloadT", bound=BaseModel)
ResponseT = TypeVar("ResponseT", bound=BaseModel)

GenderAPIURL = Literal["https://gender-api.com/v2/gender"]
url: GenderAPIURL = "https://gender-api.com/v2/gender"


class GenderAPIResponse(BaseModel, Generic[PayloadT, ResponseT]):
    url: GenderAPIURL = Field(description="The URL used for the request")
    payload: PayloadT = Field(description="The payload used for the request")
    response: ResponseT = Field(description="The response from the request")


class GenderAPIError(Exception):
    def __init__(self, response_code: int, response_payload: str) -> None:
        super().__init__(f"GenderAPIError: {response_code}: {response_payload}")
        self.response_code = response_code
        self.response_payload = response_payload


class GenderAPI:
    """The interface for interacting with Gender-API.com. Acts as a
    async context manager, so you can use it with `async with`.

    This uses the V2 API: https://gender-api.com/en/api-docs/v2
    """

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        """The API Key: https://gender-api.com/en/account/auth-tokens"""

        self.session: Optional[aiohttp.ClientSession] = None
        """If this has been entered as an async context manager, this will be
        the aiohttp session
        """

    async def __aenter__(self) -> "GenderAPI":
        if self.session is not None:
            raise RuntimeError("GenderAPI is non-reentrant")

        self.session = aiohttp.ClientSession()
        await self.session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session is None:
            raise RuntimeError("not entered")

        sess = self.session
        self.session = None

        await sess.__aexit__(exc_type, exc_val, exc_tb)

    async def query_by_first_name(
        self, first_name: str, /, *, locale: Optional[str] = None
    ) -> GenderAPIResponse[
        GenderByFirstNamePayload,
        GenderByFirstNameResponse,
    ]:
        """Determines the gender associated with the given first name. The accuracy
        is imporved if a locale is provided, but it is not required.

        Args:
            first_name (str): The first name, stripped of whitespace, e.g., 'timothy'
            locale (Optional[str], optional): The locale, as an IETF BCP 47 language tag,
                e.g., 'en-US'. Defaults to None. May use underscores instead of dashes
                as a separator.
        """
        assert self.session is not None

        payload = GenderByFirstNamePayload(locale=locale, first_name=first_name)

        async with self.session.get(
            url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            data=payload.__pydantic_serializer__.to_json(payload),
        ) as resp:

            if not resp.ok:
                data = await resp.text()
                raise GenderAPIError(resp.status, data)

            data = await resp.read()

        logger.debug(f"gender-api.com using {payload=} got {data=}")
        parsed = GenderByFirstNameResponse.model_validate_json(data)
        return GenderAPIResponse(url=url, payload=payload, response=parsed)

    async def query_by_full_name(
        self, full_name: str, /, *, locale: Optional[str] = None
    ) -> GenderAPIResponse[GenderByFullNamePayload, GenderByFullNameResponse]:
        """Extracts the first name from the full name and then attempts to guess
        the gender associated with that first name. Note that the first name extraction
        is performed via GenderAPI, which will tend to be more accurate than a simple
        split on whitespace.

        Args:
            full_name (str): The full name, e.g., 'Timothy McTimothy'
            locale (Optional[str], optional): The locale, as an IETF BCP 47 language tag,
                e.g., 'en-US'. Defaults to None. May use underscores instead of dashes
                as a separator.
        """
        assert self.session is not None

        payload = GenderByFullNamePayload(locale=locale, full_name=full_name)

        async with self.session.get(
            url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            data=payload.__pydantic_serializer__.to_json(payload),
        ) as resp:

            if not resp.ok:
                data = await resp.text()
                raise GenderAPIError(resp.status, data)

            data = await resp.read()

        logger.debug(f"gender-api.com using {payload=} got {data=}")
        parsed = GenderByFullNameResponse.model_validate_json(data)
        return GenderAPIResponse(url=url, payload=payload, response=parsed)

    async def query_by_email_address(
        self, email: str, /, *, locale: Optional[str] = None
    ) -> GenderAPIResponse[GenderByEmailAddressPayload, GenderByEmailAddressResponse]:
        """Extracts a first name from the email address and then attempts to guess
        the gender associated with that first name. Note that the first name extraction
        is performed via GenderAPI and can handle common email address formats, but this
        is the most likely to return `result_found: False`

        Args:
            email (str): The email address, e.g., 'foo@example.com`
            locale (Optional[str], optional): The locale, as an IETF BCP 47 language tag,
                e.g., 'en-US'. Defaults to None. May use underscores instead of dashes
                as a separator.
        """
        assert self.session is not None

        payload = GenderByEmailAddressPayload(locale=locale, email=email)

        async with self.session.get(
            url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            data=payload.__pydantic_serializer__.to_json(payload),
        ) as resp:

            if not resp.ok:
                data = await resp.text()
                raise GenderAPIError(resp.status, data)

            data = await resp.read()

        logger.debug(f"gender-api.com using {payload=} got {data=}")
        parsed = GenderByEmailAddressResponse.model_validate_json(data)
        return GenderAPIResponse(url=url, payload=payload, response=parsed)
