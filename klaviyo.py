import json
from typing import AsyncIterator, List, Literal, Optional
import os
import aiohttp
from urllib.parse import urlencode
from error_middleware import handle_contextless_error
from dataclasses import dataclass
import asyncio


class DuplicateProfileError(Exception):
    def __init__(self, duplicate_profile_id: str) -> None:
        super().__init__(
            "A duplicate profile already exists with one of these identifiers: "
            + duplicate_profile_id
        )
        self.duplicate_profile_id = duplicate_profile_id


@dataclass
class ProfileListsResponse:
    items: List[str]
    """The list ids on this page of results"""

    next_uri: Optional[str]
    """If there are more results, the uri to use to fetch them"""


class Klaviyo:
    """The interface for interacting with Klaviyo. Acts as a
    async context manager, so you can use it with `async with`."""

    def __init__(self, api_key: str) -> None:
        self.api_key: str = api_key
        """The api key for authenticating with klaviyo"""

        self.session: Optional[aiohttp.ClientSession] = None
        """If this has been entered as an async context manager, this will be
        the aiohttp session
        """

    async def __aenter__(self) -> "Klaviyo":
        if self.session is not None:
            raise RuntimeError("Klaviyo is non-reentrant")

        self.session = aiohttp.ClientSession()
        await self.session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session is None:
            raise RuntimeError("not entered")

        sess = self.session
        self.session = None

        await sess.__aexit__(exc_type, exc_val, exc_tb)

    async def list_id(
        self,
        internal_id: Literal["users", "sms-morning", "sms-afternoon", "sms-evening"],
    ) -> str:
        """Fetches the list id from the given internal id. There are so many list ids
        and they change so often that using environment variables would be excessively
        tedious - we will probably eventually want an admin interface and database for
        this - hence this is async - but for now we store the values in code.

        Args:
            internal_id (str): The internal identifier for the list. Has the following
                possible values:
                - users: The list of users who have signed up for the app
                - sms-morning: The list of users who want to receive sms messages in the morning
                - sms-afternoon: The list of users who want to receive sms messages in the afternoon
                - sms-evening: The list of users who want to receive sms messages in the evening

        Returns:
            str: The list id
        """
        is_dev = os.environ["ENVIRONMENT"] == "dev"
        if internal_id == "users":
            return "U6UhZy" if is_dev else "R97Dmh"
        elif internal_id == "sms-morning":
            return "ShbEjR" if is_dev else "VFuuyw"
        elif internal_id == "sms-afternoon":
            return "Vap8nN" if is_dev else "TZnjyB"
        elif internal_id == "sms-evening":
            return "VsT9BR" if is_dev else "S4hbpG"

        raise ValueError(f"Unknown internal id: {internal_id}")

    async def suppress_email(self, email: str) -> None:
        """Suppresses the given email address from receiving emails from Klaviyo.
        If they do not have an account, an account is created and immediately suppressed.

        Args:
            email (str): The email address to suppress.
        """
        async with self.session.post(
            "https://a.klaviyo.com/api/profile-suppression-bulk-create-jobs/",
            json={
                "data": {
                    "type": "profile-suppression-bulk-create-job",
                    "attributes": {"suppressions": [{"email": email}]},
                }
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            response.raise_for_status()

    async def unsuppress_email(self, email: str) -> None:
        """Removes the suppression on the given email address.

        Args:
            email (str): The email address to unsuppress.
        """
        async with self.session.post(
            "https://a.klaviyo.com/api/profile-unsuppression-bulk-create-jobs/",
            json={
                "data": {
                    "type": "profile-unsuppression-bulk-create-job",
                    "attributes": {"suppressions": [{"email": email}]},
                }
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            response.raise_for_status()

    async def unsubscribe_from_list(
        self,
        *,
        emails: List[Optional[str]],
        phone_numbers: List[Optional[str]],
        list_id: str,
    ) -> None:
        """Unsubscribes the given email from the given klaviyo list

        Args:
            emails (list[str, None]): The email addresses to unsubscribe. Nones are
                filtered out and duplicates are removed.
            phone_numbers (list[str, None]): The phone numbers to unsubscribe. Nones are
                filtered out and duplicates are removed.
            list_id (str): The list id to unsubscribe from
        """
        emails = list(set([email for email in emails if email is not None]))
        phone_numbers = list(
            set(
                [
                    phone_number
                    for phone_number in phone_numbers
                    if phone_number is not None
                ]
            )
        )
        if not emails and not phone_numbers:
            return

        async with self.session.post(
            "https://a.klaviyo.com/api/profile-unsubscription-bulk-create-jobs/",
            json={
                "data": {
                    "type": "profile-unsubscription-bulk-create-job",
                    "attributes": {
                        "list_id": list_id,
                        "emails": emails,
                        "phone_numbers": phone_numbers,
                    },
                }
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            response.raise_for_status()

    async def request_profile_deletion(self, profile_id: str) -> None:
        """Requests that klaviyo permanently delete the profile with the given id.
        The delete is performed asynchronously, and all old entries will be replaced
        with emails that start with `redacted`, and an entry will be added to the
        deleted profiles page to show that we complied with the request.

        https://developers.klaviyo.com/en/reference/request_profile_deletion

        Args:
            profile_id (str): The klaviyo profile id to delete
        """
        async with self.session.post(
            "https://a.klaviyo.com/api/data-privacy-deletion-jobs/",
            json={
                "data": {
                    "type": "data-privacy-deletion-job",
                    "attributes": {"profile_id": profile_id},
                }
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            response.raise_for_status()

    async def create_profile(
        self,
        *,
        email: str,
        phone_number: Optional[str],
        external_id: str,
        first_name: Optional[str],
        last_name: Optional[str],
        timezone: Optional[str],
        environment: str,
    ) -> Optional[str]:
        """Creates a profile on klaviyo with the given data, returning the profile id.
        If the profile already exists, returns None.

        Args:
            email (str): The email address of the user
            phone_number (str, None): The phone number of the user, if known
            external_id (str): The external id to use, typically the user sub
            first_name (str, None): The first name of the user, if known
            last_name (str, None): The last name of the user, if known
            timezone (str, None): The timezone of the user, if known
            environment (str): The environment the user is in, e.g. dev, production

        Returns:
            str: The profile id
        """
        body = {
            "data": {
                "type": "profile",
                "attributes": {
                    "email": email,
                    "external_id": external_id,
                    **(
                        {"phone_number": phone_number}
                        if phone_number is not None
                        else {}
                    ),
                    **({"first_name": first_name} if first_name is not None else {}),
                    **({"last_name": last_name} if last_name is not None else {}),
                    **(
                        {"location": {"timezone": timezone}}
                        if timezone is not None
                        else {}
                    ),
                    "properties": {
                        "environment": environment,
                    },
                },
            }
        }

        async with self.session.post(
            "https://a.klaviyo.com/api/profiles/",
            json=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            if response.status == 409:
                return None

            if not response.ok:
                data = await response.text()
                await handle_contextless_error(
                    extra_info=f"body: ```\n{json.dumps(body)}\n```\nresponse:\n\n```\n{data}\n```"
                )
            response.raise_for_status()

            data = await response.json()
            return data["data"]["id"]

    async def get_profile_id(self, *, email: str) -> Optional[str]:
        """Gets the profile id for the given email address

        Args:
            email (str): The email address to look up

        Returns:
            str or None: The profile id, if found, otherwise None
        """
        async with self.session.get(
            "https://a.klaviyo.com/api/v2/people/search?"
            + urlencode({"api_key": self.api_key, "email": email}),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
            },
        ) as response:
            if response.status == 404:
                return None

            response.raise_for_status()

            data = await response.json()
            return data["id"]

    async def update_profile(
        self,
        *,
        profile_id: str,
        phone_number: Optional[str],
        environment: Optional[str] = None,
        email: Optional[str] = None,
        external_id: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        timezone: Optional[str] = None,
    ) -> None:
        """Updates the klaviyo profile with the given id to match the given data.

        One notable error that gets raised is 409 with the code `duplicate_profile`,
        in this case we raise a special exception that can be used to handle this.
        This typically happens if we have emails A and B, and phone numbers X and Y,
        where we are trying to get one account with AX, but there exists AY and BX.
        To fix this:
        - Remove the phone number from this account to make A, BX
        - Remove the phone number from the other account to make A, B
        - Add the phone number back to this account to make AX, B

        Args:
            profile_id (str): The profile id to update
            email (str, None): The email address of the user, None to keep the same
            phone_number (str, None): The phone number of the user, if known. Will overwrite
                the existing phone number even if None.
            external_id (str, None): The external id to use, typically the user sub, or None
                to keep the same
            first_name (str, None): The first name of the user, if known
            last_name (str, None): The last name of the user, if known
            timezone (str, None): The timezone of the user, if known
            environment (str, None): The environment the user is in, e.g. dev, production,
                or None to keep the same
        """
        body = {
            "data": {
                "type": "profile",
                "id": profile_id,
                "attributes": {
                    "phone_number": phone_number,
                    **({"email": email} if email is not None else {}),
                    **({"external_id": external_id} if external_id is not None else {}),
                    **({"first_name": first_name} if first_name is not None else {}),
                    **({"last_name": last_name} if last_name is not None else {}),
                    **(
                        {"location": {"timezone": timezone}}
                        if timezone is not None
                        else {}
                    ),
                    **(
                        {
                            "properties": {
                                "environment": environment,
                            }
                        }
                        if environment is not None
                        else {}
                    ),
                },
            }
        }

        async with self.session.patch(
            f"https://a.klaviyo.com/api/profiles/{profile_id}/",
            json=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            if not response.ok:
                data = await response.text()

                if response.status == 409:
                    data_parsed = json.loads(data)
                    if (
                        isinstance(data_parsed, dict)
                        and "errors" in data_parsed
                        and isinstance(data_parsed["errors"], list)
                    ):
                        for error in data_parsed["errors"]:
                            if (
                                isinstance(error, dict)
                                and "code" in error
                                and error["code"] == "duplicate_profile"
                            ):
                                raise DuplicateProfileError(
                                    error["meta"]["duplicate_profile_id"]
                                )

                await handle_contextless_error(
                    extra_info=f"body: ```\n{json.dumps(body)}\n```\nresponse:\n\n```\n{data}\n```"
                )
            response.raise_for_status()

    async def subscribe_profile_to_list(
        self,
        *,
        profile_id: str,
        email: Optional[str],
        phone_number: Optional[str],
        list_id: str,
    ) -> None:
        """Subscribes the given profile to the given list

        Args:
            profile_id (str): The profile id to subscribe; this is only used on
                klaviyo's side to speed up the process
            email (str, None): The email address to subscribe, if desired
            phone_number (str, None): The phone number to subscribe, if desired
            list_id (str): The list id to subscribe to
        """
        if email is None and phone_number is None:
            return

        body = {
            "data": {
                "type": "profile-subscription-bulk-create-job",
                "attributes": {
                    "list_id": list_id,
                    "custom_source": "Website",
                    "subscriptions": [
                        {
                            "profile_id": profile_id,
                            **({"email": email} if email is not None else {}),
                            **(
                                {"phone_number": phone_number}
                                if phone_number is not None
                                else {}
                            ),
                        }
                    ],
                },
            }
        }
        async with self.session.post(
            "https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs/",
            json=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            if not response.ok:
                data = await response.text()
                await handle_contextless_error(
                    extra_info=f"body: ```\n{json.dumps(body)}\n```\nresponse:\n\n```\n{data}\n```"
                )
            response.raise_for_status()

    async def get_profile_lists(
        self, *, profile_id: str, uri: Optional[str] = None
    ) -> ProfileListsResponse:
        """Gets the lists that the profile with the given id belongs to.

        Args:
            profile_id (str): The profile id to get lists for
            uri (str, None): If specified, should be the next_uri of a previous response.
                This is used for pagination.

        Returns:
            ProfileListsResponse: The profile ids and pagination info
        """
        request_uri = uri or (
            f"https://a.klaviyo.com/api/profiles/{profile_id}/lists/?"
            + urlencode({"fields[list]": "id"})
        )
        async with self.session.get(
            uri,
            headers={
                "Authorization": f"Klaviyo-API-Key {self.api_key}",
                "Accept": "application/json",
                "revision": "2023-02-22",
            },
        ) as response:
            data = await response.text()
            if not response.ok:
                await handle_contextless_error(
                    extra_info=f"{request_uri=} response:\n\n```\n{data}\n```"
                )
            response.raise_for_status()

            data_parsed: dict = json.loads(data)
            list_ids = [list_data["id"] for list_data in data_parsed["data"]]
            next_uri = data_parsed.get("links", {}).get("next", None)

            return ProfileListsResponse(list_ids=list_ids, next_uri=next_uri)

    async def get_profile_lists_auto_paginated(
        self, *, profile_id: str
    ) -> AsyncIterator[str]:
        """Gets the list ids that the profile with the given id belongs to,
        automatically paginating through all results. Sleeps 1 seconds between
        requests, does not sleep before the first request or after the last
        request.

        Args:
            profile_id (str): The profile id to get lists for

        Returns:
            AsyncIterator[str]: The list ids
        """
        next_uri = None
        while True:
            response = await self.get_profile_lists(profile_id=profile_id, uri=next_uri)
            for id in response.items:
                yield id
            next_uri = response.next_uri
            if next_uri is None:
                break
            await asyncio.sleep(1)
