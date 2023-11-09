"""Helper module for writing to contact method stats in redis"""
from typing import Literal, Optional
from itgs import Itgs
from lib.redis_stats_preparer import RedisStatsPreparer


class ContactMethodStatsPreparer:
    """A basic helper class for updating contact method stats"""

    def __init__(self, stats: RedisStatsPreparer):
        self.stats = stats
        """The base stats object to modify"""

    def incr_contact_methods(
        self,
        unix_date: int,
        event: str,
        *,
        event_extra: Optional[bytes] = None,
        amt: int = 1,
    ):
        """Updates the given event in stats:contact_methods:daily:{unix_date}.
        Not intended to be used directly; instead, prefer the more specific
        variants which will check the types for you
        """
        self.stats.incrby(
            unix_date=unix_date,
            basic_key_format="stats:contact_methods:daily:{unix_date}",
            earliest_key=b"stats:contact_methods:daily:earliest",
            event=event,
            event_extra_format="stats:contact_methods:daily:{unix_date}:extra:{event}",
            event_extra=event_extra,
            amt=amt,
        )

    def incr_created(
        self,
        unix_date: int,
        *,
        channel: Literal["email", "phone", "push"],
        verified: bool,
        enabled: bool,
        reason: str,
        amt: int = 1,
    ):
        """Called when a contact method with the given properties was associated
        with a user.

        Args:
            unix_date (int): the unix date the action occurred
            channel ("email", "phone", or "push"): describes what type of contact
                method was associated
            verified (bool): True if the contact method is initially verified,
                false if initially unverified. Irrelevant for the push channel,
                since they are only ever transiently valid.
            enabled (bool): True if the contact method is initially enabled,
                false if initially disabled. This is referring to if notifications
                of any kind can be sent to the contact method without asking the
                user first. Suppression is used to describe a contact method that
                we cannot reach for a user.
            reason (str): The reason, which depends on the channel. See
                `contact_method_stats` documentation for details. Will be verified,
                so this implementation is also a way to see what reasons can be
                used
            amt (int): the amount to increment by. Defaults to 1
        """
        assert channel in ("email", "phone", "push"), channel
        assert isinstance(verified, bool), verified
        assert isinstance(enabled, bool), enabled
        assert isinstance(reason, str), reason

        if channel == "email":
            assert reason == "identity", reason
        elif channel == "phone":
            assert reason in ("identity", "verify"), reason
        elif channel == "push":
            assert reason == "app", reason

        event_extra_parts = [channel]
        if channel != "push":
            event_extra_parts.append("verified" if verified else "unverified")
        event_extra_parts.append("enabled" if enabled else "disabled")
        event_extra_parts.append(reason)
        self.incr_contact_methods(
            unix_date,
            "created",
            event_extra=(":".join(event_extra_parts)).encode("utf-8"),
            amt=amt,
        )

    def incr_deleted(
        self,
        unix_date: int,
        *,
        channel: Literal["email", "phone", "push"],
        reason: str,
        amt: int = 1,
    ):
        """Called when a contact method has been disassociated with a user.

        Args:
            unix_date (int): the unix date the action occurred
            channel ("email", "phone", or "push"): describes what type of contact
                method was associated
            reason (str): The reason, which depends on the channel. See
                `contact_method_stats` documentation for details. Will be verified,
                so this implementation is also a way to see what reasons can be
                used
        """
        assert channel in ("email", "phone", "push"), channel
        assert isinstance(reason, str), reason

        if channel == "email":
            assert reason == "account", reason
        elif channel == "phone":
            assert reason == "account", reason
        elif channel == "push":
            assert reason in (
                "account",
                "reassigned",
                "excessive",
                "device_not_registered",
            ), reason

        self.incr_contact_methods(
            unix_date,
            "deleted",
            event_extra=f"{channel}:{reason}".encode("utf-8"),
            amt=amt,
        )

    def incr_verified(
        self,
        unix_date: int,
        *,
        channel: Literal["email", "phone"],
        reason: str,
        amt: int = 1,
    ):
        """Called when a contact method already associated with a user changes
        from unverified to verified

        Args:
            unix_date (int): the unix date the action occurred
            channel ("email", "phone"): describes what type of contact
                method was verified
            reason (str): The reason, which depends on the channel. See
                `contact_method_stats` documentation for details. Will be verified,
                so this implementation is also a way to see what reasons can be
                used
            amt (int): the amount to increment by. Defaults to 1
        """
        assert channel in ("email", "phone"), channel
        assert isinstance(reason, str), reason

        if channel == "email":
            assert reason == "identity", reason
        elif channel == "phone":
            assert reason in ("identity", "verify", "sms_start"), reason

        self.incr_contact_methods(
            unix_date,
            "verified",
            event_extra=f"{channel}:{reason}".encode("utf-8"),
            amt=amt,
        )

    def incr_enabled(
        self,
        unix_date: int,
        *,
        channel: Literal["email", "phone", "push"],
        reason: str,
        amt: int = 1,
    ):
        """Called when a contact method already associated with a user changes
        from disabled to enabled, meaning we can contact it without asking the
        user first (i.e., in passive flows like daily reminders)

        Args:
            unix_date (int): the unix date the action occurred
            channel ("email", "phone", or "push"): describes what type of contact
                method was enabled
            reason (str): The reason, which depends on the channel. See
                `contact_method_stats` documentation for details. Will be verified,
                so this implementation is also a way to see what reasons can be
                used
            amt (int): the amount to increment by. Defaults to 1
        """
        assert channel in ("email", "phone", "push"), channel
        assert isinstance(reason, str), reason

        if channel == "email":
            assert False, "there is currently no user flow that enables email"
        elif channel == "phone":
            assert reason in ("verify", "sms_start"), reason
        elif channel == "push":
            assert False, "there is currently no user flow that enables push"

        self.incr_contact_methods(
            unix_date,
            "enabled",
            event_extra=f"{channel}:{reason}".encode("utf-8"),
            amt=amt,
        )

    def incr_disabled(
        self,
        unix_date: int,
        *,
        channel: Literal["email", "phone", "push"],
        reason: str,
        amt: int = 1,
    ):
        """Called when a contact method already associated with a user changes
        from enabled to disabled, meaning we cannot contact it without asking the
        user first (i.e., in passive flows like daily reminders), but we might
        still use it after a prompt for e.g. 2FA

        Args:
            unix_date (int): the unix date the action occurred
            channel ("email", "phone", or "push"): describes what type of contact
                method was disabled
            reason (str): The reason, which depends on the channel. See
                `contact_method_stats` documentation for details. Will be verified,
                so this implementation is also a way to see what reasons can be
                used
            amt (int): the amount to increment by. Defaults to 1
        """
        assert channel in ("email", "phone", "push"), channel
        assert isinstance(reason, str), reason

        if channel in ("email", "push"):
            assert reason == "unsubscribe", reason
        elif channel == "phone":
            assert reason in ("unsubscribe", "verify", "dev_auto_disable"), reason

        self.incr_contact_methods(
            unix_date,
            "disabled",
            event_extra=f"{channel}:{reason}".encode("utf-8"),
            amt=amt,
        )


class contact_method_stats:
    def __init__(self, itgs: Itgs) -> None:
        """An alternative simple interface for using ContactMethodStats stats which
        provides it as a context manager, storing it on exit.
        """
        self.itgs = itgs
        self.stats: Optional[ContactMethodStatsPreparer] = None

    async def __aenter__(self) -> ContactMethodStatsPreparer:
        assert self.stats is None
        self.stats = ContactMethodStatsPreparer(RedisStatsPreparer())
        return self.stats

    async def __aexit__(self, *args) -> None:
        assert self.stats is not None
        await self.stats.stats.store(self.itgs)
        self.stats = None
