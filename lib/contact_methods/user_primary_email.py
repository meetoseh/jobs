"""Convenience module for getting a users primary email address. This is the
email address to use for display when there is only room for a single email
address. This address cannot be set directly but is consistent over the short
term.

SEE ALSO: `user_primary_phone` for the phone number equivalent.
"""
from typing import Optional
from pypika import Query, Table, Criterion, Not
from pypika.terms import ExistsCriterion
from lib.db.utils import CaseInsensitiveCriterion, ShieldFields


def primary_email_join_clause(
    *, users: Optional[Table] = None, user_email_addresses: Optional[Table] = None
) -> Criterion:
    """Returns the appropriate pypika join clause for joining users
    to user_email_addresses, but only the primary user email address.

    The resulting join will produce 0 or 1 rows and will always contain
    1 row if the user has at least one email address.

    This selects the email address associated with the user, preferring
    unsuppressed, then preferring verified, then preferring lower ids.

    In other words, within the following query:

    ```sql
    SELECT
        users.sub,
        user_email_addresses.email
    FROM users
    LEFT OUTER JOIN user_email_addresses ON (
        users.id = user_email_addresses.user_id
        AND NOT EXISTS (
            SELECT 1 FROM user_email_addresses AS uea
            WHERE
                uea.user_id = users.id
                AND uea.id != user_email_addresses.id
                AND (
                    EXISTS (
                        SELECT 1 FROM suppressed_emails AS se
                        WHERE se.email_address = user_email_addresses.email COLLATE NOCASE
                    ) < EXISTS (
                        SELECT 1 FROM suppressed_emails AS se
                        WHERE se.email_address = uea.email COLLATE NOCASE
                    )
                    OR (
                        EXISTS (
                            SELECT 1 FROM suppressed_emails AS se
                            WHERE se.email_address = user_email_addresses.email COLLATE NOCASE
                        ) = EXISTS (
                            SELECT 1 FROM suppressed_emails AS se
                            WHERE se.email_address = uea.email COLLATE NOCASE
                        )
                        AND (
                            user_email_addresses.verified < uea.verified
                            OR (
                                user_email_addresses.verified = uea.verified
                                AND user_email_addresses.id > uea.id
                            )
                        )
                    )
                )
        )
    )
    ```

    This produces the criterion within the on clause.

    Args:
        users (Table, None): If set, used as the users table. Usually
            used to alias the users table.
        user_email_addresses (Table, None): If set, used as the
            user_email_addresses table. Usually used to alias the
            user_email_addresses table.
    """
    if users is None:
        users = Table("users")
    if user_email_addresses is None:
        user_email_addresses = Table("user_email_addresses")

    inner_suppressed_emails = Table("suppressed_emails").as_("se")
    inner_user_email_addresses = Table("user_email_addresses").as_("uea")

    current = user_email_addresses
    alternative = inner_user_email_addresses

    current_is_suppressed = ExistsCriterion(
        Query.from_(inner_suppressed_emails)
        .select(1)
        .where(
            CaseInsensitiveCriterion(
                inner_suppressed_emails.email_address == current.email
            )
        )
    )

    alternative_is_suppressed = ExistsCriterion(
        Query.from_(inner_suppressed_emails)
        .select(1)
        .where(
            CaseInsensitiveCriterion(
                inner_suppressed_emails.email_address == alternative.email
            )
        )
    )

    return (users.id == current.user_id) & Not(
        ShieldFields(
            ExistsCriterion(
                Query.from_(alternative)
                .select(1)
                .where(
                    (alternative.user_id == users.id)
                    & (alternative.id != current.id)
                    & (
                        (current_is_suppressed < alternative_is_suppressed)
                        | (
                            (current_is_suppressed == alternative_is_suppressed)
                            & (
                                (current.verified < alternative.verified)
                                | (
                                    (current.verified == alternative.verified)
                                    & (current.id > alternative.id)
                                )
                            )
                        )
                    )
                )
            )
        )
    )


if __name__ == "__main__":
    from itgs import Itgs
    from pypika import Order
    import asyncio

    async def main():
        users = Table("users")
        user_email_addresses = Table("user_email_addresses")

        query = (
            Query.from_(users)
            .select(users.id, user_email_addresses.email)
            .join(user_email_addresses)
            .on(primary_email_join_clause())
            .orderby(users.id, order=Order.asc)
            .limit(5)
            .get_sql()
        )

        async with Itgs() as itgs:
            conn = await itgs.conn()
            cursor = conn.cursor("none")
            await cursor.execute(query)

    asyncio.run(main())
