"""Convenience module for getting a users primary phone number. This is the
phone number to use for display when there is only room for a single phone
number. This cannot be set directly but is consistent over the short term.

SEE ALSO: `user_primary_email` for the email address equivalent.
"""

import asyncio
from itgs import Itgs
from typing import Optional
from pypika import Query, Table, Criterion, Not, Order
from pypika.terms import ExistsCriterion


def primary_phone_join_clause(
    *, users: Optional[Table] = None, user_phone_numbers: Optional[Table] = None
) -> Criterion:
    """Returns the appropriate pypika join clause for joining users
    to user_phone_numbers, but only the primary phone number.

    The resulting join will produce 0 or 1 rows and will always contain
    1 row if the user has at least one phone number.

    This selects the phone number associated with the user, preferring
    unsuppressed, then preferring verified, then preferring lower ids.

    In other words, within the following query:

    ```sql
    SELECT
        users.sub,
        user_phone_numbers.phone_number
    FROM users
    LEFT OUTER JOIN user_phone_numbers ON (
        users.id = user_phone_numbers.user_id
        AND NOT EXISTS (
            SELECT 1 FROM user_phone_numbers AS upn
            WHERE
                upn.user_id = users.id
                AND upn.id != user_phone_numbers.id
                AND (
                    EXISTS (
                        SELECT 1 FROM suppressed_phone_numbers AS spn
                        WHERE spn.phone_number = user_phone_numbers.phone_number
                    ) < EXISTS (
                        SELECT 1 FROM suppressed_phone_numbers AS spn
                        WHERE spn.phone_number = upn.phone_number
                    )
                    OR (
                        EXISTS (
                            SELECT 1 FROM suppressed_phone_numbers AS spn
                            WHERE spn.phone_number = user_phone_numbers.phone_number
                        ) = EXISTS (
                            SELECT 1 FROM suppressed_phone_numbers AS spn
                            WHERE spn.phone_number = upn.phone_number
                        )
                        AND (
                            user_phone_numbers.verified < upn.verified
                            OR (
                                user_phone_numbers.verified = upn.verified
                                AND user_phone_numbers.id > upn.id
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
        user_phone_numbers (Table, None): If set, used as the
            user_phone_numbers table. Usually used to alias the
            user_phone_numbers table.
    """
    if users is None:
        users = Table("users")
    if user_phone_numbers is None:
        user_phone_numbers = Table("user_phone_numbers")

    inner_suppressed_phone_numbers = Table("suppressed_phone_numbers").as_("spn")
    inner_user_phone_numbers = Table("user_phone_numbers").as_("upn")

    current = user_phone_numbers
    alternative = inner_user_phone_numbers

    current_is_suppressed = ExistsCriterion(
        Query.from_(inner_suppressed_phone_numbers)
        .select(1)
        .where((inner_suppressed_phone_numbers.phone_number == current.phone_number))
    )

    alternative_is_suppressed = ExistsCriterion(
        Query.from_(inner_suppressed_phone_numbers)
        .select(1)
        .where(
            (inner_suppressed_phone_numbers.phone_number == alternative.phone_number)
        )
    )

    return (users.id == current.user_id) & Not(
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


if __name__ == "__main__":

    async def main():
        users = Table("users")
        user_phone_numbers = Table("user_phone_numbers")

        query = (
            Query.from_(users)
            .select(users.id, user_phone_numbers.phone_number)
            .join(user_phone_numbers)
            .on(primary_phone_join_clause())
            .orderby(users.id, order=Order.asc)
            .limit(5)
            .get_sql()
        )

        async with Itgs() as itgs:
            conn = await itgs.conn()
            cursor = conn.cursor("none")
            await cursor.execute(query)

    asyncio.run(main())
