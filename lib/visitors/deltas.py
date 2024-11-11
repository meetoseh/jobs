"""This module helps with generating utm conversion information. In particular, it determines
the changes required to produce the following lists.

Specifically, this will tell you exactly what entries need to be added or removed from
the following lists:

- `any_click_utms`: An entry is in this list if all of the following are true:
  - The utm and click is associated with any visitor of the given user
  - The utm and click was detected before the user was created
- `last_click_utms`: An entry is in this list if all of the following are true:
  - The utm and click is associated with any visitor of the given user
  - The utm and click was detected before the user was created
  - There is not a utm and click associated with any visitor of the given user
    which was detected after the utm and click in this entry
- `preexisting_utms`: An entry is in this list if all of the following are true:
  - The utm and click is associated with any visitor of the given user
  - The utm and click was detected after the user was created

This handles determining the updates to the list in two contexts - when a utm is
associated with a visitor, and when a user is associated with a visitor. 

When associating a utm with a visitor, the relevant information is related to each
user the visitor is associated with:

1. If the utm and click is before the user was created, add the utm to the
   `any_click_utms` list for the user. 
  a. If there are no other utm and clicks for the user, add the utm to the
     `last_click_utms` list for the user.
  b. If there are other utm and clicks for the user, let `old_last_click_utm`
     be the oldest of those which is still before the user was created. If
     the new utm and click is more recent than `old_last_click_utm`, remove
    `old_last_click_utm` from the `last_click_utms` list for the user and
    add the new utm and click to the `last_click_utms` list for the user.
2. Otherwise, when the utm and click is after the user was created, add the utm
   to the `preexisting_utms` list for the user.

When associating a user with a visitor, the relevant information is related to each
visitor the user is associated with, and for each visitor, each utm and click that
is associated with the visitor:

1. If the utm and click is before the user was created, add the utm to the
    `any_click_utms` list for the user.
    a. If there does not exist a utm and click for that visitor which is
       before the user was created and is more recent than the new utm and
       click, add the utm to the `last_click_utms` list for the user.
2. Otherwise, when the utm and click is after the user was created, add the utm
   to the `preexisting_utms` list for the user.

Note this definition means that a single click of a utm can be associated
with arbitrarily many signups so long as that same device was used to create
all those users.
"""

from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple
from itgs import Itgs
from lib.utms.parse import (
    UTM,
    get_canonical_utm_representation_from_wrapped,
    get_utm_parts,
)
import secrets
import logging
import pytz
import unix_dates


@dataclass
class UTMAndTime:
    """Describes a utm and the time it was clicked"""

    utm: UTM
    """The utm"""
    clicked_at: float
    """The time the utm was clicked"""


@dataclass
class UTMAndTimeAndRowUID:
    """Describes a utm, the time it was clicked, and the uid of the
    association row in visitor_utms
    """

    utm: UTM
    """The utm"""
    clicked_at: float
    """The time the utm was clicked"""
    visitor_utm_uid: str
    """The uid of the row in visitor_utms relating this utm and visitor, used
    for breaking ties on clicked_at
    """


@dataclass
class VisitorUserInfo:
    """Describes pre-existing state on a particular user for a visitor. This
    is used in the VisitorUTMState for inserting a new utm.
    """

    sub: str
    """The sub of the user"""

    visitor_user_uid: str
    """The uid of the row in visitor_users relating this user and visitor, used
    for breaking ties on created_at
    """

    created_at: float
    """When the user was created in seconds since the epoch"""

    last_click_utm: Optional[UTMAndTimeAndRowUID]
    """If the user has a last-click utm already associated with them, the
    utm and time it was clicked. Otherwise, None.
    """


@dataclass
class VisitorUTMState:
    """The state required to determine the changes for inserting a visitor utm association"""

    visitor_uid: str
    """The uid of the visitor this state is for"""

    visitor_version: int
    """The version of the visitor when the state was determined"""

    users: List[VisitorUserInfo]
    """The list of users the visitor is associated with, ordered arbitrarily"""

    new_visitor_utm_uid: str
    """The uid that will be used for the new visitor_utm record if one is/was inserted"""


@dataclass
class UserVisitorInfo:
    """Describes the pre-existing state on a particular visitor of a user.
    This is used in the VisitorUserState for inserting a new visitor.
    """

    visitor_uid: str
    """The uid of the visitor associated with the user"""

    visitor_version: int
    """The version of the visitor when the state was determined"""


@dataclass
class VisitorUserState:
    """The state required to determine the changes for inserting a visitor user association"""

    user_sub: str
    """The sub of the user this state is for"""

    user_created_at: float
    """When the user was created in seconds since the epoch"""

    previously_associated_visitors: List[UserVisitorInfo]
    """The list of visitors the user is associated with, in ascending order of visitor uid"""

    user_last_click_utm: Optional[UTMAndTimeAndRowUID]
    """If the user already has a last-click utm associated with them from one of their
    visitors, the utm, time it was clicked, and the association uid for breaking ties. 
    Otherwise, None.
    """

    visitor_uid: str
    """The uid of the visitor this state is for"""

    visitor_version: int
    """The version of the visitor when the state was determined"""

    visitor_attributable_clicks: List[UTMAndTimeAndRowUID]
    """If the visitor has any clicks, the list of utms and times they were clicked, in
    clicked_at asc, visitor_utm_uid asc order. Otherwise, an empty list. Only includes
    clicks from at or before when the user was created
    """

    visitor_unattributable_clicks: List[UTMAndTimeAndRowUID]
    """If the visitor has any clicks after the user was created, the list of utms and
    times they were clicked, in clicked_at asc, visitor_utm_uid asc order. Otherwise,
    an empty list.
    """


consistency_to_orderable = {
    "none": 0,
    "weak": 1,
    "strong": 2,
}


async def get_visitor_utm_state(
    itgs: Itgs,
    *,
    visitor_uid: str,
    consistency: Literal["none", "weak", "strong"],
    retry_on_fail: Literal["none", "weak", "strong"],
) -> Optional[VisitorUTMState]:
    """Determines the state of the visitor with the given uid, so that we can
    determine what will happen when a new utm is inserted.

    Args:
        itgs (Itgs): The integrations to (re)use
        visitor_uid (str): The uid of the visitor to get the state of
        consistency ('none', 'weak', or 'strong'): The read consistency to use for the
            database
        retry_on_fail ('none', 'weak', or 'strong'): Before returning None, if the
            consistency is weaker than this, retry with this consistency

    Returns:
        VisitorUTMState or None: The pertinent state of that visitor for associating a new
            utm, or None if there is no visitor with that uid
    """
    new_visitor_utm_uid = f"oseh_vutm_{secrets.token_urlsafe(16)}"

    conn = await itgs.conn()
    cursor = conn.cursor(consistency)

    response = await cursor.execute(
        """
        SELECT
            visitors.version,
            users.sub,
            visitor_users.uid,
            users.created_at,
            last_utms.canonical_query_param,
            last_visitor_utms.clicked_at,
            last_visitor_utms.uid
        FROM visitors
        LEFT OUTER JOIN visitor_users ON visitor_users.visitor_id = visitors.id
        LEFT OUTER JOIN users ON users.id = visitor_users.user_id
        LEFT OUTER JOIN visitor_utms AS last_visitor_utms ON (
            last_visitor_utms.visitor_id = visitors.id
            AND last_visitor_utms.clicked_at <= users.created_at
            AND NOT EXISTS (
                SELECT 1 FROM visitor_utms AS other_visitor_utms
                WHERE 
                    EXISTS (
                        SELECT 1 FROM visitor_users as other_visitor_users
                        WHERE other_visitor_users.user_id = visitor_users.user_id
                            AND other_visitor_users.visitor_id = other_visitor_utms.visitor_id
                    )
                    AND (
                        other_visitor_utms.clicked_at > last_visitor_utms.clicked_at
                        OR (
                            other_visitor_utms.clicked_at = last_visitor_utms.clicked_at
                            AND other_visitor_utms.uid > last_visitor_utms.uid
                        )
                    )
                    AND other_visitor_utms.clicked_at <= users.created_at
            )
        )
        LEFT OUTER JOIN utms AS last_utms ON last_utms.id = last_visitor_utms.utm_id
        WHERE visitors.uid = ?
        """,
        (visitor_uid,),
    )

    if not response.results:
        if (
            consistency_to_orderable[consistency]
            < consistency_to_orderable[retry_on_fail]
        ):
            return await get_visitor_utm_state(
                itgs,
                visitor_uid=visitor_uid,
                consistency=retry_on_fail,
                retry_on_fail=retry_on_fail,
            )
        return None

    visitor_version: int = response.results[0][0]
    users: List[VisitorUserInfo] = []

    if response.results[0][1] is None:
        return VisitorUTMState(
            visitor_uid=visitor_uid,
            visitor_version=visitor_version,
            users=users,
            new_visitor_utm_uid=new_visitor_utm_uid,
        )

    for row in response.results:
        user_sub: str = row[1]
        visitor_user_uid: str = row[2]
        created_at: float = row[3]
        last_click_utm: Optional[UTMAndTimeAndRowUID] = None
        if row[4] is not None and row[5] is not None and row[6] is not None:
            last_click_utm_utm = get_utm_parts(row[4])
            assert last_click_utm_utm is not None, row
            last_click_utm = UTMAndTimeAndRowUID(
                utm=last_click_utm_utm, clicked_at=row[5], visitor_utm_uid=row[6]
            )

        users.append(
            VisitorUserInfo(
                sub=user_sub,
                visitor_user_uid=visitor_user_uid,
                created_at=created_at,
                last_click_utm=last_click_utm,
            )
        )

    return VisitorUTMState(
        visitor_uid=visitor_uid,
        visitor_version=visitor_version,
        users=users,
        new_visitor_utm_uid=new_visitor_utm_uid,
    )


async def get_visitor_user_state(
    itgs: Itgs,
    *,
    user_sub: str,
    visitor_uid: str,
    consistency: Literal["none", "weak", "strong"],
    retry_on_fail: Literal["none", "weak", "strong"],
) -> Optional[VisitorUserState]:
    """Determines all of the required state for associating a new visitor with
    the given user.

    Args:
        itgs (Itgs): The integrations to (re)use
        user_sub (str): The sub of the user to get the state of
        visitor_uid (str): The uid of the visitor to get the state of
        consistency ('none', 'weak', or 'strong'): The read consistency to use for the
            database
        retry_on_fail ('none', 'weak', or 'strong'): Before returning None, if the
            consistency is weaker than this, retry with this consistency

    Returns:
        VisitorUserState or None: The state of the user, or None if the user
            doesn't exist or the visitor doesn't exist
    """
    conn = await itgs.conn()
    cursor = conn.cursor(consistency)

    response = await cursor.execute(
        """
        SELECT
            visitors.version,
            users.created_at,
            prev_visitors.uid,
            prev_visitors.version,
            prev_last_click_visitor_utms.uid,
            prev_last_click_visitor_utms.clicked_at,
            prev_last_click_utms.canonical_query_param
        FROM visitors, users
        LEFT OUTER JOIN visitor_users ON visitor_users.user_id = users.id
        LEFT OUTER JOIN visitors AS prev_visitors ON prev_visitors.id = visitor_users.visitor_id
        LEFT OUTER JOIN visitor_utms AS prev_last_click_visitor_utms ON (
            prev_last_click_visitor_utms.visitor_id = prev_visitors.id
            AND prev_last_click_visitor_utms.clicked_at <= users.created_at
            AND NOT EXISTS (
                SELECT 1 FROM visitor_utms AS other_visitor_utms
                WHERE
                    other_visitor_utms.visitor_id = prev_visitors.id
                    AND (
                        other_visitor_utms.clicked_at > prev_last_click_visitor_utms.clicked_at
                        OR (
                            other_visitor_utms.clicked_at = prev_last_click_visitor_utms.clicked_at
                            AND other_visitor_utms.uid > prev_last_click_visitor_utms.uid
                        )
                    )
            )
        )
        LEFT OUTER JOIN utms AS prev_last_click_utms ON prev_last_click_utms.id = prev_last_click_visitor_utms.utm_id
        WHERE visitors.uid = ? AND users.sub = ?
        """,
        (visitor_uid, user_sub),
    )

    if not response.results:
        if (
            consistency_to_orderable[consistency]
            < consistency_to_orderable[retry_on_fail]
        ):
            return await get_visitor_user_state(
                itgs,
                user_sub=user_sub,
                visitor_uid=visitor_uid,
                consistency=retry_on_fail,
                retry_on_fail=retry_on_fail,
            )

        return None

    visitor_version: int = response.results[0][0]
    user_created_at: float = response.results[0][1]
    previously_associated_visitors: List[UserVisitorInfo] = []
    user_last_click_utm: Optional[UTMAndTimeAndRowUID] = None
    for row in response.results:
        if row[2] is None:
            continue

        prev_visitor_uid: str = row[2]
        prev_visitor_version: int = row[3]
        if row[4] is not None and row[5] is not None and row[6] is not None:
            prev_last_click_utm_utm = get_utm_parts(row[6])
            assert prev_last_click_utm_utm is not None, row
            prev_last_click_utm = UTMAndTimeAndRowUID(
                utm=prev_last_click_utm_utm, clicked_at=row[5], visitor_utm_uid=row[4]
            )

            if user_last_click_utm is None or (
                prev_last_click_utm.clicked_at > user_last_click_utm.clicked_at
                or (
                    prev_last_click_utm.clicked_at == user_last_click_utm.clicked_at
                    and prev_last_click_utm.visitor_utm_uid
                    > user_last_click_utm.visitor_utm_uid
                )
            ):
                user_last_click_utm = prev_last_click_utm

        previously_associated_visitors.append(
            UserVisitorInfo(
                visitor_uid=prev_visitor_uid,
                visitor_version=prev_visitor_version,
            )
        )

    response = await cursor.execute(
        """
        SELECT
            visitor_utms.uid,
            visitor_utms.clicked_at,
            utms.canonical_query_param
        FROM visitor_utms, utms
        WHERE
            EXISTS (
                SELECT 1 FROM visitors
                WHERE 
                    visitors.id = visitor_utms.visitor_id 
                    AND visitors.uid = ?
                    AND visitors.version = ?
            )
            AND utms.id = visitor_utms.utm_id
        ORDER BY visitor_utms.clicked_at ASC, visitor_utms.uid ASC
        """,
        (
            visitor_uid,
            visitor_version,
        ),
    )

    visitor_attributable_clicks: List[UTMAndTimeAndRowUID] = []
    visitor_unattributable_clicks: List[UTMAndTimeAndRowUID] = []
    for row in response.results or []:
        row_utm = get_utm_parts(row[2])
        assert row_utm is not None
        parsed = UTMAndTimeAndRowUID(
            utm=row_utm, clicked_at=row[1], visitor_utm_uid=row[0]
        )
        if parsed.clicked_at <= user_created_at:
            visitor_attributable_clicks.append(parsed)
        else:
            visitor_unattributable_clicks.append(parsed)

    return VisitorUserState(
        user_sub=user_sub,
        user_created_at=user_created_at,
        previously_associated_visitors=previously_associated_visitors,
        user_last_click_utm=user_last_click_utm,
        visitor_uid=visitor_uid,
        visitor_version=visitor_version,
        visitor_attributable_clicks=visitor_attributable_clicks,
        visitor_unattributable_clicks=visitor_unattributable_clicks,
    )


def _visitor_user_state_clause(state: VisitorUserState) -> Tuple[str, list]:
    """Creates the sql clause and corresponding ordered arguments to verify
    the given state is still valid. This requires that `users` is already joined
    and corresponds to the user with the given sub, and that `visitors` is already
    joined and corresponds to the visitor with the given uid.

    Args:
        state (VisitorUserState): The state to verify

    Returns:
        (str, list): The sql clause and corresponding ordered arguments
    """
    result_parts: List[Tuple[str, list]] = [
        (
            "visitors.version = ?",
            [state.visitor_version],
        ),
        (
            "? = (SELECT COUNT(*) FROM visitor_users AS vu_inner WHERE vu_inner.user_id = users.id)",
            [len(state.previously_associated_visitors)],
        ),
    ]

    for visitor in state.previously_associated_visitors:
        result_parts.append(
            (
                "EXISTS (SELECT 1 FROM visitor_users AS vu_inner, visitors AS v_inner WHERE vu_inner.user_id = users.id AND v_inner.id = vu_inner.visitor_id AND v_inner.uid = ? AND v_inner.version = ?)",
                [visitor.visitor_uid, visitor.visitor_version],
            )
        )

    return " AND ".join([part[0] for part in result_parts]), [
        arg for part in result_parts for arg in part[1]
    ]


async def try_insert_visitor_utm(
    itgs: Itgs, *, state: VisitorUTMState, utm: UTM, clicked_at: float
) -> bool:
    """Inserts a new UTM into the database for the visitor with the given
    state, but only if the visitor state would still be correct immediately
    before the insert (in a concurrent-safe manner).

    Args:
        itgs (Itgs): The integrations to (re)use
        state (VisitorUTMState): The current state of the visitor as fetched
            for a utm insertion
        utm (UTM): The utm to insert
        clicked_at (float): The time the utm was clicked

    Returns:
        (bool): True if the utm was inserted, False otherwise
    """
    canonical_utm = get_canonical_utm_representation_from_wrapped(utm)

    conn = await itgs.conn()
    cursor = conn.cursor()

    response = await cursor.executemany3(
        (
            (
                """
                INSERT INTO utms (
                    uid, canonical_query_param, verified, utm_source, utm_medium, 
                    utm_campaign, utm_term, utm_content, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (canonical_query_param) DO NOTHING
                """,
                (
                    f"oseh_utm_{secrets.token_urlsafe(16)}",
                    canonical_utm,
                    0,
                    utm.source,
                    utm.medium,
                    utm.campaign,
                    utm.term,
                    utm.content,
                    clicked_at,
                ),
            ),
            (
                """
                INSERT INTO visitor_utms (
                    uid, visitor_id, utm_id, clicked_at
                )
                SELECT ?, visitors.id, utms.id, ?
                FROM visitors, utms
                WHERE
                    visitors.uid = ?
                    AND visitors.version = ?
                    AND utms.canonical_query_param = ?
                """,
                (
                    state.new_visitor_utm_uid,
                    clicked_at,
                    state.visitor_uid,
                    state.visitor_version,
                    canonical_utm,
                ),
            ),
            (
                """
                UPDATE visitors
                SET version=version+1
                WHERE
                    visitors.uid = ?
                    AND EXISTS (
                        SELECT 1 FROM visitor_utms
                        WHERE visitor_utms.uid = ?
                    )
                """,
                (state.visitor_uid, state.new_visitor_utm_uid),
            ),
        )
    )

    pretty_clicked_at = unix_dates.unix_timestamp_to_datetime(
        clicked_at, tz=pytz.timezone("America/Los_Angeles")
    )
    if response[0].rows_affected is not None and response[0].rows_affected > 0:
        logging.info(
            f"Inserted new utm {canonical_utm} while processing click for visitor {state.visitor_uid}"
        )

    if response[1].rows_affected is not None and response[1].rows_affected > 0:
        logging.debug(
            f"Inserted new utm click by visitor {state.visitor_uid} "
            f"at {pretty_clicked_at} on utm {canonical_utm}"
        )
        assert (
            response[2].rows_affected is not None and response[2].rows_affected > 0
        ), "Failed to update visitor version"
        return True

    logging.debug(
        f"Failed to insert new utm click by visitor {state.visitor_uid} "
        f"on utm {canonical_utm} at {pretty_clicked_at} (state is stale)"
    )
    return False


async def try_upsert_visitor_user(
    itgs: Itgs,
    *,
    state: VisitorUserState,
    visitor_uid: str,
    user_sub: str,
    seen_at: float,
) -> Literal["inserted", "updated", "failed"]:
    """Attempts to upsert that the the given visitor is associated with the given user
    at the given time. If the visitor has already been associated with the given user,
    this updates `last_seen_at`, otherwise this inserts a row into `visitor_users`.

    This is a concurrent-safe operation, and will only succeed if the visitor state
    is still valid immediately before the insert.

    Args:
        itgs (Itgs): The integrations to (re)use
        state (VisitorUserState): The users associated with the visitor already
        visitor_uid (str): The uid of the visitor
        user_sub (str): The sub of the user
        seen_at (float): The time the user was seen

    Returns:
        ('inserted', 'updated', or 'failed'): Whether the upsert succeeded or failed,
          and if it succeeded, whether it inserted or updated
    """
    valid_clause, valid_clause_args = _visitor_user_state_clause(state)

    conn = await itgs.conn()
    cursor = conn.cursor()

    new_vu_uid = f"oseh_vu_{secrets.token_urlsafe(16)}"
    response = await cursor.executemany3(
        (
            (
                f"""
                INSERT INTO visitor_users (
                    uid, user_id, visitor_id, first_seen_at, last_seen_at
                )
                SELECT
                    ?, users.id, visitors.id, ?, ?
                FROM users, visitors
                WHERE
                    users.sub = ?
                    AND visitors.uid = ?
                    AND {valid_clause}
                ON CONFLICT (user_id, visitor_id) DO UPDATE SET last_seen_at = max(?, last_seen_at)
                """,
                (
                    new_vu_uid,
                    seen_at,
                    seen_at,
                    user_sub,
                    visitor_uid,
                    *valid_clause_args,
                    seen_at,
                ),
            ),
            (
                """
                UPDATE visitors
                SET version=version+1
                WHERE
                    visitors.uid = ?
                    AND EXISTS (
                        SELECT 1 FROM visitor_users
                        WHERE visitor_users.uid = ?
                    )
                """,
                (visitor_uid, new_vu_uid),
            ),
        )
    )

    tz = pytz.timezone("America/Los_Angeles")
    seen_at_pretty = unix_dates.unix_timestamp_to_datetime(seen_at, tz=tz)
    if response[1].rows_affected is not None and response[1].rows_affected > 0:
        logging.debug(
            f"Inserted that visitor {visitor_uid} is associated "
            f"with user {user_sub} at {seen_at_pretty} ({seen_at})"
        )
        return "inserted"

    if response[0].rows_affected is not None and response[0].rows_affected > 0:
        logging.debug(
            "Updated that we have seen the relationship between "
            f"{visitor_uid} and {user_sub} at {seen_at_pretty} ({seen_at})"
        )
        return "updated"

    logging.debug(
        f"Failed to insert or update that visitor {visitor_uid} is associated "
        f"with user {user_sub} at {seen_at_pretty} ({seen_at}) (state is stale)"
    )
    return "failed"


@dataclass
class VisitorChangesFromUTM:
    """Describes the changes to statistics as a result of a new
    visitor utm association being processed
    """

    last_clicks_changed_to_any_click: List[UTMAndTime]
    """The utm and times to decrement from the last-click utm count.

    Intended result:
    - Decrement `stats:visitors:daily:{utm}:{unix_date}:counts` `last_click_signups`
      or `holdover_last_click_signups` as appropriate by 1
    """

    new_last_click: List[UTMAndTime]
    """The the new last-click utm and times.

    Intended result:
    - Increment `stats:visitors:daily:{utm}:{unix_date}:counts` `last_click_signups`
      or `holdover_last_click_signups` as appropriate by 1
    """

    new_any_click: List[UTMAndTime]
    """The new any-click utm and times. Note that whenever this is always at least
    as many items as new_last_click, since any new last clicks are also new any clicks.

    Intended result:
    - Increment `stats:visitors:daily:{utm}:{unix_date}:counts` `any_click_signups`
      or `holdover_any_click_signups` as appropriate by 1
    """

    new_preexisting: List[UTMAndTime]
    """The new pre-existing utm and times.

    Intended result:
    - Increment `stats:visitors:daily:{utm}:{unix_date}:counts` `preexisting`
      or `holdover_preexisting` as appropriate by 1
    """


def compute_changes_from_visitor_utm(
    state: VisitorUTMState, utm: UTM, clicked_at: float
) -> VisitorChangesFromUTM:
    """Computes what signup statistics changed as a result of the visitor in the
    given state clicking the given utm.

    Args:
        state (VisitorState): The current state of the visitor
        utm (UTM): The utm that was clicked
        clicked_at (float): The time the utm was clicked
    """
    result = VisitorChangesFromUTM(
        last_clicks_changed_to_any_click=[],
        new_last_click=[],
        new_any_click=[],
        new_preexisting=[],
    )

    for user in state.users:
        if user.created_at < clicked_at:
            result.new_preexisting.append(UTMAndTime(utm=utm, clicked_at=clicked_at))
            continue

        if user.last_click_utm is None:
            result.new_last_click.append(UTMAndTime(utm=utm, clicked_at=clicked_at))
            result.new_any_click.append(UTMAndTime(utm=utm, clicked_at=clicked_at))
            continue

        if user.last_click_utm.clicked_at > clicked_at or (
            user.last_click_utm.clicked_at == clicked_at
            and user.last_click_utm.visitor_utm_uid > state.new_visitor_utm_uid
        ):
            # existing last click is more recent
            result.new_any_click.append(UTMAndTime(utm=utm, clicked_at=clicked_at))
            continue

        result.last_clicks_changed_to_any_click.append(
            UTMAndTime(
                utm=user.last_click_utm.utm, clicked_at=user.last_click_utm.clicked_at
            )
        )
        result.new_last_click.append(UTMAndTime(utm=utm, clicked_at=clicked_at))
        result.new_any_click.append(UTMAndTime(utm=utm, clicked_at=clicked_at))

    logging.debug(
        f"Result of associating visitor {state} with {utm} and {clicked_at}: {result}"
    )
    return result


@dataclass
class VisitorChangesFromUser:
    """Describes the changes to statistics as a result of a visitor
    user association being processed.
    """

    last_clicks_changed_to_any_click: List[UTMAndTime]
    """UTMs that lost the last-click status and are now just any-click signups.

    Intended result:
    - Decrement `stats:visitors:daily:{utm}:{unix_date}:counts` `last_click_signups`
      or `holdover_last_click_signups` as appropriate by 1
    """

    new_any_clicks: List[UTMAndTime]
    """UTMs that are now any-click signups as they occurred prior to this user
    being created.

    Intended result:
    - Increment `stats:visitors:daily:{utm}:{unix_date}:counts` `any_click_signups`
      or `holdover_any_click_signups` as appropriate by 1
    """

    new_last_clicks: List[UTMAndTime]
    """UTMs that are now last-click signups as they occurred prior to this user
    being created and there is no more recent click to attribute the signup to.
    
    Intended result:
    - Increment `stats:visitors:daily:{utm}:{unix_date}:counts` `last_click_signups`
      or `holdover_last_click_signups` as appropriate by 1
    """

    new_preexisting: List[UTMAndTime]
    """UTMs that are now preexisting as they occurred after this user being
    created on a visitor associated with this user.

    Intended result:
    - Increment `stats:visitors:daily:{utm}:{unix_date}:counts` `preexisting`
        or `holdover_preexisting` as appropriate by 1
    """


def compute_changes_from_visitor_user(
    state: VisitorUserState,
) -> VisitorChangesFromUser:
    """Determines how our statistics should be updated when a user is associated
    with a visitor for the first time.

    Args:
        state (VisitorUserState): The state of the user and visitor being
            associated

    Returns:
        VisitorChangesFromUser: The changes to make to the statistics
    """
    result = VisitorChangesFromUser(
        last_clicks_changed_to_any_click=[],
        new_any_clicks=[],
        new_last_clicks=[],
        new_preexisting=[],
    )

    if (
        state.user_last_click_utm is not None
        and state.visitor_attributable_clicks
        and (
            state.visitor_attributable_clicks[-1].clicked_at
            > state.user_last_click_utm.clicked_at
            or (
                state.visitor_attributable_clicks[-1].clicked_at
                == state.user_last_click_utm.clicked_at
                and state.visitor_attributable_clicks[-1].visitor_utm_uid
                > state.user_last_click_utm.visitor_utm_uid
            )
        )
    ):
        result.last_clicks_changed_to_any_click.append(
            UTMAndTime(
                utm=state.user_last_click_utm.utm,
                clicked_at=state.user_last_click_utm.clicked_at,
            )
        )
        result.new_last_clicks.append(
            UTMAndTime(
                utm=state.visitor_attributable_clicks[-1].utm,
                clicked_at=state.visitor_attributable_clicks[-1].clicked_at,
            )
        )
    elif state.user_last_click_utm is None and state.visitor_attributable_clicks:
        result.new_last_clicks.append(
            UTMAndTime(
                utm=state.visitor_attributable_clicks[-1].utm,
                clicked_at=state.visitor_attributable_clicks[-1].clicked_at,
            )
        )

    for click in state.visitor_attributable_clicks:
        result.new_any_clicks.append(
            UTMAndTime(
                utm=click.utm,
                clicked_at=click.clicked_at,
            )
        )

    for click in state.visitor_unattributable_clicks:
        result.new_preexisting.append(
            UTMAndTime(
                utm=click.utm,
                clicked_at=click.clicked_at,
            )
        )

    logging.debug(f"Result of associating visitor/user in {state}: {result}")
    return result
