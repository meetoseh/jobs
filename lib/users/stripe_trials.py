"""Handles determining if users are eligible for trials on stripe"""

from itgs import Itgs

STRIPE_TRIAL_RESET_TIME_SECONDS = 60 * 60 * 24 * 60
"""How long before you can use a Stripe trial again after using one"""


async def is_user_stripe_trial_eligible(
    itgs: Itgs, /, *, user_sub: str, now: float
) -> bool:
    """Determines if the given user is eligible for a trial on stripe.
    Often it makes sense to do this query alongside other queries, but
    this is still useful as a reference.
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")
    response = await cursor.execute(
        """
SELECT 1 FROM users, stripe_trials 
WHERE 
    users.sub = ? 
    AND stripe_trials.user_id = users.id 
    AND stripe_trials.subscription_created > ? 
LIMIT 1
        """,
        (user_sub, now - STRIPE_TRIAL_RESET_TIME_SECONDS),
    )
    return not response.results
