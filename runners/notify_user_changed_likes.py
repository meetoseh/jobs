import os
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
from lib.shared.clean_for_slack import clean_for_non_code_slack
from lib.shared.describe_user import describe_user

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, liked: bool, user_sub: str, journey_uid: str
):
    """Notifies slack that the user changed their like status on the given journey. This
    can either be adding the journey to their liked journeys, or removing it.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        liked (bool): true if the user now likes the journey, false if they don't
        user_sub (str): the sub of the user who changed their like status
        journey_uid (str): the uid of the journey that was liked or unliked
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    user = await describe_user(itgs, user_sub)
    if user is None:
        await handle_contextless_error(
            extra_info=f"`notify_user_changed_likes`: no corresponding `{user_sub=}`"
        )
        return

    response = await cursor.execute(
        """
        SELECT
            journeys.title
        FROM journeys
        WHERE
            journeys.uid = ?
        """,
        (journey_uid,),
    )
    if not response.results:
        await handle_contextless_error(
            extra_info=f"`notify_user_changed_likes`: no corresponding `{journey_uid=}`"
        )
        return

    title: str = response.results[0][0]

    message = (
        f"{{name}} favorited {clean_for_non_code_slack(title)}"
        if liked
        else f"{{name}} unfavorited {clean_for_non_code_slack(title)}"
    )

    blocks = [await user.make_slack_block(itgs, message)]
    logging.info(f"notify_user_changed_likes: {blocks}")
    if os.environ["ENVIRONMENT"] != "production":
        return

    slack = await itgs.slack()
    await slack.send_oseh_bot_blocks(
        blocks, preview=message.format(name=user.name_equivalent)
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        liked = input("liked (y/n): ").lower().startswith("y")
        user_sub = input("user sub: ")
        journey_uid = input("journey uid: ")
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.notify_user_changed_likes",
                liked=liked,
                user_sub=user_sub,
                journey_uid=journey_uid,
            )

    asyncio.run(main())
