import os
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory

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

    response = await cursor.execute(
        """
        SELECT
            users.given_name,
            users.family_name, 
            journeys.title
        FROM users, journeys
        WHERE
            users.sub = ?
            AND journeys.uid = ?
        """,
        (user_sub, journey_uid),
    )
    if not response.results:
        await handle_contextless_error(
            f"notify_user_changed_likes: no corresponding {user_sub=} and {journey_uid=}"
        )
        return

    name: str = (
        (response.results[0][0] or "") + " " + (response.results[0][1] or "")
    ).strip()
    if name == "":
        name = "Anonymous"
    title: str = response.results[0][2]

    base_url = os.environ["ROOT_FRONTEND_URL"]
    user_url = f"{base_url}/admin/user?sub={user_sub}"

    message = f"{name} {'favorited' if liked else 'unfavorited'} {title} (<{user_url}|view user>)"

    logging.info(f"notify_user_changed_likes: {message}")
    if os.environ["ENVIRONMENT"] != "production":
        return

    slack = await itgs.slack()
    await slack.send_oseh_bot_message(message)


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
