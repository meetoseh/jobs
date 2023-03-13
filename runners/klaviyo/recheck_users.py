"""For each user which has a klaviyo profile, double checks what lists they are actually
on in klaviyo and adds them to the ones they aren't on.
"""
import asyncio
from typing import Optional, Set
from itgs import Itgs
from graceful_death import GracefulDeath
import logging

from jobs import JobCategory

category = JobCategory.HIGH_RESOURCE_COST
"""The category of the job; used to determine which instances can run this job.
"""


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Syncs klaviyo profile subscriptions with klaviyo.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    conn = await itgs.conn()
    cursor = conn.cursor("none")

    slack = await itgs.slack()
    klaviyo = await itgs.klaviyo()

    sms_list_ids: Set[str] = set()
    for internal_id in ["sms-morning", "sms-afternoon", "sms-evening"]:
        sms_list_ids.add(await klaviyo.list_id(internal_id))

    last_klaviyo_profile_id: str = None
    last_list_id: str = None

    current_klaviyo_profile_id: Optional[str] = None
    current_klaviyo_profile_actual_list_ids: Optional[Set[str]] = None

    while True:
        response = await cursor.execute(
            """
            SELECT
                user_klaviyo_profiles.klaviyo_id,
                user_klaviyo_profiles.email,
                user_klaviyo_profiles.phone_number,
                user_klaviyo_profile_lists.list_id
            FROM user_klaviyo_profile_lists
            JOIN user_klaviyo_profiles ON user_klaviyo_profiles.id = user_klaviyo_profile_lists.user_klaviyo_profile_id
            WHERE
                (? IS NULL OR (
                    user_klaviyo_profiles.klaviyo_id > ?
                    OR (
                        user_klaviyo_profiles.klaviyo_id = ?
                        AND user_klaviyo_profile_lists.list_id > ?
                    )
                ))
            ORDER BY user_klaviyo_profiles.klaviyo_id, user_klaviyo_profile_lists.list_id
            LIMIT 50
            """,
            (
                last_klaviyo_profile_id,
                last_klaviyo_profile_id,
                last_klaviyo_profile_id,
                last_list_id,
            ),
        )

        if not response.results:
            await slack.send_ops_message("Finished syncing klaviyo profiles")
            return

        for (
            klaviyo_profile_id,
            email,
            phone_number,
            list_id_they_should_be_on,
        ) in response.results:
            if klaviyo_profile_id != current_klaviyo_profile_id:
                current_klaviyo_profile_id = klaviyo_profile_id
                current_klaviyo_profile_actual_list_ids = set()
                async for list_id in klaviyo.get_profile_lists_auto_paginated(
                    profile_id=klaviyo_profile_id
                ):
                    current_klaviyo_profile_actual_list_ids.add(list_id)
                await asyncio.sleep(1)

                if phone_number is None:
                    for list_id in sms_list_ids:
                        if list_id in current_klaviyo_profile_actual_list_ids:
                            await slack.send_web_error_message(
                                f"User {email=} ({klaviyo_profile_id=}) is subscribed to {list_id=}, which is for sms, but they have no phone number?"
                            )
                            await asyncio.sleep(1)

            if list_id_they_should_be_on not in current_klaviyo_profile_actual_list_ids:
                if list_id_they_should_be_on in sms_list_ids:
                    if phone_number is None:
                        await slack.send_web_error_message(
                            f"User {email=} ({klaviyo_profile_id=}) is not subscribed to {list_id_they_should_be_on=}, which is for sms, but we think they should be despite no phone number?"
                        )
                        await asyncio.sleep(1)
                        continue

                    logging.info(
                        f"Adding {phone_number} to list {list_id_they_should_be_on}"
                    )
                    await klaviyo.subscribe_profile_to_list(
                        profile_id=current_klaviyo_profile_id,
                        email=None,
                        phone_number=phone_number,
                        list_id=list_id_they_should_be_on,
                    )
                    await slack.send_web_error_message(
                        f"User {email=} ({phone_number=}) ({klaviyo_profile_id=}) was supposed to be subscribed to {list_id_they_should_be_on=} "
                        "(for sms) but they weren't; added them successfully"
                    )
                    await asyncio.sleep(1)
                else:
                    logging.info(f"Adding {email} to list {list_id_they_should_be_on}")
                    await klaviyo.subscribe_profile_to_list(
                        profile_id=current_klaviyo_profile_id,
                        email=email,
                        phone_number=None,
                        list_id=list_id_they_should_be_on,
                    )
                    await slack.send_web_error_message(
                        f"User {email=} ({klaviyo_profile_id=}) was supposed to be subscribed to {list_id_they_should_be_on=} "
                        "(for email) but they weren't; added them successfully"
                    )
                    await asyncio.sleep(1)

        last_klaviyo_profile_id = response.results[-1][0]
        last_list_id = response.results[-1][3]


if __name__ == "__main__":

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.klaviyo.recheck_users")
            print("job queued successfully")

    asyncio.run(main())
