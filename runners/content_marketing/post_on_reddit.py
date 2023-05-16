import secrets
import time
from typing import Optional
from error_middleware import handle_contextless_error
from itgs import Itgs
from graceful_death import GracefulDeath
import logging
from jobs import JobCategory
import praw
from lib.basic_redis_lock import basic_redis_lock
import os
import textwrap
import string


category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath, *, dry_run: Optional[bool] = None):
    """Makes a new post on /r/oseh using an AI video, if there is one that hasn't been
    posted yet.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        dry_run (bool, None): True to not actually post anything, false to post,
            None to decide based on the current environment (i.e., post only in prod)
    """
    if dry_run is None:
        dry_run = os.environ["ENVIRONMENT"] != "production"

    logging.debug("Connecting to reddit...")
    async with basic_redis_lock(itgs, key="reddit:lock", gd=gd, spin=False):
        redis = await itgs.redis()
        refresh_token = await redis.get(b"reddit:refresh_token")

        if refresh_token is None:
            raise Exception(
                "Reddit refresh token not initialized; manual intervention required"
            )

        reddit = praw.Reddit(
            client_id=os.environ["OSEH_REDDIT_CLIENT_ID"],
            client_secret=os.environ["OSEH_REDDIT_CLIENT_SECRET"],
            refresh_token=refresh_token.decode("utf-8"),
            user_agent="oseh by u/Tjstretchalot for r/oseh",
        )

        logging.debug("Successfully connected to reddit, selecting video...")

        conn = await itgs.conn()
        cursor = conn.cursor("weak")

        response = await cursor.execute(
            """
            SELECT
                journeys.title,
                journeys.description,
                journeys.uid
            FROM journeys
            WHERE
                journeys.special_category = 'ai'
                AND journeys.deleted_at IS NULL
                AND journeys.sample_content_file_id IS NOT NULL
                AND journeys.video_content_file_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM journey_reddit_posts, journey_public_links
                    WHERE
                        journey_public_links.id = journey_reddit_posts.journey_public_link_id
                        AND journey_public_links.journey_id = journeys.id
                        AND journey_reddit_posts.subreddit = 'oseh'
                )
            ORDER BY journeys.created_at DESC
            LIMIT 1
            """
        )
        if not response.results:
            await handle_contextless_error(
                extra_info="Cannot make a reddit post: no AI journeys found that are ready and haven't been posted yet",
            )
            return

        journey_title: str = response.results[0][0]
        journey_description: str = response.results[0][1]
        journey_uid: str = response.results[0][2]

        post_title = textwrap.shorten(
            f"{journey_title}: {journey_description}", width=300
        )

        jpl_uid = f"oseh_jpl_{secrets.token_urlsafe(16)}"
        jpl_code = make_code(journey_title)
        jpl_url = os.environ["ROOT_FRONTEND_URL"] + "/jpl?code=" + jpl_code

        now = time.time()
        response = await cursor.execute(
            """
            INSERT INTO journey_public_links (
                uid, code, journey_id, created_at, deleted_at
            )
            SELECT
                ?, ?, journeys.id, ?, NULL
            FROM journeys
            WHERE
                journeys.uid = ?
            """,
            (jpl_uid, jpl_code, now, journey_uid),
        )
        if response.rows_affected is None or response.rows_affected < 1:
            await handle_contextless_error(
                extra_info="Cannot make a reddit post: failed to insert the new journey public link",
            )
            return

        logging.info(f"Posting on /r/oseh: {post_title} ({jpl_url})")
        if dry_run:
            logging.debug("Dry run, not actually posting")
            return

        submission = reddit.subreddit("oseh").submit(
            title=post_title,
            url=jpl_url,
        )

        jrp_uid = f"oseh_jrp_{secrets.token_urlsafe(16)}"
        response = await cursor.execute(
            """
            INSERT INTO journey_reddit_posts (
                uid, journey_public_link_id, submission_id, permalink,
                title, subreddit, author, created_at
            )
            SELECT
                ?, journey_public_links.id, ?, ?, ?, ?, ?, ?
            FROM journey_public_links
            WHERE
                journey_public_links.uid = ?
            """,
            (
                jrp_uid,
                submission.id,
                submission.permalink,
                submission.title,
                submission.subreddit.display_name,
                submission.author.name,
                now,
                jpl_uid,
            ),
        )
        if response.rows_affected is None or response.rows_affected < 1:
            await handle_contextless_error(
                extra_info=f"Failed to store the reddit post in the database: {submission.permalink}"
            )

        logging.info(f"Successfully posted on reddit: {submission.permalink}")

        slack = await itgs.slack()
        await slack.send_ops_message(f"Posted on reddit: {submission.permalink}")


def make_code(text: str) -> str:
    """Makes a url-safe code from the given text. All non-ascii characters are
    removed, then the text is shortened to at most 16 characters, and all spaces
    are replaced with hyphens, and all other non-alphanumeric characters are
    removed, then a short random suffix is added
    """
    text = "".join(
        (c if c != " " else "-") for c in text if c in string.ascii_letters or c == " "
    )
    text = text[:16]
    text = text.rstrip("-")
    text += "-" + secrets.token_urlsafe(4)
    return text


if __name__ == "__main__":
    import asyncio
    import argparse

    async def main():
        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--no-dry-run", action="store_true")
        args = parser.parse_args()

        dry_run = None
        if args.dry_run:
            dry_run = True
        if args.no_dry_run:
            dry_run = False

        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue(
                "runners.content_marketing.post_on_reddit", dry_run=dry_run
            )

    asyncio.run(main())
