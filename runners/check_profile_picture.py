"""Checks if a given users profile picture matches the given url, and if it does not,
downloads the image, modifies it appropriately, and updates our database
"""
from itgs import Itgs
from graceful_death import GracefulDeath


async def execute(itgs: Itgs, gd: GracefulDeath, *, user_sub: str, picture_url: str):
    """If the given user exists, but its profile picture is either not set or does
    not match the given url, download the image, modify it appropriately, and update
    our database

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        user_sub (str): the user to check
        picture_url (str): the url of the profile picture to check against
    """
    slack = await itgs.slack()
    await slack.send_web_error_message(f"TODO: would check profile picture {picture_url} against {user_sub}")
