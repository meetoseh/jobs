"""a very simple job"""
from itgs import Itgs
from graceful_death import GracefulDeath
import logging


async def execute(itgs: Itgs, gd: GracefulDeath, *, message: str):
    """an example job execute - this is invoked when 'runners.example' is the name of the job

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        message (str): the only keyword argument for this job; to be printed out
    """
    logging.info(message)
