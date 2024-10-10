from itgs import Itgs
from graceful_death import GracefulDeath
import runners.delete_file_upload

from jobs import JobCategory

category = JobCategory.LOW_RESOURCE_COST


async def execute(
    itgs: Itgs, gd: GracefulDeath, *, file_upload_uid: str, voice_note_uid: str
):
    """Unused alias for runners.delete_file_upload. This is here because I considered
    using it for a bit, then had some jobs in the queue related to this, and so it's
    easier to keep it to cleanup the remaining jobs and file uploads. It could also
    be useful if we need/want fancier cleanup, in which case just update the docs here
    to indicate it's being used.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): the uid of the file upload to delete
    """
    await runners.delete_file_upload.execute(itgs, gd, file_upload_uid=file_upload_uid)
