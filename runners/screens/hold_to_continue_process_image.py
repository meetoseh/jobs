"""Processes a raw image intended to be used for a press to hold screen"""

from itgs import Itgs
from graceful_death import GracefulDeath
from jobs import JobCategory
import runners.screens.exact_dynamic_process_image

category = JobCategory.HIGH_RESOURCE_COST


async def execute(
    itgs: Itgs,
    gd: GracefulDeath,
    *,
    file_upload_uid: str,
    uploaded_by_user_sub: str,
    job_progress_uid: str,
):
    """Processes the s3 file upload with the given uid for the image on a
    hold to continue screen.

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
        file_upload_uid (str): used to indicate which file to process.
        uploaded_by_user_sub (str): The sub of the user who uploaded the file
        job_progress_uid (str): The uid of the job progress to update
    """
    return await runners.screens.exact_dynamic_process_image.execute(
        itgs,
        gd,
        file_upload_uid=file_upload_uid,
        uploaded_by_user_sub=uploaded_by_user_sub,
        job_progress_uid=job_progress_uid,
        dynamic_size=runners.screens.exact_dynamic_process_image.DynamicSize(
            width=200, height=200
        ),
    )
