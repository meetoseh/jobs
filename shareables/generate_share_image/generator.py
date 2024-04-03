from multiprocessing.synchronize import Event as MPEvent
from typing import Any, Protocol

from graceful_death import GracefulDeath
from images import ImageFile
from itgs import Itgs
from lib.progressutils.progress_helper import ProgressHelper
from PIL import Image


class ShareImageGenerator(Protocol):
    async def get_configuration(
        self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper
    ) -> Any:
        """Returns an object which is trivially JSON serializable (i.e., can be passed
        via `json.dumps`) and which exactly identifies how the image will be generated.
        This is called before prepare.

        For example, if this generator depends on a background image specified as an
        image file and some text, this could return a dictionary representing

        ```json
        {
            "generator": "shareables.my_gen.gen.MyGenerator",
            "generator_version": "1.0.0",
            "image_file_uid": "string",
            "text": "string"
        }
        ```

        This will be embedded in a larger object and saved as a JSON file which will
        act as the original file for the image file, for deduplication purposes.

        Args:
            itgs (Itgs): the integrations to (re)use.
            gd (GracefulDeath): if a term signal is received, the result value of
                prepare() and any exceptions will be ignored, and as soon as it
                returns a ShareImageBounceError will be raised to request a retry
                on a different instance.
            prog (ProgressHelper): the progress helper to use for reporting progress.
        """

    async def prepare(self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper):
        """Used for any cross-process setup. This will be called on the primary process
        in the async thread while no other work is being performed.

        Typically, this would be used for:
        - downloading any required files (e.g., a background image)
        - preprocessing (e.g., gathering aspect ratios of svgs)

        State can be saved in `self`, but be aware that this object will need to
        be sent to the child processes, and that will break references. Generally,
        it's safe to save primitives (str, float, int, etc.), and simple data objects.

        It is generally not safe to save connections or file handles (except for
        those exposed by the multiprocessing module, like multiprocessing.Queue,
        though this shouldn't be necessary in most cases).

        Large objects (e.g., numpy arrays) can lead to memory issues as they may
        need to be copied multiple times if they are stored here.

        NOTE:
            This protocol is not intended to replicate the __aenter__ of a
            context manager. If, for example, you need a place to store
            temporary files, then you should accept a directory in your __init__
            function for which the caller is responsible for cleaning up.

        Arguments:
            itgs (Itgs): the integrations to (re)use.
            gd (GracefulDeath): if a term signal is received, the result value of
                prepare() and any exceptions will be ignored, and as soon as it
                returns a ShareImageBounceError will be raised to request a retry
                on a different instance.
            prog (ProgressHelper): the progress helper to use for reporting progress.
        """
        ...

    def prepare_process(self, exit_requested: MPEvent):
        """Used for per-process setup. For example, if a large image was downloaded
        in prepare() and stored to file, and that image will be used for all targets,
        this could load that image into memory so it's ready for generate().

        Args:
            exit_requested (MPEvent): this event will be set if the parent
                process graceful death signal has been received. Once this is set,
                the result and any errors by this function will be ignored, and the
                parent process will raise a ShareImageBounceError to request a retry
                as soon as all child processes have finished.
        """
        ...

    def generate(self, width: int, height: int, exit_requested: MPEvent) -> Image.Image:
        """Generates the appropriate image for the given resolution and returns it as
        a loaded PIL image (for example, via Image.fromarray).

        NOTE:
            Because this returns an in-memory image, it can be efficient and
            effective to compose generators (which might not necessarily be true
            if this returned or accepted a file path)

        Args:
            width (int): the width of the image to generate in physical pixels.
            height (int): the height of the image to generate in physical pixels.
            exit_requested (MPEvent): this event will be set if the parent
                process graceful death signal has been received. Once this is set,
                the result and any errors by this function will be ignored, and the
                parent process will raise a ShareImageBounceError to request a retry
                as soon as all child processes have finished.

        Returns:
            Image.Image: the generated image.
        """
        ...

    async def finish(
        self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper, img: ImageFile
    ):
        """Called after the all image targets were generated successfully, and they
        were able to be encoded and saved for the appropriate targets, uploaded
        to s3, and metadata stored in the database, aggregating all the given targets
        as an ImageFile.

        This should be used for relating the image to whatever it was generated for,
        e.g., the journey share image, and deleting the old one if it had one which is
        no longer needed.

        This is not meant to replicate the __aexit__ of a context manager. It will not
        be called if an exception is raised in prepare() or generate(), or a term signal
        is received, or if there is an error during uploading, etc. Hence this cannot
        be used to cleanup temporary files, etc. created in prepare()

        Arguments:
            itgs (Itgs): the integrations to (re)use.
            gd (GracefulDeath): once this function is called it is responsible for
                dealing with term signals. If it would like to bounce, it must itself
                raise a ShareImageBounceError.
            prog (ProgressHelper): the progress helper to use for reporting progress.
            img (ImageFile): the image that was generated. the exports will not be filled
                in; refetch if you need them.
        """
        ...
