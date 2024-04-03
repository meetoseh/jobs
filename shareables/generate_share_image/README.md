# generate_share_image

This folder is for a meta-pipeline; it's intended to assist with more specific
pipelines more-so than being used directly.

This is for generating custom open-graph meta images for content. This folder
mostly handles the generation parallelization and the list of targets.

Unlike with the `images` library, which expects that the different targets
are produced by cropping and resizing a single original image, this folder
expects that the image is generated "on the fly" for each target based on
an algorithm that may differ. This allows for the targets to, for example,
all include text a fixed number of pixels from the bottom-left, despite
targets varying in both aspect ratio and overall size.

## Usage

Construct an object meeting the `ShareImageGenerator` protocol, then call
`run_pipeline`.

```py
import shareables.generate_share_image.main
from shareables.generate_share_image.exceptions import ShareImageBounceError
from itgs import Itgs
from graceful_death import GracefulDeath
from images import ImageTarget, ImageFile
from multiprocessing.synchronize import Event as MPEvent
from typing import List
from PIL import Image

class MyGenerator:
    def __init__(self):
        ...

    async def get_configuration(
        self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper
    ) -> Any:
        # called on the main process, in the async thread, before prepare
        # returns json-serializable configuration that uniquely identifies the
        # targets that will be generated, for deduplication purposes
        ...

    async def prepare(self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper):
        # this is called on the main process, in the async thread,
        # without any other processes working
        ...

    def prepare_process(self, exit_requested: MPEvent):
        # this is called on a child process, once per process. The exit requested
        # event is set if this function should exit as soon as possible; it is
        # acceptable to raise an error when the exit event is set and it will
        # be caught and suppressed (useful for e.g. generate which would otherwise
        # need to return an image even if none was ready)
        ...

    def generate(self, width: int, height: int, exit_requested: MPEvent) -> Image.Image:
        # this is called potentially multiple times per child process. This should either
        # error, which will eventually bubble to the main process in the async thread, or
        # return the PIL image at the appropriate width and height. Note that via Image.fromarray,
        # you can compute the image using numpy with shape height x width x 3 (for RGB)
        ...

    async def finish(self, itgs: Itgs, gd: GracefulDeath, prog: ProgressHelper, img: ImageFile):
        # this is called on the main process, in the async thread,
        # without any other processes working, after the image targets have
        # all been generated and uploaded and an image file has been assigned
        #
        # the redis connection will have been refreshed since the start, in case
        # it was disconnected due to idleness
        ...

async def execute(itgs: Itgs, gd: GracefulDeath):
    try:
        await shareables.generate_share_image.main.run_pipeline(
            itgs,
            gd,
            MyGenerator()
        )
    except ShareImageBounceError as e:
        # ... bounce the job (queue it again, this instance needs to shut down) ...
        # redis connection will have already been refreshed at this point
```
