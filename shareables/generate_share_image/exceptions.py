class ShareImageBounceError(Exception):
    """Raised when a termination signal is received while the image is being
    generated. Since generating images can take a while, we prefer to discard
    any work and queue the job to be retried, as if we do not respect the
    SIGTERM in time we will likely be SIGKILL'd, which will generally leak
    temp files.
    """

    def __init__(self) -> None:
        super().__init__("this instance is shutting down")
