class ShareablePipelineException(Exception):
    """Base class for shareable pipeline exceptions"""

    def __init__(self, message: str, step_number: int, step_name: str):
        super().__init__(f"In {step_number=} ({step_name=}): {message}")
