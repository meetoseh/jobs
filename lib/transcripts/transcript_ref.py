import io
from pydantic import BaseModel, Field


class TranscriptRef(BaseModel):
    uid: str = Field(description="The UID of the transcript file")
    jwt: str = Field(
        description="A token which provides access to the transcript with the given uid"
    )

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        """A stable sorted json serialization of this model"""
        out.write(b'{"jwt": "')
        out.write(self.jwt.encode("ascii"))
        out.write(b'", "uid": "')
        out.write(self.uid.encode("ascii"))
        out.write(b'"}')
