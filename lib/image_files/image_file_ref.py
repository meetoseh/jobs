import io
from pydantic import BaseModel, Field


class ImageFileRef(BaseModel):
    uid: str = Field(description="The UID of the image file")
    jwt: str = Field(description="The JWT to use to access the image file")

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        """A stable sorted json serialization of this model"""
        out.write(b'{"jwt": "')
        out.write(self.jwt.encode("ascii"))
        out.write(b'", "uid": "')
        out.write(self.uid.encode("ascii"))
        out.write(b'"}')
