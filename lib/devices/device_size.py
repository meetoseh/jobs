from pydantic import BaseModel, Field


class DeviceSize(BaseModel):
    name: str = Field(description="The name of the device")
    family: str = Field(description="The family of the device, e.g., iPhone")
    logical_width: int = Field(description="The logical width of the device in pixels")
    logical_height: int = Field(
        description="The logical height of the device in pixels"
    )
    physical_width: int = Field(
        description="The physical width of the device in pixels"
    )
    physical_height: int = Field(
        description="The physical height of the device in pixels"
    )
    ppi: int = Field(description="The pixels per inch of the device")
    scale_factor: int = Field(description="The scale factor of the device")
    release_date_iso8601: str = Field(
        description="The date the device was released in YYYY-MM-DD"
    )
