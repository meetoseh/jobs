from pydantic import BaseModel, Field


class JobCallback(BaseModel):
    name: str = Field(description="the name of the job")
    kwargs: dict = Field(
        description="Additional keyword arguments for the job, must be trivially json serializable"
    )
