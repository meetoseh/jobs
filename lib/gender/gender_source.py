from pydantic import BaseModel, Field, StringConstraints, TypeAdapter
from typing import Annotated, Generic, Literal, Optional, TypeVar, Union


class GenderByFirstNamePayload(BaseModel):
    locale: Annotated[
        Optional[str], StringConstraints(min_length=2, strip_whitespace=True)
    ] = Field(
        None,
        description="The locale of the user, as an IETF BCP 47 language tag, e.g., en-US",
    )
    first_name: Annotated[
        str, StringConstraints(min_length=1, max_length=50, strip_whitespace=True)
    ] = Field(
        description="The first name of the user",
    )


class GenderByFirstNameInput(BaseModel):
    first_name: str = Field(description="The first name provided")


class GenderByFirstNameDetails(BaseModel):
    credits_used: Optional[int] = Field(
        None, description="The number of credits used for the request"
    )
    samples: Optional[int] = Field(
        None, description="Number of records which match the request"
    )
    country: Optional[str] = Field(None, description="The country found for the name")
    first_name_sanitized: Optional[str] = Field(
        None, description="The extracted name after the normalization process"
    )
    duration: Optional[str] = Field(
        None, description="Time the server took to process the request, e.g., 78ms"
    )


InputT = TypeVar("InputT", bound=BaseModel)


class GenderResponse(BaseModel, Generic[InputT]):
    input: Optional[InputT] = Field(
        None, description="The input parameters for the request"
    )
    details: Optional[GenderByFirstNameDetails] = Field(
        None, description="The details of the request"
    )
    result_found: bool = Field(description="Whether a result was found for the request")
    first_name: Optional[str] = Field(
        None, description="The first name used for genderization"
    )
    probability: Optional[float] = Field(
        None,
        description="Indicates the reliability of this answer, between 0 and 1",
        ge=0,
        le=1,
    )
    gender: Optional[Literal["male", "female", "unknown"]] = Field(
        None,
        description="The gender we determined, unknown if the name is equally likely to be male or female",
    )


GenderByFirstNameResponse = GenderResponse[GenderByFirstNameInput]

GenderGuessSourceT = TypeVar("GenderGuessSourceT", bound=str)
PayloadT = TypeVar("PayloadT", bound=BaseModel)
ResponseT = TypeVar("ResponseT", bound=BaseModel)


class GenderGuessSource(BaseModel, Generic[GenderGuessSourceT, PayloadT, ResponseT]):
    type: GenderGuessSourceT = Field(description="Discriminatory field")
    url: str = Field(description="The URL used for the request")
    payload: PayloadT = Field(description="The payload used for the request")
    response: ResponseT = Field(description="The response from the request")


GenderByFirstNameSource = GenderGuessSource[
    Literal["by-first-name"], GenderByFirstNamePayload, GenderByFirstNameResponse
]


class GenderByFullNamePayload(BaseModel):
    locale: Annotated[
        Optional[str], StringConstraints(min_length=2, strip_whitespace=True)
    ] = Field(
        None,
        description="The locale of the user, as an IETF BCP 47 language tag, e.g., en-US",
    )
    full_name: Annotated[
        str, StringConstraints(min_length=3, max_length=100, strip_whitespace=True)
    ] = Field(
        description="The full name of the user",
    )


class GenderByFullNameInput(BaseModel):
    full_name: str = Field(description="The full name provided")


GenderByFullNameResponse = GenderResponse[GenderByFullNameInput]
GenderByFullNameSource = GenderGuessSource[
    Literal["by-full-name"], GenderByFullNamePayload, GenderByFullNameResponse
]


class GenderByEmailAddressPayload(BaseModel):
    locale: Annotated[
        Optional[str], StringConstraints(min_length=2, strip_whitespace=True)
    ] = Field(
        None,
        description="The locale of the user, as an IETF BCP 47 language tag, e.g., en-US",
    )
    email: Annotated[
        str, StringConstraints(min_length=3, max_length=100, strip_whitespace=True)
    ] = Field(
        description="The email address of the user",
    )


class GenderByEmailAddressInput(BaseModel):
    email: str = Field(description="The email address provided")


GenderByEmailAddressResponse = GenderResponse[GenderByEmailAddressInput]
GenderByEmailAddressSource = GenderGuessSource[
    Literal["by-email-address"],
    GenderByEmailAddressPayload,
    GenderByEmailAddressResponse,
]


class GenderByUserEntrySource(BaseModel):
    type: Literal["by-user-entry"] = Field(description="Discriminatory field")


class GenderByAdminEntrySource(BaseModel):
    type: Literal["by-admin-entry"] = Field(description="Discriminatory field")
    admin_sub: str = Field(
        description="The sub of the admin user that identified the gender"
    )


class GenderByFallbackSource(BaseModel):
    type: Literal["by-fallback"] = Field(description="Discriminatory field")


GenderSource = Union[
    GenderByFirstNameSource,
    GenderByFullNameSource,
    GenderByEmailAddressSource,
    GenderByUserEntrySource,
    GenderByAdminEntrySource,
    GenderByFallbackSource,
]

gender_source_adapter: TypeAdapter[GenderSource] = TypeAdapter(GenderSource)
