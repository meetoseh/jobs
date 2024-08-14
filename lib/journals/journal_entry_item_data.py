import io
from pydantic import BaseModel, Field, TypeAdapter, validator
from typing import Literal, List, Optional, Union, cast

from lib.image_files.image_file_ref import ImageFileRef

# small strings:
# >>> timeit.timeit(stmt='adapter.dump_json(s)', setup='from pydantic import TypeAdapter; adapter = TypeAdapter(str); s="some test string" * 10')
# 0.5624214999988908
# >>> timeit.timeit(stmt='json.dumps(s).encode("utf-8")', setup='import json; s="some test string" * 10')
# 0.46921870000005583
#
# note - if we know the value is ascii, 10x speedup:
# >>> timeit.timeit(stmt='s.encode("ascii")', setup='import json; s="some test string" * 10')
# 0.041761100001167506
#
# long strings:
# >>> timeit.timeit(stmt='adapter.dump_json(s)', setup='from pydantic import TypeAdapter; adapter = TypeAdapter(str); s="some test string" * 1000')
# 5.546335699997144
# >>> timeit.timeit(stmt='json.dumps(s).encode("utf-8")', setup='import json; s="some test string" * 1000')
# 21.25628290000168

str_adapter = cast(TypeAdapter[str], TypeAdapter(str))


class JournalEntryItemTextualPartParagraph(BaseModel):
    type: Literal["paragraph"] = Field(description="A single paragraph of text")
    value: str = Field(description="The contents of the paragraph")

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"type": "paragraph", "value": ')
        out.write(str_adapter.dump_json(self.value))
        out.write(b"}")


class JournalEntryItemTextualPartJourney(BaseModel):
    type: Literal["journey"] = Field(description="A link to a journey")
    uid: str = Field(description="The UID of the journey that was linked")


class MinimalJourneyInstructor(BaseModel):
    name: str = Field(description="The full name of the instructor")
    image: Optional[ImageFileRef] = Field(
        description="The profile image for the instructor, if available"
    )

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        """A stable sorted json serialization of this model"""
        out.write(b"{")
        if self.image is not None:
            out.write(b'"image": ')
            self.image.model_dump_for_integrity(out)
            out.write(b", ")
        out.write(b'"name": ')
        out.write(str_adapter.dump_json(self.name))
        out.write(b"}")


class JournalEntryItemTextualPartJourneyClientDetails(BaseModel):
    """Contains minimal information about a journey and notably lacks a JWT
    to access the journey.
    """

    uid: str = Field(description="The unique identifier for the journey")
    title: str = Field(description="The title of the of the journey")
    description: str = Field(description="The description of the journey")
    darkened_background: ImageFileRef = Field(
        description="The darkened background image for this journey"
    )
    duration_seconds: float = Field(
        description="The duration of the audio portion of the journey in seconds"
    )
    instructor: MinimalJourneyInstructor = Field(
        description="The instructor for the journey"
    )
    last_taken_at: Optional[float] = Field(
        description="The last time the user took the journey"
    )
    liked_at: Optional[float] = Field(description="When the user liked the journey")
    access: Literal["free", "paid-requires-upgrade", "paid-unlocked"] = Field(
        description="The access level for the user"
    )

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        """A stable sorted json serialization of this model"""
        out.write(b'{"access": "')
        out.write(self.access.encode("ascii"))
        out.write(b'", "darkened_background": ')
        self.darkened_background.model_dump_for_integrity(out)
        out.write(b', "description": ')
        out.write(str_adapter.dump_json(self.description))
        out.write(b', "duration_seconds": ')
        out.write(encode_float(self.duration_seconds).encode("ascii"))
        out.write(b', "instructor": ')
        self.instructor.model_dump_for_integrity(out)
        if self.last_taken_at is not None:
            out.write(b', "last_taken_at": ')
            out.write(encode_float(self.last_taken_at).encode("ascii"))
        if self.liked_at is not None:
            out.write(b', "liked_at": ')
            out.write(encode_float(self.liked_at).encode("ascii"))
        out.write(b', "title": ')
        out.write(str_adapter.dump_json(self.title))
        out.write(b', "uid": "')
        out.write(self.uid.encode("ascii"))
        out.write(b'"}')


class JournalEntryItemTextualPartJourneyClient(BaseModel):
    details: JournalEntryItemTextualPartJourneyClientDetails = Field(
        description="Details about the journey"
    )
    type: Literal["journey"] = Field(description="A link to a journey")
    uid: str = Field(description="The UID of the journey that was linked")

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"details": ')
        self.details.model_dump_for_integrity(out)
        out.write(b', "type": "journey", "uid": "')
        out.write(self.uid.encode("ascii"))
        out.write(b'"}')


JournalEntryItemTextualPart = Union[
    JournalEntryItemTextualPartJourney, JournalEntryItemTextualPartParagraph
]

JournalEntryItemTextualPartClient = Union[
    JournalEntryItemTextualPartJourneyClient, JournalEntryItemTextualPartParagraph
]


class JournalEntryItemDataDataTextual(BaseModel):
    parts: List[JournalEntryItemTextualPart] = Field(
        description="The parts of the textual data"
    )

    type: Literal["textual"] = Field(description="The type of data described")


class JournalEntryItemDataDataTextualClient(BaseModel):
    parts: List[JournalEntryItemTextualPartClient] = Field(
        description="The parts of the textual data"
    )

    type: Literal["textual"] = Field(description="The type of data described")

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"parts": [')
        if self.parts:
            self.parts[0].model_dump_for_integrity(out)
            for idx in range(1, len(self.parts)):
                out.write(b", ")
                self.parts[idx].model_dump_for_integrity(out)
        out.write(b'], "type": "textual"}')

    def __str__(self):
        return f"JournalEntryItemDataDataTextualClient(OMITTED FOR PRIVACY)"

    def repr(self):
        return str(self)


class JournalEntryItemUIConceptualUserJourney(BaseModel):
    journey_uid: str = Field(description="The UID of the journey")
    type: Literal["user_journey"] = Field(
        description="we were trying to have the user take a journey"
    )
    user_journey_uid: str = Field(
        description="The UID of the record tracking the user took the journey"
    )

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"journey_uid": "')
        out.write(self.journey_uid.encode("ascii"))
        out.write(b'", "type": "user_journey", "user_journey_uid": "')
        out.write(self.user_journey_uid.encode("ascii"))
        out.write(b'"}')


class JournalEntryItemUIConceptualUpgrade(BaseModel):
    type: Literal["upgrade"] = Field(
        description="we were trying to have the user upgrade to oseh+"
    )

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"type": "upgrade"}')


JournalEntryItemUIConceptual = Union[
    JournalEntryItemUIConceptualUserJourney, JournalEntryItemUIConceptualUpgrade
]


class JournalEntryItemUIFlow(BaseModel):
    slug: str = Field(description="the slug of the client flow they took")

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"slug": ')
        out.write(str_adapter.dump_json(self.slug))
        out.write(b"}")


class JournalEntryItemDataDataUI(BaseModel):
    conceptually: JournalEntryItemUIConceptual = Field(
        description="What this UI event was trying to accomplish"
    )
    flow: JournalEntryItemUIFlow = Field(
        description="The flow we triggered on the users screen queue"
    )
    type: Literal["ui"] = Field(description="The type of data described")

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"conceptually": ')
        self.conceptually.model_dump_for_integrity(out)
        out.write(b', "flow": ')
        self.flow.model_dump_for_integrity(out)
        out.write(b', "type": "ui"}')


JournalEntryItemDataData = Union[
    JournalEntryItemDataDataTextual, JournalEntryItemDataDataUI
]

JournalEntryItemDataDataClient = Union[
    JournalEntryItemDataDataTextualClient, JournalEntryItemDataDataUI
]


class JournalEntryItemProcessingBlockedReason(BaseModel):
    """The object used to describe why we are blocking processing of a journal entry item"""

    reasons: List[Literal["flagged", "unchecked"]] = Field(
        description="The reasons why we are blocking processing of this item. Sorted, unique items, not empty",
        min_length=1,
    )

    @validator("reasons")
    def unique_sorted_reasons(cls, v):
        if len(set(v)) != len(v):
            raise ValueError("Reasons must be unique")
        if v != sorted(v):
            raise ValueError("Reasons must be sorted")
        return v

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"reasons": ["')
        out.write(self.reasons[0].encode("ascii"))
        for idx in range(1, len(self.reasons)):
            out.write(b'", "')
            out.write(self.reasons[idx].encode("ascii"))
        out.write(b'"]}')


class JournalEntryItemData(BaseModel):
    """The data for a journal entry item that we consider particularly sensitive.
    This should only ever be transferred encrypted with a journal master key
    (for internal communication) or journal client key (for providing it to the
    user who wrote it)
    """

    data: JournalEntryItemDataData = Field(
        description="describes how to render this item"
    )

    display_author: Literal["self", "other"] = Field(
        description="who to display as the author of this item; self means the user, other means the system"
    )

    processing_block: Optional[JournalEntryItemProcessingBlockedReason] = Field(
        None,
        description="If this item is blocked from processing, the reasons why",
    )

    type: Literal["chat", "reflection-question", "reflection-response", "ui"] = Field(
        description="The type of thing that occurred"
    )

    def __str__(self):
        return f"JournalEntryItemData(OMITTED FOR PRIVACY)"

    def repr(self):
        return str(self)


class JournalEntryItemDataClient(BaseModel):
    """The data for a journal entry item that we consider particularly sensitive.
    This should only ever be transferred encrypted with a journal master key
    (for internal communication) or journal client key (for providing it to the
    user who wrote it)
    """

    data: JournalEntryItemDataDataClient = Field(
        description="describes how to render this item"
    )

    display_author: Literal["self", "other"] = Field(
        description="who to display as the author of this item; self means the user, other means the system"
    )

    type: Literal["chat", "reflection-question", "reflection-response", "ui"] = Field(
        description="The type of thing that occurred"
    )

    def model_dump_for_integrity(self, out: io.BytesIO) -> None:
        out.write(b'{"data": ')
        self.data.model_dump_for_integrity(out)
        out.write(b', "display_author": "')
        out.write(self.display_author.encode("ascii"))
        out.write(b'", "type": "')
        out.write(self.type.encode("ascii"))
        out.write(b'"}')

    def __str__(self):
        return f"JournalEntryItemDataClient(OMITTED FOR PRIVACY)"

    def repr(self):
        return str(self)


def encode_float(v: Union[int, float]) -> str:
    """Encodes a float as string in a way that can be exactly replicated
    in javascript via

    return v.toLocaleString('en-US', {
        minimumFractionDigits: 3,
        maximumFractionDigits: 3,
        notation: 'standard',
        useGrouping: false,
    });
    """
    return f"{v:.3f}"
