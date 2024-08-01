from typing import Optional
from pydantic import BaseModel, Field

from lib.content_files.models import ContentFileRef
from lib.image_files.image_file_ref import ImageFileRef
from lib.transcripts.transcript_ref import TranscriptRef


class ExternalJourneyCategory(BaseModel):
    """A category as represented for the external-facing journey endpoint"""

    external_name: str = Field(description="The name of the category, e.g., 'Verbal'")


class ExternalJourneyInstructor(BaseModel):
    """An instructor as represented for the external-facing journey endpoint"""

    name: str = Field(description="The name of the instructor")


class ExternalJourneyDescription(BaseModel):
    """A description as represented for the external-facing journey endpoint"""

    text: str = Field(description="The description text")


class ExternalJourney(BaseModel):
    """Describes a journey in the format we return it to clients with
    so that they can start the journey. This is the format that is
    sufficient for the user to actually take the journey, which differs
    from the format we might provide if the user is just browsing

    Typically the first thing a client does with this is use the
    journey jwt to get an interactive prompt jwt for the lobby.
    """

    uid: str = Field(description="The UID of the journey")

    jwt: str = Field(description="The JWT which provides access to the journey")

    interactive_prompt_uid: str = Field(
        description=(
            "The UID of the interactive prompt associated with this journey. This "
            "is not often directly useful for the client, but it can be used to improve "
            "caching and is very helpful for client flow triggers as an extraction "
            "target"
        )
    )

    duration_seconds: float = Field(
        description="The duration of the journey, in seconds"
    )

    background_image: ImageFileRef = Field(
        description="The background image for the journey."
    )

    blurred_background_image: ImageFileRef = Field(
        description="The blurred background image for the journey."
    )

    darkened_background_image: ImageFileRef = Field(
        description="The darkened background image for the journey."
    )

    audio_content: ContentFileRef = Field(
        description="The audio content for the journey"
    )

    transcript: Optional[TranscriptRef] = Field(
        description="The transcript for the journey, if available"
    )

    category: ExternalJourneyCategory = Field(
        description="How the journey is categorized"
    )

    title: str = Field(description="The very short class title")

    instructor: ExternalJourneyInstructor = Field(
        description="The instructor for the journey"
    )

    description: ExternalJourneyDescription = Field(
        description="The description of the journey"
    )

    sample: Optional[ContentFileRef] = Field(
        description="A sample for the journey as a 15 second clip, if one is available."
    )
