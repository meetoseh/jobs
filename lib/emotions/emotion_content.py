from pydantic import BaseModel, Field
from typing import List, Optional
from itgs import Itgs


class Emotion(BaseModel):
    word: str = Field(
        description="The unique word that represents this emotion, e.g., lost, angry, sad"
    )
    antonym: str = Field(
        description="The action that is taken to resolve this emotion, e.g., find yourself, calm down, cheer up"
    )


class EmotionContentStatistics(BaseModel):
    emotion: Emotion = Field(description="The emotion these statistics are for")
    num_journeys: int = Field(
        description="The number of undeleted journeys that have this emotion"
    )


class EmotionContentPurgeMessage(BaseModel):
    replace_stats: Optional[List[EmotionContentStatistics]] = Field(
        description=(
            "If present, instead of just purging the cache, replace "
            "the cache with these statistics"
        )
    )


async def purge_emotion_content_statistics_everywhere(
    itgs: Itgs, *, emotions: Optional[List[str]] = None
):
    """Purges emotion content statistics from the distributed cache and
    all local caches. The cache will be filled again with the latest
    statistics from the database on the next request.

    Args:
        itgs (Itgs): The integrations to (re)use
        emotions (list[str] or None): If the callee knows that only
            certain emotions statistics may have changed, they can
            specify them here. Currently unused, but left open for
            future use.
    """
    message = EmotionContentPurgeMessage(replace_stats=None).json().encode("utf-8")

    redis = await itgs.redis()
    async with redis.pipeline(transaction=True) as pipe:
        pipe.multi()
        await pipe.delete(b"emotion_content_statistics")
        await pipe.publish("ps:emotion_content_statistics:push_cache", message)
        await pipe.execute()
