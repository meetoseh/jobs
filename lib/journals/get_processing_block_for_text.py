import asyncio
import openai
from itgs import Itgs
import os
from typing import List, Optional
from lib.journals.journal_entry_item_data import JournalEntryItemProcessingBlockedReason


async def get_processing_block_for_text(
    itgs: Itgs, text: str
) -> Optional[JournalEntryItemProcessingBlockedReason]:
    """Determines if the given text should be blocked from processing using openai's moderation endpoint"""
    # PERF: this is slow if >2000 characters, but i don't think thats very common right now,
    # and i'd rather degrade slow than lose accuracy
    client = openai.OpenAI(api_key=os.environ["OSEH_OPENAI_API_KEY"])

    chunks: List[str] = []
    handled_up_to = 0
    while True:
        if handled_up_to >= len(text):
            break

        split_at = handled_up_to + 2000
        if split_at >= len(text):
            chunks.append(text[handled_up_to:])
            break

        while text[split_at] != " " and text[split_at] != "\n":
            split_at -= 1
            if split_at <= handled_up_to + 1000:
                split_at = handled_up_to + 2000
                break

        chunks.append(text[handled_up_to:split_at])
        handled_up_to = split_at

    chunks = [c.strip() for c in chunks]
    chunks = [c for c in chunks if c]
    if not chunks:
        return None

    for chunk in chunks:
        moderation_response = await asyncio.to_thread(
            client.moderations.create, input=chunk
        )
        if moderation_response.results[0].flagged:
            return JournalEntryItemProcessingBlockedReason(reasons=["flagged"])

    return None
