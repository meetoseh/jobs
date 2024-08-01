from dataclasses import dataclass
import time
from typing import Dict, Optional

import pytz

from itgs import Itgs
from lib.journals.journal_chat_job_stats import JobType, JournalChatJobStats
from lib.journals.journal_chat_task import JournalChatTask
from lib.journals.master_keys import GetJournalMasterKeyForEncryptionResultSuccess


@dataclass
class RefMemoryCachedData:
    """Generic reference in memory, already presigned"""

    uid: str
    """The uid of the thing the jwt provides access to"""
    jwt: str
    """The JWT that provides access to the thing the uid points to"""


@dataclass
class InstructorMemoryCachedData:
    """Minimal data we have fetched about an instructor already in the context of processing
    the job
    """

    name: str
    """The name of the instructor"""
    image: Optional[RefMemoryCachedData]
    """The profile image of the instructor, if the instructor has a profile image"""


@dataclass
class JourneyMemoryCachedData:
    """Data we have fetched about a journey in the context of processing this job; primarily
    used by the `data_to_client` module
    """

    uid: str
    """The unique identifier for the journey"""
    title: str
    """The title of the of the journey"""
    description: str
    """The description of the journey"""
    darkened_background: RefMemoryCachedData
    """The darkened background image for this journey, already signed"""
    duration_seconds: float
    """The duration of the audio portion of the journey in seconds"""
    instructor: InstructorMemoryCachedData
    """The instructor for the journey"""
    last_taken_at: Optional[float]
    """The last time the user took the journey"""
    liked_at: Optional[float]
    """When the user liked the journey"""
    requires_pro: bool
    """True if only pro users can access this journey, False if free and pro users can access this journey"""


@dataclass
class JournalChatJobContext:
    journal_chat_uid: str
    """The 'journal chat' uid, which is the identifier for the job, which consists
    of replacing or adding new journal entry items by forming a JournalChat object
    via a series of mutations (allowing the object to be written to in pieces and out
    of order, if desired)
    """
    journal_master_key: GetJournalMasterKeyForEncryptionResultSuccess
    """The journal master key to use for internal communication about the sensitive
    details (basically, anything in task or that would go in task)
    """
    starts: int
    """The number of times this job has been started in total; at least 1 by the time
    this context object is received
    """
    start_time: int
    """The time when this job was started in seconds since the epoch"""
    log_id: str
    """A random identifier that can be used in log messages so that the related ones can be
    grepped
    """
    queued_at: int
    """The time when this job was originally queued in seconds since the epoch. This should
    be used as the time for most stats
    """
    queued_at_unix_date_in_stats_tz: int
    """The time this job was queued as a unix date in the stats timezone"""
    user_sub: str
    """The sub of the user this job is for"""
    user_tz: pytz.BaseTzInfo
    """The timezone the user is in"""
    journal_entry_uid: str
    """The journal entry that we are manipulating"""
    next_event_counter: int
    """The event counter to use for the next event in the journal chat jobs event list.
    After consuming an event, this value should be incremented by 1 to keep it in sync
    """
    task: JournalChatTask
    """The task to be performed. This is considered sensitive, so avoid logging it or
    sending it anywhere unencrypted
    """
    last_checked_redis: float
    """The last time we called ensure_redis_liveliness() on the underlying itgs instance"""
    has_pro: Optional[bool]
    """True if we know the user has pro, False if we know they don't have the pro entitlement,
    None if we haven't checked
    """
    memory_cached_journeys: Dict[str, Optional[JourneyMemoryCachedData]]
    """The journeys we have already loaded while processing this job. These cannot be used
    across jobs as its time-sensitive (e.g., jwts and information that could have changed
    or is specific to the user like entitlements)

    None if we have already checked and the journey does not exist
    """
    stats: JournalChatJobStats
    """The stats we are storing the success or failure of this job in. Stored for you after closing.
    The started event is handled for you, but you must write the failed or completed event.
    """
    type: JobType
    """The type of job this is"""

    def reserve_event_counter(self) -> int:
        """Increments self.next_event_counter and returns the previous value"""
        result = self.next_event_counter
        self.next_event_counter += 1
        return result

    async def maybe_check_redis(self, itgs: Itgs) -> None:
        """Checks that the redis connection is still alive, but only if it hasn't been
        checked just now"""
        if time.time() - self.last_checked_redis > 0.1:
            self.last_checked_redis = time.time()
            await itgs.ensure_redis_liveliness()
