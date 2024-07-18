from dataclasses import dataclass

from lib.journals.journal_chat_task import JournalChatTask
from lib.journals.master_keys import GetJournalMasterKeyForEncryptionResultSuccess


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

    def reserve_event_counter(self) -> int:
        """Increments self.next_event_counter and returns the previous value"""
        result = self.next_event_counter
        self.next_event_counter += 1
        return result
