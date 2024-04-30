from dataclasses import dataclass
from typing import Callable, Coroutine
from rqdb.result import ResultItem


@dataclass
class Query:
    sql: str
    """The actual SQL to execute"""
    args: list
    """The prepared arguments for the SQL"""

    process_result: Callable[[ResultItem], Coroutine[None, None, None]]
    """The function to process the result of the query. Should raise
    PreconditionFailedException, SubresourceMissingException, 
    or UpdateFailedException if the result is not as expected.
    """
