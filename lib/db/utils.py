"""Utils for generating parameterized queries which are slightly complicated
but not enough to warrant pypika.
"""

from dataclasses import dataclass
from pypika.terms import Criterion, Term, ComplexCriterion, ValueWrapper
from pypika.enums import Comparator
from typing import Any, Dict, List, Optional, Union, cast


def question_mark_list(num: int) -> str:
    """Return a list of `num` question marks.

    Args:
        num (int): Number of question marks to return.

    Returns:
        list: List of question marks.
    """
    return ",".join(["?"] * num)


def _two_bytes_hash(view: memoryview) -> int:
    return view[0] * 256 + view[1]


def handle_parameters_with_unknown_position(
    query: str, qargs: list, substitutions: Dict[str, Any]
) -> str:
    """Sometimes it is particularly onerous to produce the qargs list in the
    order that parameters are expected in the query. This function will replace
    any of the substitutions in the query with question marks and add the
    substitution values to the qargs list.

    This depends on the fact that the query is fully parameterized, so if there
    is a question mark in the query it's a parameter, and if this encounters a
    substitution key, it's a parameter. This would not necessarily be true if
    the user had control over the query, such as when using raw string concatenation!

    This only works if the query is ascii only, though the qargs and substitution
    values can be unicode.

    Substitutions, where relevant, are checked in the order that substitutions yields
    them, which is guarranteed to be consistent and controllable since python 3.7:
    https://mail.python.org/pipermail/python-dev/2017-December/151283.html

    This means that if both `:id:` and `:id:2` are substitution targets (not
    recommended, but possible), then in order for `:id:2` to ever work, it must
    be inserted into the substitutions dictionary first.

    Example:

    ```py
    query = "SELECT * FROM table WHERE id = :id:"
    qargs = []
    substitutions = {":id:": 1}

    result = handle_parameters_with_unknown_position(query, qargs, substitutions)
    print(f"{result=}")  # result='SELECT * FROM table WHERE id = ?'
    print(f"{qargs=}")  # qargs=[1]
    ```

    PERF:
        This is fairly slow, worst case O(max(nm, pq)) where n is the length of
        the query, m is maximum number of duplicates in substitutions keyed by
        their first two characters, p is the number of substitutions required,
        and q is the length of qargs, so should be avoided where necessary.

    Args:
        query (str): The query to modify.
        qargs (list): The list of arguments to add to.
        substitutions (Dict[str, Any]): The substitutions to make. Each must be
          at least 2 characters long

    Returns:
        str: The modified query.
    """
    if not substitutions:
        return query

    if any(len(k) < 2 for k in substitutions):
        raise ValueError("substitutions must be at least 2 characters long")

    bquery = memoryview(query.encode("ascii", errors="strict"))
    bsubstitutions = {
        memoryview(k.encode("ascii", errors="strict")): v
        for k, v in substitutions.items()
    }

    # substitutions typically have a marker character, so it's not
    # helpful to break them down by the first character alone, but
    # two should do reasonably well
    substitutions_by_first_2_chars: Dict[int, List[memoryview]] = dict()
    for k in bsubstitutions:
        first_2_chars = _two_bytes_hash(k)
        if first_2_chars not in substitutions_by_first_2_chars:
            substitutions_by_first_2_chars[first_2_chars] = []
        substitutions_by_first_2_chars[first_2_chars].append(k)

    result_parts: List[Union[memoryview, bytes]] = []

    consumed_until = 0  # exclusive
    idx = 0
    parameter_idx = 0

    parameter_identifier = ord("?")

    while idx < len(bquery) - 1:
        if bquery[idx] == parameter_identifier:
            parameter_idx += 1
            idx += 1
            continue

        peek_2 = _two_bytes_hash(bquery[idx : idx + 2])
        possible_subs = substitutions_by_first_2_chars.get(peek_2)
        if possible_subs is None:
            idx += 1
            continue

        for sub in possible_subs:
            if bquery[idx : idx + len(sub)] == sub:
                result_parts.append(bquery[consumed_until:idx])
                qargs.insert(parameter_idx, bsubstitutions[sub])
                result_parts.append(b"?")
                consumed_until = idx + len(sub)
                idx = consumed_until
                parameter_idx += 1
                break
        else:
            idx += 1

    result_parts.append(bquery[consumed_until:])

    total_len = sum(len(p) for p in result_parts)
    result = bytearray(total_len)
    idx = 0
    for p in result_parts:
        result[idx : idx + len(p)] = p
        idx += len(p)

    return result.decode("ascii", errors="strict")


class StringComparator(Comparator):
    concat = "||"


def sqlite_string_concat(a: Union[str, Term], b: Union[str, Term]) -> ComplexCriterion:
    """Returns the sqlite string concat operator for the two terms,
    e.g., a || b.

    This will give null if either the left or right is null in
    sqlite
    """
    return ComplexCriterion(
        StringComparator.concat,
        a if not isinstance(a, str) else ValueWrapper(a),
        b if not isinstance(b, str) else ValueWrapper(b),
    )


class ParenthisizeCriterion(Criterion):
    """A criterion which will be parenthesized.

    Args:
        criterion (Criterion): The criterion to parenthesize.
    """

    def __init__(self, criterion: Criterion, alias: Optional[str] = None):
        super().__init__(alias=alias)
        self.criterion = criterion

    def get_sql(self, *args, **kwargs) -> str:
        _as = f' AS "{self.alias}"' if self.alias else ""
        return f"({self.criterion.get_sql(*args, **kwargs)}){_as}"


class CaseInsensitiveCriterion(Criterion):
    """A criterion which we add COLLATE NO TEXT to, e.g.,
    users.email = ? COLLATE NOCASE

    Args:
        criterion (Criterion): The criterion to perform case insensitively.
    """

    def __init__(self, criterion: Criterion):
        self.criterion = criterion

    def get_sql(self, *args, **kwargs) -> str:
        return f"{self.criterion.get_sql(*args, **kwargs)} COLLATE NOCASE"


class EscapeCriterion(Criterion):
    """A criterion which we add ESCAPE '\\' to, e.g.,
    users.email LIKE ? ESCAPE '\\'

    Args:
        criterion (Criterion): The criterion to perform case insensitively.
    """

    def __init__(self, criterion: Criterion, character="\\"):
        assert len(character) == 1, "only single characters allowed"
        assert character != "'", "cannot use a quote as the escape character"
        self.criterion = criterion
        self.character = character

    def get_sql(self, *args, **kwargs) -> str:
        return f"{self.criterion.get_sql(*args, **kwargs)} ESCAPE '{self.character}'"


@dataclass
class TermWithParameters:
    """Describes a term with any number of ordered parameters."""

    term: Term
    """The term to use."""

    parameters: List[Any]
    """The parameters that are used with the term"""


class ShieldFields(Criterion):
    """Equivalent to the underlying criterion, except shields the _fields parameter
    so it won't e.g. be considered by the join validator.
    """

    def __init__(self, container, alias=None):
        super().__init__(alias)
        self.container = container
        self._is_negated = False

    def get_sql(self, **kwargs):
        return self.container.get_sql(**kwargs)

    def negate(self):
        self.container = self.container.negate()
        return cast(Any, self)

    def _fields(self):
        return []
