from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode, parse_qs


@dataclass
class UTM:
    source: str
    medium: Optional[str] = None
    campaign: Optional[str] = None
    term: Optional[str] = None
    content: Optional[str] = None


def get_canonical_utm_representation(
    *,
    utm_source: str,
    utm_medium: Optional[str] = None,
    utm_campaign: Optional[str] = None,
    utm_term: Optional[str] = None,
    utm_content: Optional[str] = None,
) -> str:
    """Produces the canonical representation of these utm tags as they would be
    url-encoded with nulls/blanks omitted and keys in ascending alphabetical order.
    Examples:

    - `utm_source=google`
    - `utm_campaign=summer-sale-2023&utm_medium=cpc&utm_source=facebook&utm_term=meditation+app`
    """
    utm_dict = {}
    if utm_campaign:
        utm_dict["utm_campaign"] = utm_campaign
    if utm_content:
        utm_dict["utm_content"] = utm_content
    if utm_medium:
        utm_dict["utm_medium"] = utm_medium
    if utm_source:
        utm_dict["utm_source"] = utm_source
    if utm_term:
        utm_dict["utm_term"] = utm_term

    return urlencode(utm_dict)


def get_canonical_utm_representation_from_wrapped(utm: UTM) -> str:
    """Produces the canonical representation of these utm tags as they would be
    url-encoded with nulls/blanks omitted and keys in ascending alphabetical order.
    Examples:

    - `utm_source=google`
    - `utm_campaign=summer-sale-2023&utm_medium=cpc&utm_source=facebook&utm_term=meditation+app`
    """
    return get_canonical_utm_representation(
        utm_source=utm.source,
        utm_medium=utm.medium,
        utm_campaign=utm.campaign,
        utm_term=utm.term,
        utm_content=utm.content,
    )


def get_utm_parts(query_param: str) -> Optional[UTM]:
    """Parses a query param representation of utm tags and returns a UTM
    object with the parts. If there are duplicates for any of the keys, or
    an issue parsing, an exception will be raised. If the source is missing,
    returns None.

    Examples:
    - `utm_source=google` -> `UTM(source="google")`
    - `utm_campaign=summer-sale-2023&utm_medium=cpc&utm_source=facebook&utm_term=meditation+app` ->
        `UTM(source="facebook", medium="cpc", campaign="summer-sale-2023", term="meditation app")`

    Args:
        query_param: The query param representation of utm tags. Must be
            url-encoded and have the leading `?` removed.

    Returns:
        A UTM object with the parts, or None if the source is missing.
    """
    parsed = parse_qs(
        query_param, keep_blank_values=False, strict_parsing=True, errors="strict"
    )
    if any(len(v) > 1 for v in parsed.values()):
        raise ValueError("Duplicate keys in query param")

    source = parsed.get("utm_source", [""])[0].strip()
    if source == "" or len(source) > 255:
        raise ValueError("Invalid source")

    medium = parsed.get("utm_medium", [""])[0].strip()
    if medium == "":
        medium = None
    elif len(medium) > 255:
        raise ValueError("Invalid medium")

    campaign = parsed.get("utm_campaign", [""])[0].strip()
    if campaign == "":
        campaign = None
    elif len(campaign) > 255:
        raise ValueError("Invalid campaign")

    term = parsed.get("utm_term", [""])[0].strip()
    if term == "":
        term = None
    elif len(term) > 255:
        raise ValueError("Invalid term")

    content = parsed.get("utm_content", [""])[0].strip()
    if content == "":
        content = None
    elif len(content) > 255:
        raise ValueError("Invalid content")

    return UTM(
        source=source, medium=medium, campaign=campaign, term=term, content=content
    )
