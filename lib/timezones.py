import json
from typing import Literal


TimezoneTechniqueSlug = Literal["browser", "app", "app-guessed"]


def convert_timezone_technique_slug_to_db(
    timezone_technique: TimezoneTechniqueSlug,
) -> str:
    """Converts the given timezone technique slug, as specified in requests,
    to the value that we store in the database. We disambiguate the combined
    terms to make processing simpler.
    """
    if timezone_technique == "app-guessed":
        return json.dumps({"style": "app", "guessed": True}, sort_keys=True)
    elif timezone_technique == "app":
        return json.dumps({"style": "app", "guessed": False}, sort_keys=True)
    else:
        return json.dumps({"style": timezone_technique})
