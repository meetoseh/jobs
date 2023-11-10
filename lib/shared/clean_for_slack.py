import string


def clean_for_slack(s: str) -> str:
    """Cleans the given string so it can be included in a code block in a
    Slack message without breaking markdown
    """
    s = (
        s.replace("`", "BACKTICK")
        .replace("\\", "BACKSLASH")
        .replace("{", "{{")
        .replace("}", "}}")
    )
    if any(c not in string.printable for c in s):
        s = "".join(c if c in string.printable else f"\\x{ord(c):02x}" for c in s)
    return s


_slack_safe = string.ascii_letters + string.digits + " :,.-"


def clean_for_non_code_slack(s: str) -> str:
    """Cleans the given string so it can be included in a Slack message without
    breaking markdown
    """
    if any(c not in _slack_safe for c in s):
        s = "".join(c if c in _slack_safe else f"\\x{ord(c):02x}" for c in s)
    return s


def clean_for_preview(s: str) -> str:
    """Cleans the given string so it can be included in a slack preview, i.e.,
    removes markdown formatting without substitution
    """
    if any(c not in _slack_safe for c in s):
        s = "".join(c for c in s if c in _slack_safe)
    return s
