def clean_for_slack(s: str) -> str:
    return s.replace("`", "BACKTICK").replace("\\", "BACKSLASH")
