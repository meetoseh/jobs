def format_duration(seconds: float) -> str:
    """Formats the given seconds in HH:MM:SS format, with the hours omitted if the
    duration is less than one hour.
    """
    hours = int(seconds / 3600)
    minutes = int((seconds % 3600) / 60)
    seconds = int(seconds % 60)

    return (
        f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        if hours
        else f"{minutes:02d}:{seconds:02d}"
    )
