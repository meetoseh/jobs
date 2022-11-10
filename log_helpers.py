from si_prefix import si_format


def format_bps(time_seconds: float, bytes: int) -> str:
    """Formats the given number of bytes in the given number of seconds into a
    human-readable string representing the transfer speed. This is always in the
    form of two numbers, one for Bps (bytes per second), and one for bps (bits per
    second)

    Examples:
        5000 bytes in 5 seconds is 1 kBps / 8 kbps
        8,192,456 bytes in 7.345 seconds is 1.115 MBps / 8.923 Mbps
    """
    bps = bytes / time_seconds
    return (
        si_format(bps, precision=3) + "Bps / " + si_format(bps * 8, precision=3) + "bps"
    )
