import signal
import time
import logging


class GracefulDeath:
    """Catch signals to allow graceful shutdown."""

    def __init__(self):
        self.received_term_signal = False
        """if we've recieved a term signal"""
        self.raise_on_signal = False
        """if we raise system exit on signal"""

        term_signals = []
        if hasattr(signal, "CTRL_C_EVENT"):
            term_signals.append(signal.CTRL_C_EVENT)
        if hasattr(signal, "SIGTERM"):
            term_signals.append(signal.SIGTERM)
        if hasattr(signal, "SIGINT"):
            term_signals.append(signal.SIGINT)

        self.term_signals = frozenset(term_signals)
        for signum in self.term_signals:
            try:
                signal.signal(signum, self.handler)
            except ValueError:
                pass

    def handler(self, signum, frame):
        if signum in self.term_signals:
            logging.info(
                f"[Graceful Death] Received signal: {signum}, will terminate ASAP"
            )
            self.received_term_signal = True
            if self.raise_on_signal:
                raise SystemExit(0)


def graceful_sleep(
    gd: GracefulDeath, seconds: float, max_increment: float = 0.1
) -> bool:
    """Attempts to sleep for the given number of fractional seconds, stopping
    early if a term signal is detected

    Args:
        gd (GracefulDeath): The GracefulDeath instance to use
        seconds (float): The number of seconds to sleep
        max_increment (float): The maximum amount of time between death checks

    Returns:
        bool: True if the full duration was slept, false if we were interrupted
    """
    start_time = time.time()
    while True:
        if gd.received_term_signal:
            return False
        remaining_sleep_time = seconds - (time.time() - start_time)
        if remaining_sleep_time < 0.01:
            return True
        try:
            time.sleep(min(max_increment, remaining_sleep_time))
        except (InterruptedError, KeyboardInterrupt):
            gd.received_term_signal = True
            return False
