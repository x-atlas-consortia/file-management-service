"""
timed_step.py

Shared timing instrumentation. Wrap any block in `with TimedStep(logger, "description"):`
to log how long it took - this is how the original per-operation cost breakdown (SQLite
queries, UUID API calls, ES reads/writes) was built for HuBMAP's nightly pipeline, letting
slow operations be identified from real logs rather than guessed at.
"""
import logging
import time


class TimedStep:
    """Context manager that times the wrapped block and logs its duration via the
    given logger at INFO level, but only if the elapsed time exceeds `threshold`
    seconds - so routine, fast operations don't clutter the log. Pass threshold=0.0
    to always log, regardless of duration.

    Usage:
        with TimedStep(logger, f"SQLite query_files for dataset {uuid}"):
            ...

    Logs even if the wrapped block raises - the duration up to the exception is
    still useful, and the exception itself propagates normally afterward.
    """

    def __init__(self, logger: logging.Logger, description: str, threshold: float = 0.1):
        self.logger = logger
        self.description = description
        self.threshold = threshold
        self._start = None

    def __enter__(self) -> "TimedStep":
        self._start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        elapsed = time.time() - self._start
        if elapsed > self.threshold:
            self.logger.info(f"{self.description}: {elapsed:.3f}s")
        return False  # never suppress an exception
