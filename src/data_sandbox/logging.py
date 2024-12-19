import logging
import time
from functools import wraps
from typing import Optional


def setup_logging(log_level: int = logging.INFO) -> logging.Logger:
    """
    Initialize console-only logging configuration with performance-optimized formatting.
    """
    logger = logging.getLogger("app")
    logger.setLevel(log_level)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(module)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def measure_time(custom_logger: Optional[logging.Logger] = None):
    """
    Execution time decorator
    """
    log = custom_logger or logging.getLogger("app")

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start

            hours, remainder = divmod(elapsed, 3600)
            minutes, seconds = divmod(remainder, 60)
            formatted_time = (
                f"{int(hours):02d}:{int(minutes):02d}:{seconds:06.3f}"
            )

            log.info("%s execution time: %s", func.__name__, formatted_time)
            return result

        return wrapper

    return decorator
