import logging
import os

import sys

from pulse.constants import DEFAULT_LOG_FORMAT

LOG_LEVEL = os.getenv("LOG_LEVEL", logging.INFO)


def _build_plain_formatter() -> logging.Formatter:
    log_format = os.getenv("LOG_FORMAT", DEFAULT_LOG_FORMAT)
    return logging.Formatter(log_format)


def get_logger(logger_name: str | None = None) -> logging.Logger:
    logger = logging.getLogger(logger_name)

    if not logger.handlers:
        # Level
        logger.setLevel(LOG_LEVEL)
        # Formatter
        formatter = _build_plain_formatter()
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


class LoggingMixing:
    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)
