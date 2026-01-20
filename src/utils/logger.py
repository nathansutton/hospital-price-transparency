"""Structured logging setup using structlog.

Provides consistent, machine-readable logging with context propagation.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog


def setup_logging(
    log_level: str = "INFO",
    log_dir: Path | None = None,
    json_logs: bool = False,
) -> None:
    """Configure structured logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files. If None, logs only to console.
        json_logs: If True, output JSON formatted logs (useful for log aggregation)
    """
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )

    # Set up processors for structlog
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_logs:
        # Machine-readable JSON output
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Human-readable console output
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            ),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Set up file logging if log_dir is specified
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"scrape_{timestamp}.log"

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))

        # Use JSON format for file logs (easier to parse)
        file_formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=shared_processors,
        )
        file_handler.setFormatter(file_formatter)

        # Add to root logger
        logging.getLogger().addHandler(file_handler)


def get_logger(name: str | None = None, **initial_context: Any) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name (typically __name__ of the calling module)
        **initial_context: Initial context to bind to all log messages

    Returns:
        A bound logger instance with the specified context
    """
    logger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger


class ScrapeLogContext:
    """Context manager for scrape operation logging.

    Provides structured logging around scrape operations with automatic
    timing and error capture.
    """

    def __init__(self, logger: structlog.stdlib.BoundLogger, hospital_npi: str, hospital_name: str):
        self.logger = logger.bind(hospital_npi=hospital_npi, hospital_name=hospital_name)
        self.hospital_npi = hospital_npi
        self.hospital_name = hospital_name
        self.start_time: datetime | None = None
        self.records_scraped: int = 0

    def __enter__(self) -> "ScrapeLogContext":
        self.start_time = datetime.now()
        self.logger.info("scrape_started")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0

        if exc_val is not None:
            self.logger.error(
                "scrape_failed",
                error_type=exc_type.__name__ if exc_type else "Unknown",
                error_message=str(exc_val),
                duration_seconds=duration,
            )
            return False  # Re-raise the exception

        self.logger.info(
            "scrape_completed",
            records_scraped=self.records_scraped,
            duration_seconds=duration,
        )
        return False

    def set_records_scraped(self, count: int) -> None:
        """Record the number of records successfully scraped."""
        self.records_scraped = count
