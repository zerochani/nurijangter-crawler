"""
Logging utilities for the NuriJangter crawler.

This module provides structured logging with support for multiple outputs
(console, file) and configurable formats.
"""

import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
import yaml


def setup_logger(
    config_path: Optional[Path] = None,
    log_level: Optional[str] = None,
    log_dir: Optional[Path] = None
) -> None:
    """
    Set up logging configuration.

    Args:
        config_path: Path to logging configuration YAML file
        log_level: Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
    """
    # Ensure log directory exists
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
    else:
        log_dir = Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)

    # Load logging configuration
    if config_path and config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # Update log file paths
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if 'handlers' in config:
                if 'file' in config['handlers']:
                    config['handlers']['file']['filename'] = str(
                        log_dir / f"crawler_{timestamp}.log"
                    )
                if 'error_file' in config['handlers']:
                    config['handlers']['error_file']['filename'] = str(
                        log_dir / f"errors_{timestamp}.log"
                    )

            logging.config.dictConfig(config)
        except Exception as e:
            print(f"Failed to load logging config: {e}", file=sys.stderr)
            _setup_basic_logging(log_level, log_dir)
    else:
        _setup_basic_logging(log_level, log_dir)

    # Override log level if specified
    if log_level:
        logging.getLogger().setLevel(log_level.upper())


def _setup_basic_logging(log_level: Optional[str] = None, log_dir: Optional[Path] = None) -> None:
    """
    Set up basic logging configuration as fallback.

    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
    """
    level = getattr(logging, log_level.upper() if log_level else "INFO")

    # Create formatters
    console_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(console_formatter)

    # File handler
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_handler = logging.FileHandler(
            log_dir / f"crawler_{timestamp}.log",
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(file_formatter)

        # Configure root logger
        logging.basicConfig(
            level=level,
            handlers=[console_handler, file_handler]
        )
    else:
        logging.basicConfig(
            level=level,
            handlers=[console_handler]
        )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.

    Args:
        name: Name for the logger (typically __name__ of the module)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class CrawlerLogger:
    """
    Enhanced logger wrapper with crawler-specific methods.

    Provides convenient methods for logging common crawler events
    with structured information.
    """

    def __init__(self, name: str):
        """Initialize crawler logger."""
        self.logger = logging.getLogger(name)

    def log_page_visit(self, url: str, page_type: str = "unknown") -> None:
        """Log page visit."""
        self.logger.info(f"Visiting {page_type} page: {url}")

    def log_data_extracted(self, item_type: str, count: int) -> None:
        """Log successful data extraction."""
        self.logger.info(f"Extracted {count} {item_type}(s)")

    def log_retry(self, attempt: int, max_attempts: int, error: str) -> None:
        """Log retry attempt."""
        self.logger.warning(
            f"Retry attempt {attempt}/{max_attempts} due to: {error}"
        )

    def log_skip(self, reason: str, details: Optional[str] = None) -> None:
        """Log skipped item."""
        message = f"Skipped: {reason}"
        if details:
            message += f" - {details}"
        self.logger.info(message)

    def log_checkpoint_save(self, checkpoint_id: str) -> None:
        """Log checkpoint save."""
        self.logger.info(f"Checkpoint saved: {checkpoint_id}")

    def log_checkpoint_load(self, checkpoint_id: str) -> None:
        """Log checkpoint load."""
        self.logger.info(f"Checkpoint loaded: {checkpoint_id}")

    def log_error(self, error: Exception, context: Optional[str] = None) -> None:
        """Log error with context."""
        message = f"Error: {str(error)}"
        if context:
            message = f"{context} - {message}"
        self.logger.error(message, exc_info=True)

    def log_crawl_start(self, target: str) -> None:
        """Log crawl start."""
        self.logger.info(f"=" * 60)
        self.logger.info(f"Starting crawl: {target}")
        self.logger.info(f"=" * 60)

    def log_crawl_complete(self, total_items: int, duration: float) -> None:
        """Log crawl completion."""
        self.logger.info(f"=" * 60)
        self.logger.info(f"Crawl completed: {total_items} items in {duration:.2f}s")
        self.logger.info(f"=" * 60)

    def __getattr__(self, name):
        """Delegate unknown attributes to underlying logger."""
        return getattr(self.logger, name)
