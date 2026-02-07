"""
Scheduler for running crawler on intervals or cron schedule.

This module provides scheduling capabilities for automated crawler runs.
"""

import time
import signal
import sys
from typing import Callable, Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
import logging

try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False

logger = logging.getLogger(__name__)


class CronScheduler:
    """
    Scheduler for periodic crawler execution.

    Supports both interval-based (every N hours) and cron-expression-based scheduling.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scheduler.

        Args:
            config: Configuration dictionary with scheduler settings
        """
        self.config = config
        self.enabled = config.get('enabled', False)
        self.mode = config.get('mode', 'interval')  # 'interval' or 'cron'
        self.interval_hours = config.get('interval', {}).get('hours', 6)
        self.cron_expression = config.get('cron', {}).get('expression', '0 */6 * * *')

        self.is_running = False
        self.last_run_time: Optional[datetime] = None

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.is_running = False

    def start(self, crawler_func: Callable[[], Any]) -> None:
        """
        Start the scheduler.

        Args:
            crawler_func: Function to call for each crawl run
        """
        if not self.enabled:
            logger.info("Scheduler is disabled, running crawler once")
            self._run_crawler(crawler_func)
            return

        logger.info(f"Starting scheduler in {self.mode} mode")
        self.is_running = True

        if self.mode == 'interval':
            self._run_interval_mode(crawler_func)
        elif self.mode == 'cron':
            self._run_cron_mode(crawler_func)
        else:
            raise ValueError(f"Invalid scheduler mode: {self.mode}")

    def _run_interval_mode(self, crawler_func: Callable[[], Any]) -> None:
        """
        Run in interval mode.

        Args:
            crawler_func: Function to call for each crawl run
        """
        logger.info(f"Running in interval mode: every {self.interval_hours} hours")

        while self.is_running:
            try:
                # Run crawler
                self._run_crawler(crawler_func)

                # Calculate next run time
                next_run = datetime.now() + timedelta(hours=self.interval_hours)
                logger.info(f"Next run scheduled at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

                # Sleep until next run
                self._sleep_until(next_run)

            except KeyboardInterrupt:
                logger.info("Scheduler interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                # Wait a bit before retrying
                time.sleep(60)

        logger.info("Scheduler stopped")

    def _run_cron_mode(self, crawler_func: Callable[[], Any]) -> None:
        """
        Run in cron mode.

        Args:
            crawler_func: Function to call for each crawl run
        """
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "croniter package is required for cron mode. "
                "Install with: pip install croniter"
            )

        logger.info(f"Running in cron mode: {self.cron_expression}")

        # Validate cron expression
        if not croniter.is_valid(self.cron_expression):
            raise ValueError(f"Invalid cron expression: {self.cron_expression}")

        cron = croniter(self.cron_expression, datetime.now())

        while self.is_running:
            try:
                # Get next run time
                next_run = cron.get_next(datetime)
                logger.info(f"Next run scheduled at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

                # Sleep until next run
                self._sleep_until(next_run)

                if not self.is_running:
                    break

                # Run crawler
                self._run_crawler(crawler_func)

            except KeyboardInterrupt:
                logger.info("Scheduler interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                # Wait a bit before retrying
                time.sleep(60)

        logger.info("Scheduler stopped")

    def _run_crawler(self, crawler_func: Callable[[], Any]) -> None:
        """
        Run the crawler function.

        Args:
            crawler_func: Function to call
        """
        try:
            start_time = datetime.now()
            logger.info(f"Starting crawler run at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Run crawler
            result = crawler_func()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"Crawler run completed in {duration:.2f} seconds")
            self.last_run_time = start_time

            return result

        except Exception as e:
            logger.error(f"Crawler run failed: {e}", exc_info=True)
            raise

    def _sleep_until(self, target_time: datetime) -> None:
        """
        Sleep until target time, with periodic wake-ups to check for shutdown.

        Args:
            target_time: Time to sleep until
        """
        while self.is_running:
            now = datetime.now()
            if now >= target_time:
                break

            # Sleep in small intervals to allow checking for shutdown
            sleep_seconds = (target_time - now).total_seconds()
            sleep_time = min(sleep_seconds, 60)  # Wake up at least every minute

            if sleep_time > 0:
                time.sleep(sleep_time)

    def stop(self) -> None:
        """Stop the scheduler."""
        logger.info("Stopping scheduler...")
        self.is_running = False

    def get_status(self) -> Dict[str, Any]:
        """
        Get scheduler status.

        Returns:
            Dictionary with status information
        """
        return {
            'enabled': self.enabled,
            'mode': self.mode,
            'is_running': self.is_running,
            'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
            'interval_hours': self.interval_hours if self.mode == 'interval' else None,
            'cron_expression': self.cron_expression if self.mode == 'cron' else None
        }


def run_scheduled_crawler(config: Dict[str, Any], crawler_func: Callable[[], Any]) -> None:
    """
    Helper function to run crawler with scheduler.

    Args:
        config: Configuration dictionary
        crawler_func: Function to call for each crawl run
    """
    scheduler_config = config.get('scheduler', {})
    scheduler = CronScheduler(scheduler_config)

    try:
        scheduler.start(crawler_func)
    except KeyboardInterrupt:
        logger.info("Scheduler interrupted, shutting down...")
        scheduler.stop()
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        raise
