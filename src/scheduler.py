"""
Scheduler module for running crawler on a schedule.

Supports both cron-style schedules and simple interval-based execution.
"""

import time
import logging
from typing import Callable, Optional, Dict, Any
from datetime import datetime, timedelta
from threading import Thread, Event
import schedule  # Using schedule library for simplicity

logger = logging.getLogger(__name__)


class CronScheduler:
    """
    Scheduler for running crawler jobs at specified intervals or times.

    Supports:
    - Interval-based execution (e.g., every 6 hours)
    - Time-based execution (e.g., daily at 02:00)
    - Cron-like expressions (via schedule library)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scheduler.

        Args:
            config: Scheduler configuration dictionary
                - mode: 'interval' or 'time'
                - interval_hours: For interval mode (e.g., 6)
                - time: For time mode (e.g., "02:00")
                - days: For weekly schedule (e.g., ["monday", "wednesday"])
        """
        self.config = config
        self.mode = config.get('mode', 'interval')
        
        # Handle nested config structure (from config.yaml)
        if 'interval' in config and isinstance(config['interval'], dict):
            self.interval_hours = config['interval'].get('hours', 24)
        else:
            self.interval_hours = config.get('interval_hours', 24)
            
        if 'cron' in config and isinstance(config['cron'], dict):
             # Map cron expression to simple time if possible, or just note it
             # For now, let's keep time_str support 
             self.time_str = config.get('time', '02:00')
        else:
            self.time_str = config.get('time', '02:00')

        self.days = config.get('days', None)  # For weekly schedules

        self.is_running = False
        self.stop_event = Event()
        self.thread: Optional[Thread] = None

    def start(self, job_func: Callable):
        """
        Start the scheduler with the given job function.

        Args:
            job_func: Function to call on schedule (should be the crawler)
        """
        if self.is_running:
            logger.warning("Scheduler is already running")
            return

        logger.info(f"Starting scheduler in {self.mode} mode")

        # Clear any existing schedules
        schedule.clear()

        # Setup schedule based on mode
        if self.mode == 'interval':
            # Run every N hours
            schedule.every(self.interval_hours).hours.do(self._safe_job_wrapper, job_func)
            logger.info(f"Scheduled to run every {self.interval_hours} hours")

        elif self.mode == 'time':
            # Run at specific time(s)
            if self.days:
                # Weekly schedule
                for day in self.days:
                    day_lower = day.lower()
                    if day_lower == 'monday':
                        schedule.every().monday.at(self.time_str).do(self._safe_job_wrapper, job_func)
                    elif day_lower == 'tuesday':
                        schedule.every().tuesday.at(self.time_str).do(self._safe_job_wrapper, job_func)
                    elif day_lower == 'wednesday':
                        schedule.every().wednesday.at(self.time_str).do(self._safe_job_wrapper, job_func)
                    elif day_lower == 'thursday':
                        schedule.every().thursday.at(self.time_str).do(self._safe_job_wrapper, job_func)
                    elif day_lower == 'friday':
                        schedule.every().friday.at(self.time_str).do(self._safe_job_wrapper, job_func)
                    elif day_lower == 'saturday':
                        schedule.every().saturday.at(self.time_str).do(self._safe_job_wrapper, job_func)
                    elif day_lower == 'sunday':
                        schedule.every().sunday.at(self.time_str).do(self._safe_job_wrapper, job_func)
                logger.info(f"Scheduled to run on {self.days} at {self.time_str}")
            else:
                # Daily schedule
                schedule.every().day.at(self.time_str).do(self._safe_job_wrapper, job_func)
                logger.info(f"Scheduled to run daily at {self.time_str}")

        else:
            raise ValueError(f"Invalid scheduler mode: {self.mode}. Must be 'interval' or 'time'")

        # Run immediately on start if configured
        if self.config.get('run_on_start', True):
            logger.info("Running job immediately on start...")
            self._safe_job_wrapper(job_func)

        # Start scheduler thread
        self.is_running = True
        self.stop_event.clear()
        self.thread = Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()

        logger.info("Scheduler started. Press Ctrl+C to stop.")

        # Keep main thread alive
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
            self.stop()

    def _run_scheduler(self):
        """Internal scheduler loop."""
        while self.is_running and not self.stop_event.is_set():
            schedule.run_pending()
            time.sleep(1)

    def _safe_job_wrapper(self, job_func: Callable):
        """
        Wrap job function with error handling.

        Args:
            job_func: The actual job function to run
        """
        try:
            logger.info("=" * 70)
            logger.info(f"Starting scheduled job at {datetime.now().isoformat()}")
            logger.info("=" * 70)

            result = job_func()

            logger.info("=" * 70)
            logger.info(f"Scheduled job completed at {datetime.now().isoformat()}")
            logger.info("=" * 70)

            return result

        except Exception as e:
            logger.error(f"Scheduled job failed: {e}", exc_info=True)
            # Don't stop scheduler on job failure

    def stop(self):
        """Stop the scheduler."""
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return

        logger.info("Stopping scheduler...")
        self.is_running = False
        self.stop_event.set()

        if self.thread:
            self.thread.join(timeout=5)

        schedule.clear()
        logger.info("Scheduler stopped")

    def get_next_run(self) -> Optional[datetime]:
        """
        Get the next scheduled run time.

        Returns:
            Next run datetime or None if no jobs scheduled
        """
        jobs = schedule.get_jobs()
        if not jobs:
            return None

        next_run = schedule.next_run()
        return next_run

    def get_status(self) -> Dict[str, Any]:
        """
        Get scheduler status.

        Returns:
            Dictionary with status information
        """
        return {
            'is_running': self.is_running,
            'mode': self.mode,
            'config': self.config,
            'next_run': self.get_next_run().isoformat() if self.get_next_run() else None,
            'jobs_count': len(schedule.get_jobs())
        }


class IntervalScheduler:
    """
    Simple interval-based scheduler.

    Runs a job repeatedly with a fixed delay between executions.
    Simpler than CronScheduler but less flexible.
    """

    def __init__(self, interval_seconds: int, run_on_start: bool = True):
        """
        Initialize interval scheduler.

        Args:
            interval_seconds: Seconds between job executions
            run_on_start: Whether to run immediately on start
        """
        self.interval_seconds = interval_seconds
        self.run_on_start = run_on_start
        self.is_running = False
        self.stop_event = Event()
        self.thread: Optional[Thread] = None

    def start(self, job_func: Callable):
        """
        Start the scheduler.

        Args:
            job_func: Function to call on interval
        """
        if self.is_running:
            logger.warning("Interval scheduler is already running")
            return

        logger.info(f"Starting interval scheduler (every {self.interval_seconds}s)")

        self.is_running = True
        self.stop_event.clear()

        def run_loop():
            # Run immediately if configured
            if self.run_on_start:
                try:
                    logger.info("Running job immediately on start...")
                    job_func()
                except Exception as e:
                    logger.error(f"Initial job run failed: {e}", exc_info=True)

            # Main loop
            while self.is_running and not self.stop_event.is_set():
                # Wait for interval
                if self.stop_event.wait(timeout=self.interval_seconds):
                    break  # Stop requested

                # Run job
                try:
                    logger.info(f"Running scheduled job at {datetime.now().isoformat()}")
                    job_func()
                except Exception as e:
                    logger.error(f"Scheduled job failed: {e}", exc_info=True)

        self.thread = Thread(target=run_loop, daemon=True)
        self.thread.start()

        logger.info("Interval scheduler started")

        # Keep main thread alive
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
            self.stop()

    def stop(self):
        """Stop the scheduler."""
        if not self.is_running:
            logger.warning("Interval scheduler is not running")
            return

        logger.info("Stopping interval scheduler...")
        self.is_running = False
        self.stop_event.set()

        if self.thread:
            self.thread.join(timeout=5)

        logger.info("Interval scheduler stopped")
