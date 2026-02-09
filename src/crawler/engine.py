
"""
Main crawler engine for NuriJangter.

This module orchestrates the crawling process, including page navigation,
data extraction, storage, checkpointing, and error handling.
"""

import time
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

from .browser import BrowserManager
from .interface import BaseCrawler
from .navigator import Navigator
from .processor import NoticeProcessor
from .retry_manager import RetryManager

from ..parser import ListPageParser, DetailPageParser
from ..storage import JSONStorage, CSVStorage
from ..models import BidNotice, BidNoticeList
from ..checkpoint import CheckpointManager, CrawlState
from ..utils import CrawlerLogger, DeduplicationManager

logger = logging.getLogger(__name__)


class CrawlerEngine(BaseCrawler):
    """
    Main crawler engine that orchestrates the entire crawling process.

    Handles:
    - Browser management
    - Component coordination (Navigator, Processor, RetryManager)
    - Deduplication
    - Checkpoint management
    - Error handling and retry logic
    - Rate limiting
    - Data storage
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize crawler engine.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = CrawlerLogger(__name__)

        # Initialize base components
        self.browser_manager = BrowserManager(config.get('crawler', {}))
        self.list_parser = ListPageParser(config)
        self.detail_parser = DetailPageParser(config)

        # Initialize checkpoint manager
        checkpoint_config = config.get('checkpoint', {})
        self.checkpoint_manager = CheckpointManager(
            checkpoint_dir=Path(checkpoint_config.get('directory', 'checkpoints')),
            checkpoint_file=checkpoint_config.get('filename', 'crawler_checkpoint.json'),
            save_interval=checkpoint_config.get('save_interval', 10)
        )

        # Initialize deduplication manager
        dedup_config = config.get('deduplication', {})
        self.dedup_manager = DeduplicationManager(
            key_fields=dedup_config.get('key_fields', ['bid_notice_number']),
            storage_file=Path(dedup_config.get('storage_file', 'checkpoints/seen_items.json')),
            enabled=dedup_config.get('enabled', True)
        )

        # Initialize storage
        storage_config = config.get('storage', {})
        output_dir = Path(storage_config.get('output_dir', 'data'))

        self.storages = []
        for format_type in storage_config.get('formats', ['json', 'csv']):
            if format_type == 'json':
                self.storages.append(JSONStorage(output_dir, storage_config.get('json', {})))
            elif format_type == 'csv':
                self.storages.append(CSVStorage(output_dir, storage_config.get('csv', {})))

        # Crawler settings
        crawler_config = config.get('crawler', {})
        self.pagination_config = crawler_config.get('pagination', {})
        
        # Collected data
        self.collected_notices = BidNoticeList()

        # Statistics
        self.stats = {
            'pages_crawled': 0,
            'items_extracted': 0,
            'items_skipped': 0,
            'errors': 0
        }

        # --- Refactored Components ---
        self.navigator = Navigator(config)
        
        self.processor = NoticeProcessor(
            navigator=self.navigator,
            list_parser=self.list_parser,
            detail_parser=self.detail_parser,
            dedup_manager=self.dedup_manager,
            checkpoint_manager=self.checkpoint_manager,
            collected_notices=self.collected_notices,
            stats=self.stats
        )

        self.retry_manager = RetryManager(
            config=config,
            navigator=self.navigator,
            processor=self.processor,
            checkpoint_manager=self.checkpoint_manager,
            dedup_manager=self.dedup_manager,
            save_callback=self._save_data
        )

    def run(self, resume: bool = True) -> BidNoticeList:
        """
        Run the crawler.

        Args:
            resume: Whether to resume from checkpoint if available

        Returns:
            BidNoticeList with collected data
        """
        start_time = time.time()
        self.logger.log_crawl_start(self.config.get('website', {}).get('base_url', 'NuriJangter'))

        try:
            # Load checkpoint if resuming
            if resume and self.checkpoint_manager.load_checkpoint():
                self.logger.log_checkpoint_load(
                    f"page {self.checkpoint_manager.current_page}"
                )
            else:
                self.checkpoint_manager.initialize_crawl({
                    'target': 'NuriJangter',
                    'config': self.config.get('website', {})
                })

            # Start browser
            with self.browser_manager as browser:
                page = browser.get_page()

                # Navigate to list page
                list_url = self.config.get('website', {}).get('list_page_url', '')
                if not list_url:
                    raise ValueError("List page URL not configured")

                self.navigator.navigate_to_page(page, list_url)

                # Crawl pages
                self._crawl_list_pages(page)

            # Mark crawl as complete
            self.checkpoint_manager.complete_crawl(success=True)

            # Save collected data
            self._save_data()

            # Save deduplication data
            self.dedup_manager.save()

            # Calculate duration
            duration = time.time() - start_time
            self.logger.log_crawl_complete(
                total_items=len(self.collected_notices.notices),
                duration=duration
            )

            return self.collected_notices

        except KeyboardInterrupt:
            self.logger.warning("Crawl interrupted by user")
            self.checkpoint_manager.set_state(CrawlState.PAUSED)
            self.checkpoint_manager.save_checkpoint(force=True)
            self.dedup_manager.save()
            raise

        except Exception as e:
            self.logger.log_error(e, "Crawl failed")
            self.stats['errors'] += 1
            self.checkpoint_manager.complete_crawl(success=False)
            raise

    def retry_failed_items(self) -> None:
        """
        Retry processing items that failed in previous crawls.
        """
        # Start browser management here, delegate retry logic to manager
        try:
            with self.browser_manager as browser:
                page = browser.get_page()
                self.retry_manager.process_retries(page)
        except Exception as e:
            self.logger.error(f"Retry process failed: {e}")


    def _crawl_list_pages(self, page) -> None:
        """
        Crawl all list pages.

        Args:
            page: Playwright page object
        """
        max_pages = self.pagination_config.get('max_pages', 0)
        current_page_num = self.checkpoint_manager.current_page

        while True:
            try:
                # Check if we've reached max pages
                if max_pages > 0 and current_page_num > max_pages:
                    self.logger.info(f"Reached max pages limit: {max_pages}")
                    break

                self.logger.info(f"Crawling page {current_page_num}")

                # Wait for page to load
                self.navigator.wait_for_page_load(page)

                # Extract notices from list page
                notices_data = self.list_parser.parse_page(page)

                self.logger.log_data_extracted("bid notice", len(notices_data))
                self.stats['pages_crawled'] += 1

                # Process each notice
                for idx, notice_data in enumerate(notices_data):
                    self.processor.process_notice(page, notice_data, current_page_num)
                    
                    # Check for early exit (Accessed via processor state if needed, or moved to processor)
                    # For now, let's keep early exit check here by accessing processor's state
                    if self.processor.consecutive_duplicates >= self.processor.early_exit_threshold:
                        self.logger.info(f"Early exit triggered: {self.processor.consecutive_duplicates} consecutive duplicates found.")
                        return  # Exit function completely

                # Check for next page
                if self.list_parser.has_next_page(page):
                    # Navigate to next page
                    if self.list_parser.go_to_next_page(page):
                        current_page_num += 1
                        self.checkpoint_manager.current_page = current_page_num
                        self.checkpoint_manager.advance_page()

                        # Rate limiting
                        self.navigator.rate_limit()
                    else:
                        self.logger.warning("Failed to navigate to next page")
                        break
                else:
                    self.logger.info("No more pages to crawl")
                    break

            except Exception as e:
                self.logger.error(f"Error crawling page {current_page_num}: {e}")
                self.stats['errors'] += 1

                # Decide whether to continue or abort
                if self.stats['errors'] > 10:
                    self.logger.error("Too many errors, aborting crawl")
                    break

                # Try to continue with next page
                current_page_num += 1
                continue

    def _save_data(self) -> None:
        """Save collected data to storage."""
        if not self.collected_notices.notices:
            self.logger.warning("No data to save")
            return

        # Sort notices by announcement_date (desc) then bid_notice_number (desc)
        # Using a stable sort sequence (primary key last)
        self.collected_notices.notices.sort(
            key=lambda x: (x.announcement_date or "", x.bid_notice_number), 
            reverse=True
        )

        # Convert to dictionaries
        data = [notice.to_dict() for notice in self.collected_notices.notices]

        # Save using all configured storages
        for storage in self.storages:
            try:
                file_path = storage.save(data)
                self.logger.info(f"Data saved to: {file_path}")
            except Exception as e:
                self.logger.error(f"Failed to save with {storage.__class__.__name__}: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get crawl statistics.

        Returns:
            Dictionary with statistics
        """
        return {
            **self.stats,
            'total_collected': len(self.collected_notices.notices),
            'checkpoint_info': self.checkpoint_manager.get_resume_info(),
            'dedup_info': self.dedup_manager.get_stats()
        }
