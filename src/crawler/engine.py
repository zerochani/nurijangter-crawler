"""
Main crawler engine for NuriJangter.

This module orchestrates the crawling process, including page navigation,
data extraction, storage, checkpointing, and error handling.
"""

import time
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import logging

from .browser import BrowserManager
from .interface import BaseCrawler
from ..parser import ListPageParser, DetailPageParser
from ..storage import JSONStorage, CSVStorage
from ..models import BidNotice, BidNoticeList
from ..checkpoint import CheckpointManager, CrawlState
from ..utils import CrawlerLogger, with_retry, DeduplicationManager

logger = logging.getLogger(__name__)


class CrawlerEngine(BaseCrawler):
    """
    Main crawler engine that orchestrates the entire crawling process.

    Handles:
    - Browser management
    - Page navigation and data extraction
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

        # Initialize components
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
        self.wait_config = crawler_config.get('wait', {})
        self.retry_config = crawler_config.get('retry', {})
        self.pagination_config = crawler_config.get('pagination', {})
        self.rate_limit_config = crawler_config.get('rate_limit', {})

        # Collected data
        self.collected_notices = BidNoticeList()

        # Statistics
        self.stats = {
            'pages_crawled': 0,
            'items_extracted': 0,
            'items_skipped': 0,
            'errors': 0
        }

        # Early exit state
        self.consecutive_duplicates = 0
        self.early_exit_threshold = crawler_config.get('early_exit_threshold', 30)

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

                self._navigate_to_page(page, list_url)

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
        Uses search functionality to find items by ID.
        """
        self.logger.info("Starting retry of failed items...")
        
        # Load checkpoint to get failed items
        if not self.checkpoint_manager.load_checkpoint():
            self.logger.warning("No checkpoint found. Cannot retry.")
            return

        failed_items = self.checkpoint_manager.get_failed_items()
        if not failed_items:
            self.logger.info("No failed items to retry.")
            return

        self.logger.info(f"Found {len(failed_items)} failed items to retry.")

        # Start browser
        with self.browser_manager as browser:
            page = browser.get_page()
            
            # Navigate to list page
            list_url = self.config.get('website', {}).get('list_page_url', '')
            self._navigate_to_page(page, list_url)

            # Process each failed item
            retry_count = 0
            success_count = 0
            
            for item in failed_items:
                bid_no = item.get('item_id')
                if not bid_no:
                    continue
                
                retry_count += 1
                self.logger.info(f"Retrying item {retry_count}/{len(failed_items)}: {bid_no}")
                
                try:
                    # Search and process
                    if self._search_and_process_item(page, bid_no):
                        success_count += 1
                        # Remove from failed items in checkpoint
                        self.checkpoint_manager.remove_failed_item(bid_no)
                        self.logger.info(f"Successfully retried and removed from failed list: {bid_no}")
                        
                        # Save deduplication state immediately to keep in sync
                        self.dedup_manager.save()
                    else:
                        self.logger.warning(f"Failed to retry {bid_no}")
                        
                except Exception as e:
                    self.logger.error(f"Error retrying {bid_no}: {e}")

            self.logger.info(f"Retry completed. Success: {success_count}/{len(failed_items)}")
            
            # Save data
            self._save_data()

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
                self._wait_for_page_load(page)

                # Extract notices from list page
                notices_data = self.list_parser.parse_page(page)

                self.logger.log_data_extracted("bid notice", len(notices_data))
                self.stats['pages_crawled'] += 1

                # Process each notice
                for idx, notice_data in enumerate(notices_data):
                    self._process_notice(page, notice_data, current_page_num)
                    
                    # Check for early exit
                    if self.consecutive_duplicates >= self.early_exit_threshold:
                        self.logger.info(f"Early exit triggered: {self.consecutive_duplicates} consecutive duplicates found.")
                        return  # Exit function completely

                # Check for next page
                if self.list_parser.has_next_page(page):
                    # Navigate to next page
                    if self.list_parser.go_to_next_page(page):
                        current_page_num += 1
                        self.checkpoint_manager.current_page = current_page_num
                        self.checkpoint_manager.advance_page()

                        # Rate limiting
                        self._rate_limit()
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

    def _process_notice(self, page, notice_data: Dict[str, Any], current_page_num: int = 1) -> None:
        """
        Process a single bid notice (fetch detail page if needed).

        Args:
            page: Playwright page object
            notice_data: Data extracted from list page
            current_page_num: Context for restoration
        """
        try:
            bid_notice_number = notice_data.get('bid_notice_number', '')

            # Check if already processed
            if self.checkpoint_manager.is_item_processed(bid_notice_number):
                self.logger.log_skip("Already processed", bid_notice_number)
                self.stats['items_skipped'] += 1
                return

            # Check for duplicates
            if self.dedup_manager.is_duplicate(notice_data):
                self.logger.log_skip("Duplicate", bid_notice_number)
                self.stats['items_skipped'] += 1
                self.checkpoint_manager.mark_item_processed(bid_notice_number)
                
                # Increment consecutive duplicates counter
                self.consecutive_duplicates += 1
                return
            
            # Reset counter since we found a new item
            self.consecutive_duplicates = 0

            # Fetch detail page if link available or has_detail flag set (SPA)
            detail_link = notice_data.get('detail_link')
            has_detail = notice_data.get('has_detail', False)
            
            if detail_link or has_detail:
                full_data = self._fetch_detail_page(page, detail_link or "", notice_data, current_page_num)
            else:
                full_data = notice_data

            # Create BidNotice object
            try:
                bid_notice = BidNotice(**full_data)
            except Exception as e:
                self.logger.warning(f"Failed to create BidNotice object: {e}")
                # Store as-is in additional_info
                full_data['additional_info'] = {
                    'raw_data': full_data.copy(),
                    'parse_error': str(e)
                }
                bid_notice = BidNotice(
                    bid_notice_number=bid_notice_number,
                    bid_notice_name=notice_data.get('bid_notice_name', 'Unknown'),
                    announcement_agency=notice_data.get('announcement_agency', 'Unknown'),
                    additional_info=full_data.get('additional_info', {})
                )

            # Add to collection
            self.collected_notices.add_notice(bid_notice)
            self.stats['items_extracted'] += 1

            # Mark as seen
            self.dedup_manager.mark_as_seen(notice_data)

            # Mark as processed
            self.checkpoint_manager.mark_item_processed(bid_notice_number)

            self.logger.debug(f"Processed: {bid_notice_number}")

        except Exception as e:
            self.logger.error(f"Failed to process notice: {e}")
            self.checkpoint_manager.mark_item_failed(
                notice_data.get('bid_notice_number', 'unknown'),
                str(e)
            )
            self.stats['errors'] += 1

    def _search_and_process_item(self, page, bid_no: str) -> bool:
        """
        Search for a specific bid notice by number and process it.
        
        Args:
            page: Playwright page object
            bid_no: Bid notice number to search
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # 1. Reset/Clear Search
            # Click '입찰공고목록' to reset state
            self.logger.debug("Resetting to list view...")
            self._handle_nurijangter_spa(page) # Re-run nav sequence to ensure clean state
            
            # 2. Enter Bid Number in Search Box
            # Inputs: input using ids or labels
            # '공고번호' input box
            # ID found from inspection: mf_wfm_container_sub_search_bidPbancNum or similar
            # Robust strategy: Label '입찰공고번호' -> following input
            
            self.logger.info(f"Searching for {bid_no}...")
            
            # Find input box
            input_box = None
            
            # Try specific ID first (most reliable if known)
            search_ids = [
                '#mf_wfm_container_tbxBidPbancNo', # Correct ID found via debug
                '#mf_wfm_container_txtBidPbancNum',
                'input[id*="BidPbancNo"]',
                'input[id*="bidPbancNo"]'
            ]
            
            for selector in search_ids:
                if page.locator(selector).count() > 0:
                    input_box = page.locator(selector).first
                    break
            
            # Fallback: Label strategy
            if not input_box:
                self.logger.warning("Specific ID not found, trying label strategy...")
                # Try to find input near "입찰공고번호" label
                try:
                    # WebSquare often pairs label and input in a table structure
                    # Strategy: Find th with label -> get parent tr -> find input in td
                    label = page.locator('th:has-text("입찰공고번호"), label:has-text("입찰공고번호")').first
                    if label.is_visible():
                        row_elem = label.locator('xpath=./parent::tr')
                        if row_elem.count() > 0:
                            input_box = row_elem.locator('input').first
                except: pass
                
            if input_box:
                input_box.fill(bid_no)
            else:
                self.logger.error("Could not find search input box for Bid Number")
                return False
                
            # 3. Click Search
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            search_btn.click()
            
            # Wait for grid to reload
            time.sleep(2)
            page.wait_for_selector('#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table', timeout=10000)
            
            # 4. Parse Result
            # Should have 1 row
            notices_data = self.list_parser.parse_page(page)
            
            if not notices_data:
                self.logger.warning(f"No results found for {bid_no}")
                return False
                
            # Find the matching item (search might return partial matches?)
            target_notice = None
            for notice in notices_data:
                # Remove version suffix if present for loose matching?
                # Usually exact match is best
                if bid_no in notice.get('bid_notice_number', ''):
                    target_notice = notice
                    break
            
            if not target_notice:
                self.logger.warning(f"Search results did not contain {bid_no}")
                return False
                
            # 5. Process
            self.logger.info(f"Found {bid_no}, processing detail...")
            self._process_notice(page, target_notice, 1) # page 1 context
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during search and process for {bid_no}: {e}")
            return False

    def _navigate_to_page(self, page, url: str) -> None:
        """
        Navigate to a URL with proper waiting.
        For NuriJangter SPA, this handles initial setup (popups, menu).
        """
        try:
            timeout = self.wait_config.get('navigation_timeout', 30000)
            page.goto(url, timeout=timeout, wait_until='domcontentloaded')
            self.logger.log_page_visit(url)

            # NuriJangter specific handling
            if "nuri.g2b.go.kr" in url:
                self._handle_nurijangter_spa(page)
        
        except Exception as e:
            self.logger.error(f"Failed to navigate to {url}: {e}")
            raise

    def _handle_nurijangter_spa(self, page) -> None:
        """Handle NuriJangter SPA initial navigation sequence."""
        self.logger.info("Initializing NuriJangter SPA state...")
        
        # 1. Close Popups
        try:
            # Wait a moment for popups to render
            time.sleep(2)
            page.evaluate("document.querySelectorAll('.w2window_close, .btn_cm.close').forEach(btn => btn.click());")
        except Exception:
            pass

        # 2. Navigate to Bid List (Click Menu)
        try:
            # Click '입찰공고'
            # XPath: //a[contains(@id, 'btn_menuLvl1') and .//span[text()='입찰공고']]
            menu_selector = "//a[contains(@id, 'btn_menuLvl1') and .//span[text()='입찰공고']]"
            
            # Wait for menu to be attached
            menu_link = page.locator(menu_selector).first
            menu_link.wait_for(state="attached", timeout=10000)
            
            # WebSquare sometimes requires force click if overlaid
            try:
                menu_link.click(timeout=5000)
            except Exception:
                self.logger.warning("Standard click failed, trying force click")
                menu_link.click(force=True)
            
            time.sleep(1)
            
            # Click '입찰공고목록'
            # XPath: //a[contains(@id, 'btn_menuLvl3') and contains(., '입찰공고목록')]
            submenu_selector = "//a[contains(@id, 'btn_menuLvl3') and contains(., '입찰공고목록')]"
            submenu_link = page.locator(submenu_selector).first
            submenu_link.wait_for(state="attached", timeout=10000)
            
            try:
                submenu_link.click(timeout=5000)
            except Exception:
                submenu_link.click(force=True)

            time.sleep(1)

            # 3. Search to populate list
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            search_btn.wait_for(state="visible", timeout=10000)
            search_btn.click()
            
            # Wait for grid to load
            page.wait_for_selector('#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table', timeout=10000)
            time.sleep(2) # Extra buffer for grid render
            
        except Exception as e:
            self.logger.warning(f"SPA navigation warning: {e}")

    @with_retry(max_attempts=3, initial_delay=1.0, backoff_factor=2.0)
    def _fetch_detail_page(
        self,
        page,
        detail_url: str,
        base_data: Dict[str, Any],
        current_page_num: int = 1
    ) -> Dict[str, Any]:
        """
        Fetch and parse detail page using Click-NewTab strategy.
        """
        try:
            # Find the row corresponding to this notice
            bid_no = base_data.get('bid_notice_number')
            if not bid_no:
                return base_data

            self.logger.info(f"Attempting to fetch detail for {bid_no}")

            # Selector to find the specific row keys
            # Robust strategy: Find tr containing the bid number, then find the specific name column
            # Use XPath to find row that has the bid number in one of its cells
            
            # Ensure no modals are blocking
            self._close_modals(page)

            # Get the correct frame for the list
            list_frame = self._get_list_frame(page)

            # Search in list frame and all frames
            row_selector = f"//tr[contains(@class, 'grid_body_row')][.//td[@col_id='bidPbancNum'][contains(., '{bid_no}')]]"
            row = None
            
            # 1. List Frame search
            if list_frame.locator(row_selector).count() > 0:
                row = list_frame.locator(row_selector).first
                if not row.is_visible():
                    row = None
            
            # 2. Frame search if not found or hidden
            if not row:
                for frame in page.frames:
                    try:
                        if frame.locator(row_selector).count() > 0:
                            possible_row = frame.locator(row_selector).first
                            if possible_row.is_visible():
                                row = possible_row
                                self.logger.debug(f"Found row for {bid_no} in frame: {frame.name or frame.url}")
                                break
                    except: continue

            # 3. IF ROW NOT FOUND: Force Reset Logic
            # If we can't find the row, the view might be stuck in a sub-frame or tab.
            # We force a refresh of the list view.
            if not row or not row.is_visible():
                self.logger.warning(f"Row not found for {bid_no}, attempting SOFT RESET...")
                self._soft_reset_list_view(page)
                
                # RESTORE PAGINATION if needed
                if current_page_num > 1:
                    self._restore_pagination(page, current_page_num)

                # Re-try finding row after soft reset
                list_frame = self._get_list_frame(page)
                if list_frame.locator(row_selector).count() > 0:
                    row = list_frame.locator(row_selector).first
            
            # If STILL not found, try HARD RESET
            if not row or not row.is_visible():
                self.logger.warning(f"Row still not found for {bid_no}, attempting HARD RESET...")
                self._hard_reset_via_menu(page)
                
                # RESTORE PAGINATION if needed (Critical step)
                if current_page_num > 1:
                    self._restore_pagination(page, current_page_num)

                # Re-try finding row after hard reset
                list_frame = self._get_list_frame(page)
                if list_frame.locator(row_selector).count() > 0:
                    row = list_frame.locator(row_selector).first

            if not row or not row.is_visible():
                self.logger.warning(f"Could not find row for {bid_no} even after HEAD RESET and RESTORE")
                
                # Debugging: Log available IDs to see what IS there
                try:
                    # Generic selector to find all bid number cells in the visible grid
                    id_cells = page.locator('td[col_id="bidPbancNum"]').all()
                    visible_ids = []
                    for cell in id_cells:
                        if cell.is_visible():
                            visible_ids.append(cell.inner_text().strip())
                    self.logger.warning(f"Visible IDs in grid: {visible_ids}")
                except Exception as e_debug:
                    self.logger.warning(f"Failed to log visible IDs: {e_debug}")
                # Debugging: Capture state when row is missing
                try:
                    timestamp = int(time.time())
                    page.screenshot(path=f"data/debug_missing_row_{bid_no}_{timestamp}.png")
                    with open(f"data/debug_missing_row_{bid_no}_{timestamp}.html", "w") as f:
                        f.write(page.content())
                    self.logger.info(f"Saved debug screenshot/html to data/debug_missing_row_{bid_no}_{timestamp}")
                except: pass
                
                return base_data

            # Ensure no modals are blocking
            self._close_modals(page)

            # Scroll row into view to ensure visibility
            try:
                row.scroll_into_view_if_needed()
            except: pass

            # Inside the row, find the name column (clickable)
            # WebSquare often puts the click event on a div/nobr inside the TD
            name_cell = row.locator("td[col_id='bidPbancNm']").first
            
            # Prioritize 'a' tag as it is most likely to be the actionable element
            # Browser Subagent verification confirmed 'a' tag is the correct clickable element
            link = name_cell.locator("a").first
            
            if not link.count() or not link.is_visible():
                self.logger.debug("Specific 'a' tag not found, trying generic children...")
                link = name_cell.locator("span, div, nobr").first
            
            if not link.count() or not link.is_visible():
                # Fallback to cell itself if no inner container found
                self.logger.debug("No inner link/container found, falling back to cell click")
                link = name_cell
            
            if not link.is_visible():
                self.logger.warning(f"Could not find name cell/link for {bid_no}")
                return base_data

            self.logger.debug(f"Found link element for {bid_no}, clicking...")


            # Click and wait for new page or modal
            new_page = None
            detail_opened = False

            # Method 1: Try new tab (shortest timeout since it usually works immediately)
            try:
                with page.context.expect_page(timeout=3000) as new_page_info:
                    # Use JS click as native click might be swallowed by event handlers
                    link.evaluate("el => el.click()")
                new_page = new_page_info.value
                new_page.wait_for_load_state()
                self.logger.info(f"Detail page opened (new tab) for {bid_no}")
                detail_opened = True

                # Parse detail page from new tab
                full_data = self.detail_parser.parse_page(new_page, base_data)

                # Close the detail tab
                new_page.close()

                # CRITICAL: Explicitly return to list page tab
                page.bring_to_front()
                time.sleep(1)

                # Verify we're back on the list page
                self._ensure_on_list_page(page)

                # VALIDATION: Ensure critical data exists
                if not full_data.get('opening_date'):
                    raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                self.logger.debug(f"Returned to list page after closing detail tab for {bid_no}")
                return full_data

            except Exception:
                self.logger.debug(f"Not opened in new tab for {bid_no}, trying other methods...")

            # Method 2: Check if it opened a modal
            if not detail_opened:
                try:
                    self.logger.debug(f"Checking for modal for {bid_no}...")
                    time.sleep(2)

                    modal_selector = '.w2window_active, .w2window_content_body, div[id^="w2window"]'
                    modal = page.locator(modal_selector).last

                    if modal.count() > 0 and modal.is_visible():
                        self.logger.info(f"Detail page opened (modal) for {bid_no}")
                        detail_opened = True

                        full_data = self.detail_parser.parse_page(page, base_data)

                        # Close modal with improved method
                        self._close_detail_modal(page)

                        # Verify we're back on the list page
                        self._ensure_on_list_page(page)

                        # VALIDATION: Ensure critical data exists
                        if not full_data.get('opening_date'):
                            raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                        self.logger.debug(f"Returned to list page after closing modal for {bid_no}")
                        return full_data

                except Exception as e_modal:
                    self.logger.debug(f"Modal check failed for {bid_no}: {e_modal}")

            # Method 3: In-page content load (SPA style)
            # NuriJangter may load detail content directly on the same page
            if not detail_opened:
                try:
                    self.logger.debug(f"Checking for in-page detail content for {bid_no}...")
                    
                    # Ensure we clicked it. The first click might have been consumed by the expectation failure.
                    # Or if the first click didn't trigger anything.
                    try:
                        self.logger.debug("Re-clicking link (JS) to ensure In-Page navigation triggers...")
                        link.evaluate("el => el.click()")
                    except: pass

                    # Wait for detail-specific content to appear
                    # Look for indicators that detail page loaded
                    detail_selectors = [
                        # Verified unique Title ID with text check
                        '#mf_wfm_title_textbox:has-text("상세")', 
                        '#mf_wfm_title_textbox:has-text("Detail")',
                        '.w2textbox:has-text("입찰공고진행상세")',
                        # Tab indicators
                        'a[title="입찰공고일반"]',
                        '.w2tabcontrol_contents_wrapper_selected'
                    ]
                    
                    content_found = False
                    try:
                        # Wait for ANY of the unique detailed page indicators
                        selector = ", ".join(detail_selectors)
                        page.wait_for_selector(selector, state='visible', timeout=15000)
                        content_found = True
                        self.logger.debug("Found unique detail page indicator")
                    except Exception as e:
                        self.logger.debug(f"Detail page detection failed: {e}")
                        content_found = False

                    if not content_found:
                        # FAILSAFE: Capture state if detection fails
                        timestamp = int(time.time())
                        screenshot_path = f"data/debug_failed_open_{bid_no}_{timestamp}.png"
                        html_path = f"data/debug_failed_open_{bid_no}_{timestamp}.html"
                        try:
                            page.screenshot(path=screenshot_path)
                            with open(html_path, "w") as f:
                                f.write(page.content())
                            self.logger.warning(f"Saved debug screenshot/html to {screenshot_path}")
                        except Exception as e_debug:
                            self.logger.warning(f"Failed to save debug info: {e_debug}")

                    if content_found:
                        self.logger.info(f"Detail page opened (in-page) for {bid_no}")
                        detail_opened = True

                        # Give time for all content to load
                        time.sleep(2)

                        # Parse detail page from same page (Step 1: Main View)
                        full_data = self.detail_parser.parse_page(page, base_data)
                        self.logger.info(f"Parsed main detail view for {bid_no}")

                        # Step 2: "Announcement Detail" (공고상세) Modal
                        try:
                            # Try to find the "공고상세" button
                            # Selector based on browser investigation: #mf_wfm_container_btnBidPbancP
                            detail_btn_selector = '#mf_wfm_container_btnBidPbancP'
                            
                            # Ensure no overlays (loading bars, etc.) block the button
                            self._close_modals(page) 
                            
                            detail_btn = page.locator(detail_btn_selector)
                            if detail_btn.is_visible():
                                self.logger.info("Found 'Announcement Detail' button, clicking...")
                                detail_btn.click()
                                
                                # Wait for modal to appear
                                # Browser subagent found: .w2window_content
                                page.wait_for_selector('.w2window_content', state='visible', timeout=5000)
                                self.logger.info("Announcement Detail modal opened")
                                
                                # Step 3: "Manager Contact" (담당자) Popup
                                try:
                                    # Find "View Detail" button next to Valid Manager
                                    # Selector: [id*='btnUsrDtail']
                                    manager_btn = page.locator("[id*='btnUsrDtail']").first
                                    
                                    if manager_btn.is_visible():
                                        self.logger.info("Found 'Manager View Detail' button, clicking...")
                                        manager_btn.scroll_into_view_if_needed()
                                        # Use JS click to bypass overlays/interceptors
                                        manager_btn.evaluate("el => el.click()")
                                        
                                        # Wait for popup
                                        # Increase wait time to ensure content loads
                                        time.sleep(5) 
                                        
                                        # Extract data from popup
                                        contact_data = self.detail_parser.extract_contact_popup(page)
                                        if contact_data:
                                            self.logger.info(f"Extracted contact info: {contact_data}")
                                            # Map to schema fields
                                            if 'manager_phone' in contact_data:
                                                full_data['phone_number'] = contact_data['manager_phone']
                                            if 'manager_email' in contact_data:
                                                full_data['email'] = contact_data['manager_email']
                                            
                                            # Also keep original keys just in case
                                            full_data.update(contact_data)
                                        
                                        # Close Manager Popup
                                        # Look for a close button in the top-most modal
                                        self._close_modals(page, level=2) 
                                        
                                    else:
                                        self.logger.warning("Manager 'View Detail' button not found")
                                        
                                except Exception as e_manager:
                                    self.logger.warning(f"Failed to process Manager Contact popup: {e_manager}")

                                # Close Announcement Modal
                                self._close_detail_modal(page)
                            else:
                                self.logger.warning("'Announcement Detail' button not found")

                        except Exception as e_step2:
                            self.logger.warning(f"Step 2 (Announcement Detail) failed: {e_step2}") 



                        # Step 3: "Base Price" (기준금액) Tab
                        try:
                            # Try to find "기준금액" tab
                            # Selector based on browser investigation: a[role="tab"] has-text
                            tab_selectors = [
                                'a[role="tab"]:has-text("기준금액")',
                                'li.w2tabControl_tab_li:has-text("기준금액")',
                                'a:has-text("기준금액")'
                            ]
                            
                            tab_btn = None
                            for selector in tab_selectors:
                                if page.locator(selector).count() > 0 and page.locator(selector).first.is_visible():
                                    tab_btn = page.locator(selector).first
                                    break
                                    
                            if tab_btn:
                                self.logger.info("Found 'Base Price' tab, clicking...")
                                # Ensure no modals are blocking before clicking tab
                                self._close_modals(page) 
                                
                                try:
                                    tab_btn.click(timeout=5000)
                                except Exception as e_click:
                                    self.logger.warning(f"Standard tab click failed: {e_click}. Trying JS click...")
                                    # Fallback to JS click if blocked
                                    page.evaluate("arguments[0].click();", tab_btn.element_handle())
                                
                                time.sleep(1) # Short sleep
                                
                                # Wait for specific element in the new tab to ensure it loaded
                                try:
                                    # '배정예산' matches budget_amount label
                                    page.wait_for_selector('th:has-text("배정예산"), label:has-text("배정예산"), th:has-text("기초금액")', timeout=3000)
                                    self.logger.debug("Base Price tab content loaded")
                                except:
                                    self.logger.debug("Base Price tab content indicator not found, proceeding anyway")
                                    time.sleep(1) # Extra wait if indicator missing
                                
                                # Parse tab content (page context is now updated)
                                self.logger.info("Parsing 'Base Price' tab...")
                                tab_data = self.detail_parser.parse_page(page, base_data)
                                full_data.update(tab_data)
                                self.logger.debug(f"Merged {len(tab_data)} fields from Base Price tab")
                            else:
                                self.logger.debug("'Base Price' tab not found")

                        except Exception as e_tab_step:
                            self.logger.warning(f"Failed to process 'Base Price' tab: {e_tab_step}")
                            # SAFETY: If tab click failed (e.g. intercepted), ensure we clean up blocking modals
                            self._close_modals(page)

                        # Return to list view
                        # This is tricky - we need to close the detail view
                        # Try clicking back button or list menu
                        try:
                            # Try to find and click back/close button
                            back_selectors = [
                                'button:has-text("목록")',
                                'button:has-text("닫기")',
                                'a:has-text("목록")',
                                '.btn_back',
                                '.btn_list',
                            ]

                            clicked_back = False
                            for selector in back_selectors:
                                try:
                                    back_btn = page.locator(selector).first
                                    if back_btn.is_visible():
                                        self.logger.debug(f"Clicking back button: {selector}")
                                        back_btn.click()
                                        time.sleep(2)
                                        clicked_back = True
                                        break
                                except:
                                    continue

                            if not clicked_back:
                                # Fallback: re-click list menu
                                self.logger.debug("No back button found, reloading list...")
                                self._ensure_on_list_page(page)

                        except Exception as e:
                            self.logger.warning(f"Failed to return to list view: {e}")
                            # Try to ensure we're on list page anyway
                            self._ensure_on_list_page(page)

                        # VALIDATION: Ensure critical data exists
                        if not full_data.get('opening_date'):
                            raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                        self.logger.debug(f"Returned to list page after in-page detail for {bid_no}")
                        return full_data

                except Exception as e_spa:
                    self.logger.debug(f"In-page check failed for {bid_no}: {e_spa}")
                    if "Validation Failed" in str(e_spa):
                        raise e_spa

            # All methods failed
            self.logger.warning(f"Failed to open detail page for {bid_no} (tried all methods)")
            raise Exception(f"Failed to open detail page for {bid_no} (tried all methods)")

        except Exception as e:
            self.logger.error(f"Failed to fetch detail page: {e}")
            self._close_modals(page) # Safety cleanup
            raise e

    def _reload_list_page(self, page):
        """
        Reload the list page completely to ensure clean state.
        This is more reliable than trying to reset the view.
        """
        try:
            self.logger.debug("Reloading list page for clean state...")

            # 1. Close any modals
            self._close_modals(page)
            time.sleep(1)

            # 2. Click on the list menu again to reload
            # This ensures we're back to the list view
            menu_selector = "//a[contains(@id, 'btn_menuLvl3') and contains(., '입찰공고목록')]"

            # Try to find in all frames
            menu_found = False
            if page.locator(menu_selector).count() > 0:
                page.locator(menu_selector).first.click(force=True)
                menu_found = True
            else:
                for frame in page.frames:
                    if frame.locator(menu_selector).count() > 0:
                        frame.locator(menu_selector).first.click(force=True)
                        menu_found = True
                        break

            if menu_found:
                time.sleep(2)  # Wait for page transition

            # 3. Click search button to reload list
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            if search_btn.count() > 0 and search_btn.is_visible():
                search_btn.click(force=True)
                time.sleep(3)  # Wait for search results

                # Wait for grid
                try:
                    page.wait_for_selector("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table", timeout=5000)
                    time.sleep(1)
                    self.logger.debug("List page reloaded successfully")
                except:
                    self.logger.warning("Grid not found after reload")
            else:
                self.logger.debug("Could not find search button for reload")

        except Exception as e:
            self.logger.debug(f"List page reload failed: {e}")

    def _soft_reset_list_view(self, page):
        """Force reset the list view by clicking Search or navigating."""
        try:
            # 1. Try closing modals first
            self._close_modals(page)

            # 2. Find Search button and click to refresh list
            # Common search button IDs in NuriJangter
            search_btn_selectors = [
                '#mf_wfm_container_btnS0001',
                '.btn_cm.search',
                'button[class*="search"]',
                '#mf_wfm_container_scBtn'
            ]

            # Try to find button in all frames
            btn = None
            for selector in search_btn_selectors:
                if page.locator(selector).count() > 0:
                    btn = page.locator(selector).first
                    break
                for frame in page.frames:
                    if frame.locator(selector).count() > 0:
                        btn = frame.locator(selector).first
                        break
                if btn: break

            if btn and btn.is_visible():
                self.logger.debug("Resetting list view via Search button (Soft Reset)...")
                btn.click(force=True)
                time.sleep(3) # Wait for grid refresh (increased from 2s to 3s)

                # Wait for grid to actually appear AND contain rows
                try:
                    # Wait for the table body to be visible
                    page.wait_for_selector("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table", timeout=5000)
                    # CRITICAL: Wait for actual rows to be present
                    page.wait_for_selector("tr.grid_body_row", timeout=5000)
                    time.sleep(1) # Extra buffer
                except:
                    pass
            else:
                self.logger.debug("Could not find Search button for soft reset")

        except Exception as e:
            self.logger.debug(f"List view soft reset failed: {e}")

    def _hard_reset_via_menu(self, page):
        """
        Hard reset: Click the sidebar menu '입찰공고목록' to fully reload the list context.
        This is more reliable than just clicking search if the page state is corrupted by modals.
        """
        self.logger.info("Triggering HARD RESET via Sidebar Menu...")
        try:
            # 1. Close any blocking modals first
            self._close_modals(page)
            
            # 2. Click '입찰공고' (Level 1 Menu) just in case
            try:
                menu_l1 = page.locator("//a[contains(@id, 'btn_menuLvl1') and .//span[text()='입찰공고']]").first
                if menu_l1.is_visible():
                    menu_l1.click(force=True)
                    time.sleep(0.5)
            except: pass

            # 3. Click '입찰공고목록' (Level 3 Menu - The actual list link)
            # Use specific ID pattern or text content
            menu_l3 = page.locator("//a[contains(@id, 'btn_menuLvl3') and contains(., '입찰공고목록')]").first
            menu_l3.click(force=True)
            self.logger.info("Clicked '입찰공고목록' menu")
            
            time.sleep(2)
            
            # 4. Click Search to populate
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            if search_btn.is_visible():
                search_btn.click(force=True)
                self.logger.info("Clicked Search button after menu reset")
                
                # Wait for grid to actually populate - CRITICAL
                try:
                    # Wait for overlay to disappear
                    page.wait_for_selector('div[id*="processbar"]', state='hidden', timeout=5000)
                    
                    # Wait for at least one row
                    page.wait_for_selector("tr.grid_body_row", state='visible', timeout=15000)
                    
                    # Double check row count
                    row_count = page.locator("tr.grid_body_row").count()
                    self.logger.info(f"Hard reset complete. Grid populated with {row_count} rows.")
                    time.sleep(1)
                except Exception as e_wait:
                    self.logger.warning(f"Grid did not populate after hard reset: {e_wait}")
        
        except Exception as e:
            self.logger.warning(f"Hard reset failed: {e}")

    def _restore_pagination(self, page, target_page_num):
        """
        Restore the list to the specific page number after a reset.
        Assumes we are currently on page 1 (after reset).
        """
        if target_page_num <= 1:
            return

        self.logger.info(f"Restoring pagination to page {target_page_num}...")
        
        try:
            # We need to jump from Page 1 to Target Page
            # The logic depends on how far we need to go.
            # Page groups are usually 10 pages (1-10, 11-20, etc.)
            
            # Current known state: Page 1
            current_group_start = 1
            target_group_start = ((target_page_num - 1) // 10) * 10 + 1
            
            # Navigate groups if needed
            while current_group_start < target_group_start:
                self.logger.debug(f"Jumping to next group from {current_group_start}...")
                
                # Click next group button (>)
                # Use list_parser logic for finding next group
                js_next_group = """
                    (function() {
                        var btn = document.querySelector('#mf_wfm_container_pagelist_next_btn');
                        if (btn) { btn.click(); return true; }
                        var nextBtn = document.querySelector('.w2pageList_next_btn, .w2pageList_btn_next');
                        if (nextBtn) { nextBtn.click(); return true; }
                        return false;
                    })();
                """
                if page.evaluate(js_next_group):
                    time.sleep(2) # Wait for reload
                    current_group_start += 10
                else:
                    self.logger.error("Failed to find next group button during restoration")
                    return

            # Now we are in the correct group (or close to it)
            # Click the specific page number
            page_btn_selector = f"#mf_wfm_container_pagelist_page_{target_page_num}"
            
            # Try ID first
            if page.locator(page_btn_selector).count() > 0:
                 page.locator(page_btn_selector).first.click()
                 time.sleep(2)
                 self.logger.info(f"Restored to page {target_page_num}")
                 return

            # Fallback: Try finding by text
            # This is risky if multiple numbers exist, but usually pagination is unique in the footer
            try:
                # Find link/li inside pagelist
                page.locator(f".w2pageList_li:has-text('{target_page_num}')").first.click()
                time.sleep(2)
                self.logger.info(f"Restored to page {target_page_num} via text match")
            except:
                self.logger.error(f"Failed to find button for page {target_page_num}")

        except Exception as e:
            self.logger.error(f"Pagination restoration failed: {e}")

    def _get_list_frame(self, page):
        """Find the frame containing the bid notice list."""
        # 1. Check main page first
        if page.locator("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table").count() > 0:
            return page
            
        # 2. Check all frames
        for frame in page.frames:
            try:
                if frame.locator("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table").count() > 0:
                    return frame
            except: continue
            
        return page # Fallback to page

    def _close_detail_modal(self, page):
        """
        Close detail page modal with improved reliability.
        This method focuses specifically on closing the detail modal, not all modals.
        """
        self.logger.debug("Closing detail modal...")

        try:
            # Strategy 1: Find and click the close button in the active modal
            # NuriJangter modals typically have a close button in the title bar
            close_selectors = [
                '.w2window_active .w2window_close',  # Active window close button
                '.w2window_content .w2window_close',  # Content window close
                'div[id^="w2window"] .w2window_close',  # Window by ID prefix
                '.btn_cm.close',  # Common close button class
                'a[title="닫기"]',  # Close link by title
                'button[title="닫기"]',  # Close button by title
            ]

            modal_closed = False
            for selector in close_selectors:
                try:
                    close_btn = page.locator(selector).last
                    if close_btn.is_visible():
                        self.logger.debug(f"Found close button with selector: {selector}")
                        close_btn.click(force=True)
                        time.sleep(1.5)
                        modal_closed = True
                        break
                except:
                    continue

            # Strategy 2: If no close button found, try ESC key
            if not modal_closed:
                self.logger.debug("No close button found, trying ESC key...")
                page.keyboard.press("Escape")
                time.sleep(1.5)

            # Strategy 3: If still visible, try JavaScript removal
            if page.locator('.w2window_active, .w2window_content_body').is_visible():
                self.logger.debug("Modal still visible, trying JavaScript removal...")
                page.evaluate("""
                    // Remove active modals
                    document.querySelectorAll('.w2window_active, .w2window, div[id^="w2window"]').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    // Remove active modals
                    document.querySelectorAll('.w2window_active, .w2window, div[id^="w2window"]').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                    // Remove modal overlays (aggressive)
                    document.querySelectorAll('.w2window_cover, .modal_overlay, #_modal, .w2modal_popup, #___processbar2, div[id*="processbar"]').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                """)
                time.sleep(1)

            self.logger.debug("Detail modal close completed")

        except Exception as e:
            self.logger.warning(f"Error closing detail modal: {e}")

    def _ensure_on_list_page(self, page):
        """
        Verify we're on the list page and list grid is visible.
        If not, try to navigate back to it.
        """
        self.logger.debug("Verifying we're on list page...")

        try:
            # Check if list grid is visible
            list_grid_selector = '#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table, tr.grid_body_row'

            # Give it a moment to appear
            try:
                # Use .first to avoid strict mode violation if multiple rows match
                page.wait_for_selector(list_grid_selector, timeout=3000, state='visible')
                self.logger.debug("✓ List grid is visible")
                return True
            except:
                self.logger.warning("List grid not visible, attempting recovery...")
                
                # FIRST ATTEMPT: Close any modals that might be blocking the view
                self._close_modals(page)
                
                # Check again
                if page.locator(list_grid_selector).first.is_visible():
                    self.logger.debug("✓ List grid became visible after closing modals")
                    return True

            # Recovery: Click on the list menu tab if it exists
            list_tab_selectors = [
                "//a[contains(., '입찰공고목록')]",
                '.w2tabcontrol_tab:first-child',
                'a[id*="tab"]:first-child',
            ]

            for selector in list_tab_selectors:
                try:
                    tab = page.locator(selector).first
                    if tab.is_visible():
                        self.logger.debug(f"Clicking list tab: {selector}")
                        tab.click(force=True)
                        time.sleep(2)

                        # Check again if grid is visible
                        if page.locator(list_grid_selector).first.is_visible():
                            self.logger.debug("✓ Successfully returned to list page")
                            return True
                except:
                    continue

            # If still not visible, try search button to refresh
            try:
                search_btn = page.locator('#mf_wfm_container_btnS0001')
                if search_btn.is_visible():
                    self.logger.debug("Clicking search button to refresh list...")
                    search_btn.click(force=True)
                    time.sleep(3)

                    if page.locator(list_grid_selector).first.is_visible():
                        self.logger.debug("✓ List refreshed successfully")
                        return True
            except:
                pass

            self.logger.warning("Could not verify list page visibility")
            return False

        except Exception as e:
            self.logger.warning(f"Error ensuring on list page: {e}")
            return False

    def _close_modals(self, page):
        """Close any open WebSquare modals and MDI tabs."""
        self.logger.debug("Closing modals and returning to list tab...")

        # Max retries to ensure we don't get stuck
        for i in range(3):
            # Check for modals OR active MDI tabs (excluding the main list tab)
            has_modal = page.locator('.w2window, .w2window_active, .w2window_content_body, iframe[src*="popup"], div[id^="w2window"]').count() > 0
            has_overlay = page.locator('#_modal, .w2modal_popup, .w2window_mask, .w2window_cover').count() > 0

            # Simple MDI close buttons matching common WebSquare tab patterns
            has_tab = page.locator('.w2tabcontrol_tab_close, .close_tab, .tab_close').count() > 1

            if not has_modal and not has_overlay and not has_tab:
                # No modals or extra tabs, we're done
                break

            try:
                # 1. Try Escape key (often closes the top-most modal)
                page.keyboard.press("Escape")
                time.sleep(0.5)

                # 2. Click standard modal close buttons (safe to click all)
                # FIX: Use double quotes for the JS string to avoid conflict with single quotes in selector
                modal_close_selector = ".w2window_close, .btn_cm.close, .w2window_close_icon, .close_button, iframe[src*='popup'] .close, div[id^='w2window'] .close"
                page.evaluate(f'document.querySelectorAll("{modal_close_selector}").forEach(btn => btn.click());')

                # 3. Force Remove Blocking Overlays (The "Hammer" approach)
                # If a modal overlay is intercepting clicks but has no close button, destroy it.
                page.evaluate("""
                    document.querySelectorAll('#_modal, .w2modal_popup, .w2window_mask, .w2window_cover').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                """)

                # 4. Handle Tab Close - CAREFULLY
                # Only click the LAST tab's close button if there are multiple tabs
                tabs = page.locator('.w2tabcontrol_tab_close, .close_tab, .tab_close')
                if tabs.count() > 1:
                     self.logger.debug(f"Closing extra tab ({tabs.count()} total tabs)...")
                     tabs.last.click(force=True)

                time.sleep(1.0)

            except Exception as e:
                self.logger.debug(f"Error in close_modals iteration {i}: {e}")
                pass

        # 5. Explicitly click on the FIRST tab to ensure we're on the list view
        try:
            # Find all tabs
            tabs = page.locator('.w2tabcontrol_tab, .tab_item, a[id*="tab"]')
            if tabs.count() > 0:
                # Click the first tab (should be the list)
                first_tab = tabs.first
                if first_tab.is_visible():
                    # Check if it looks selected already
                    class_attr = first_tab.get_attribute("class") or ""
                    if "selected" not in class_attr:
                        self.logger.debug("Switching to first tab (list view)...")
                        first_tab.click(force=True)
                        time.sleep(1)
        except Exception as e:
            self.logger.debug(f"Could not switch to first tab: {e}")

        self.logger.debug("Modal close completed")

    def _wait_for_page_load(self, page) -> None:
        """
        Wait for page to fully load.

        Args:
            page: Playwright page object
        """
        try:
            # Wait for network to be idle
            page.wait_for_load_state('networkidle', timeout=self.wait_config.get('navigation_timeout', 30000))

            # Additional wait after load
            after_load_wait = self.wait_config.get('after_load', 2000)
            if after_load_wait > 0:
                time.sleep(after_load_wait / 1000)

        except Exception as e:
            self.logger.debug(f"Wait for page load timeout: {e}")

    def _rate_limit(self) -> None:
        """Apply rate limiting delay."""
        delay = self.wait_config.get('between_pages', 1000)
        if delay > 0:
            time.sleep(delay / 1000)

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
