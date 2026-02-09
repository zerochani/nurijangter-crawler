
import logging
import time
from typing import Dict, Any, List

from ..checkpoint import CheckpointManager
from ..utils import DeduplicationManager

logger = logging.getLogger(__name__)

class RetryManager:
    """
    Handles retrying of failed items and searching for specific bid notices.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        navigator: Any,
        processor: Any,
        checkpoint_manager: CheckpointManager,
        dedup_manager: DeduplicationManager,
        save_callback: Any # Callback to trigger data save in engine
    ):
        self.config = config
        self.navigator = navigator
        self.processor = processor
        self.checkpoint_manager = checkpoint_manager
        self.dedup_manager = dedup_manager
        self.save_callback = save_callback
        self.logger = logger
        self.list_parser = processor.list_parser # Access parser via processor or pass directly

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

        # Start browser (Engine handles the browser context, here we assume it's running via callback or passed page)
        # However, retry usually runs in its own scope. 
        # Ideally, this method should receive the PAGE object if the engine is already running 
        # OR it manages its own browser if called standalone.
        # Given the refactoring, let's assume the Engine calls this with an active page object 
        # OR this method is called within a browser context.
        # But wait, original code managed browser context. 
        # Let's keep the logic abstract and assume we are passed a page or we use the browser manager.
        # Refactoring decision: Engine passes the page to methods that need it.
        # But `retry_failed_items` in Engine was a top-level method that started the browser.
        # So we should probably modify `run` to handle browser and pass page here?
        # OR Engine keeps browser management.
        
        # Let's assume Engine calls this method, and Engine manages the browser lifecycle.
        # So this method should probably take `page` as an argument if it's just a worker.
        # But `retry_failed_items` is a high-level orchestration method.
        # Let's change the signature to accept `page`.
        pass 

    def process_retries(self, page) -> None:
        """
        Execute the retry loop using the provided page.
        This replaces the browser management part of the original retry_failed_items.
        """
        # Load checkpoint to get failed items
        if not self.checkpoint_manager.load_checkpoint():
            self.logger.warning("No checkpoint found. Cannot retry.")
            return

        failed_items = self.checkpoint_manager.get_failed_items()
        if not failed_items:
            self.logger.info("No failed items to retry.")
            return
            
        # Navigate to list page
        list_url = self.config.get('website', {}).get('list_page_url', '')
        self.navigator.navigate_to_page(page, list_url)

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
                if self.search_and_process_item(page, bid_no):
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
        if self.save_callback:
            self.save_callback()

    def search_and_process_item(self, page, bid_no: str) -> bool:
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
            self.navigator.handle_nurijangter_spa(page) # Re-run nav sequence to ensure clean state
            
            # 2. Enter Bid Number in Search Box
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
                try:
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
                if bid_no in notice.get('bid_notice_number', ''):
                    target_notice = notice
                    break
            
            if not target_notice:
                self.logger.warning(f"Search results did not contain {bid_no}")
                return False
                
            # 5. Process
            self.logger.info(f"Found {bid_no}, processing detail...")
            self.processor.process_notice(page, target_notice, 1) # page 1 context
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during search and process for {bid_no}: {e}")
            return False
