"""
List page parser for NuriJangter bid notices.

This module extracts bid notice information from list pages.
"""

from typing import List, Dict, Any, Optional
from playwright.sync_api import Page
import logging
import re
import time

logger = logging.getLogger(__name__)


class ListPageParser:
    """
    Parser for extracting bid notices from list pages.

    Handles pagination and extracts basic information about each bid notice.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize list page parser.

        Args:
            config: Configuration dictionary with extraction settings
        """
        self.config = config
        self.list_fields = config.get('extraction', {}).get('list_fields', [])

    def parse_page(self, page: Page) -> List[Dict[str, Any]]:
        """
        Parse a single list page and extract bid notices.

        Args:
            page: Playwright page object

        Returns:
            List of bid notice dictionaries
        """
        notices = []

        try:
            # Wait for table to load
            # NuriJangter uses WebSquare grid
            table_selector = '#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table'
            page.wait_for_selector(table_selector, timeout=10000)

            # Extract table rows
            rows = page.locator('tr.grid_body_row').all()

            logger.info(f"Found {len(rows)} rows on page")

            for idx, row in enumerate(rows):
                try:
                    notice = self._parse_row(row, page)
                    if notice:
                        notices.append(notice)
                except Exception as e:
                    logger.warning(f"Failed to parse row {idx}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to parse list page: {e}")

        return notices

    def _parse_row(self, row, page: Page) -> Optional[Dict[str, Any]]:
        """
        Parse a single table row.
        """
        try:
            notice = {}

            # NuriJangter uses WebSquare grid with col_id attributes
            
            # Notice number (공고번호)
            number_elem = row.locator('td[col_id="bidPbancNum"]').first
            if number_elem.count() > 0:
                notice['bid_notice_number'] = self._clean_text(number_elem.inner_text())

            # Notice name/title (공고명)
            name_cell = row.locator('td[col_id="bidPbancNm"]').first
            if name_cell.count() > 0:
                notice['bid_notice_name'] = self._clean_text(name_cell.inner_text())
                # For Click-based navigation, we don't extract URL but we can mark it
                notice['has_detail'] = True
                
            # Agency (공고기관)
            agency_elem = row.locator('td[col_id="grpNm"]').first
            if agency_elem.count() > 0:
                notice['announcement_agency'] = self._clean_text(agency_elem.inner_text())

            # Bid method - Not directly visible in standard columns but might be there
            # Skipping for now or mapping if column exists

            # Announcement date (공고일시)
            date_elem = row.locator('td[col_id="pbancPstgDt"]').first
            if date_elem.count() > 0:
                notice['announcement_date'] = self._clean_text(date_elem.inner_text())

            # Deadline date (마감일시)
            deadline_elem = row.locator('td[col_id="slprRcptDdlnDt"]').first
            if deadline_elem.count() > 0:
                notice['deadline_date'] = self._clean_text(deadline_elem.inner_text())

            # Valid check
            if not notice.get('bid_notice_number'):
                return None

            return notice

        except Exception as e:
            logger.error(f"Error parsing row: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()

    def _normalize_url(self, url: str, base_url: str) -> str:
        return url  # Not used for SPA click navigation

    def has_next_page(self, page: Page) -> bool:
        """Check if there is a next page."""
        try:
            # WebSquare pagination next button
            next_button = page.locator('#mf_wfm_container_pagelist_next_btn a, .w2pageList_next_btn').first
            
            if next_button.count() > 0:
                # Check if visible and not disabled
                if next_button.is_visible():
                    return True
            return False
        except Exception as e:
            logger.debug(f"Error checking for next page: {e}")
            return False

    def go_to_next_page(self, page: Page) -> bool:
        """
        Navigate to the next page.
        Strategies:
        1. Find current active page.
        2. Look for 'current_page + 1' link.
        3. If not found, look for 'Next Group' button.
        """
        try:
            # 1. Identify current page
            # WebSquare active page usually has a class like 'w2pageList_col_selected' or similar style
            # Based on inspection, we can look for the text of the selected item or assume based on state
            # For robustness, we try to find the button connected to the "next number"
            
            # Simple heuristic: Try to find numbers 1 to 11. 
            # If 1 is selected, click 2. If 10 is selected, click next group.
            
            current_page_elem = page.locator('.w2pageList_col_selected, .w2pageList_label_selected').first
            current_page_num = 1
            if current_page_elem.count() > 0:
                try:
                    current_page_num = int(self._clean_text(current_page_elem.inner_text()))
                except ValueError:
                    pass

            next_page_num = current_page_num + 1
            logger.info(f"Attempting to navigate from page {current_page_num} to {next_page_num}")
            
            # Scroll to bottom to ensure pagination is in view/active
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            
            # 2. Try to find the next numeric page button
            # ID pattern: mf_wfm_container_pagelist_page_{number}
            next_page_id = f"mf_wfm_container_pagelist_page_{next_page_num}"
            
            # Use JS to check existence and click directly
            # This is more robust for WebSquare than Playwright's simulated click
            js_script = f"""
                (function() {{
                    var btn = document.getElementById('{next_page_id}');
                    if (btn) {{
                        btn.click();
                        return true;
                    }}
                    return false;
                }})();
            """
            
            if page.evaluate(js_script):
                logger.info(f"Clicked numeric page {next_page_num} via JS")
                page.wait_for_load_state('networkidle', timeout=10000)
                time.sleep(2)
                return True

            # 3. If next numeric button not found, try "Next Group" button (>)
            # Selector: #mf_wfm_container_pagelist_next_btn
            # Also try generic "next" class
            js_next_group = """
                (function() {
                    // Try ID first
                    var btn = document.querySelector('#mf_wfm_container_pagelist_next_btn');
                    if (btn) {
                        btn.click();
                        return true;
                    }
                    // Try by class/aria for robustness
                    var nextBtn = document.querySelector('.w2pageList_next_btn, .w2pageList_btn_next');
                    if (nextBtn) {
                        nextBtn.click();
                        return true;
                    }
                    return false;
                })();
            """
            
            if page.evaluate(js_next_group):
                logger.info("Clicked next group button via JS")
                page.wait_for_load_state('networkidle', timeout=10000)
                time.sleep(2)
                return True
            
            logger.info("No next page or group button found")
            return False

        except Exception as e:
            logger.error(f"Failed to go to next page: {e}")
            return False

    def get_total_pages(self, page: Page) -> Optional[int]:
        """
        Get total number of pages if available.

        Args:
            page: Playwright page object

        Returns:
            Total pages or None if not determinable
        """
        try:
            # Look for pagination info
            pagination = page.locator('.pagination, .paging').first

            if pagination.count() > 0:
                # Try to find the last page number
                page_links = pagination.locator('a').all()

                page_numbers = []
                for link in page_links:
                    text = link.inner_text().strip()
                    if text.isdigit():
                        page_numbers.append(int(text))

                if page_numbers:
                    return max(page_numbers)

            return None

        except Exception as e:
            logger.debug(f"Could not determine total pages: {e}")
            return None
