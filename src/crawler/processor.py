
import logging
import time
from typing import Dict, Any, Optional

from ..models import BidNotice
from ..parser import ListPageParser, DetailPageParser
from ..checkpoint import CheckpointManager
from ..utils import DeduplicationManager

logger = logging.getLogger(__name__)

class NoticeProcessor:
    """
    Handles processing of individual bid notices, including detail fetching.
    """

    def __init__(
        self,
        navigator: Any,
        list_parser: ListPageParser,
        detail_parser: DetailPageParser,
        dedup_manager: DeduplicationManager,
        checkpoint_manager: CheckpointManager,
        collected_notices: Any,
        stats: Dict[str, int]
    ):
        self.navigator = navigator
        self.list_parser = list_parser
        self.detail_parser = detail_parser
        self.dedup_manager = dedup_manager
        self.checkpoint_manager = checkpoint_manager
        self.collected_notices = collected_notices
        self.stats = stats
        self.logger = logger
        
        # Internal state
        self.consecutive_duplicates = 0
        self.early_exit_threshold = 30 # Default, should come from config

    def process_notice(self, page, notice_data: Dict[str, Any], current_page_num: int = 1) -> None:
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
                # self.logger.log_skip("Already processed", bid_notice_number) # Interface change needed
                self.logger.info(f"Skipping {bid_notice_number}: Already processed")
                self.stats['items_skipped'] += 1
                return

            # Check for duplicates
            if self.dedup_manager.is_duplicate(notice_data):
                # self.logger.log_skip("Duplicate", bid_notice_number)
                self.logger.info(f"Skipping {bid_notice_number}: Duplicate")
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
                full_data = self.fetch_detail_page(page, detail_link or "", notice_data, current_page_num)
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

            # VALIDATION: Check for data quality
            # If critical fields are missing, we might want to retry later
            # User request: "null이 많은 데이터는 실패된 항목에 추가"
            null_count = 0
            critical_fields = ['budget_amount', 'base_price', 'opening_date', 'pre_qualification', 'contract_bond']
            for field in critical_fields:
                if not getattr(bid_notice, field):
                    null_count += 1
            
            # If mostly empty (heuristic: 3 or more critical fields missing), consider it a partial failure
            # But we already added it to collection. 
            # Strategy: If it's REALLY bad, remove from collection and raise exception so it goes to failed_items
            if null_count >= 4 and not bid_notice.notes: # notes might explain why (e.g. cancelled)
                self.collected_notices.notices.pop() # Remove the last added item
                raise ValueError(f"Too many missing fields ({null_count}/{len(critical_fields)} critical fields null). Treating as failure for retry.")

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

    def fetch_detail_page(
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
            self.navigator.close_modals(page)

            # Get the correct frame for the list
            list_frame = self.navigator.get_list_frame(page)

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
                self.navigator.soft_reset_list_view(page)
                
                # RESTORE PAGINATION if needed
                if current_page_num > 1:
                    try:
                        self.navigator.restore_pagination(page, current_page_num)
                    except Exception as e:
                        self.logger.warning(f"Restore pagination failed during Soft Reset: {e}")
                        # Continue to check row, it will likely fail and trigger Hard Reset

                # Re-try finding row after soft reset
                list_frame = self.navigator.get_list_frame(page)
                if list_frame.locator(row_selector).count() > 0:
                    row = list_frame.locator(row_selector).first
            
            # If STILL not found, try HARD RESET
            if not row or not row.is_visible():
                self.logger.warning(f"Row still not found for {bid_no}, attempting HARD RESET...")
                self.navigator.hard_reset_via_menu(page)
                
                # RESTORE PAGINATION if needed (Critical step)
                if current_page_num > 1:
                    try:
                        self.navigator.restore_pagination(page, current_page_num)
                    except Exception as e:
                        self.logger.error(f"Restore pagination failed during Hard Reset: {e}")
                        # If this fails, we are truly lost for this page, but maybe next item will work

                # Re-try finding row after hard reset
                list_frame = self.navigator.get_list_frame(page)
                if list_frame.locator(row_selector).count() > 0:
                    row = list_frame.locator(row_selector).first

            if not row or not row.is_visible():
                self.logger.warning(f"Could not find row for {bid_no} even after HEAD RESET and RESTORE")
                return base_data

            # Ensure no modals are blocking
            self.navigator.close_modals(page)

            # Scroll row into view to ensure visibility
            try:
                row.scroll_into_view_if_needed()
            except: pass

            # Inside the row, find the name column (clickable)
            # WebSquare often puts the click event on a div/nobr inside the TD
            name_cell = row.locator("td[col_id='bidPbancNm']").first
            
            # Prioritize 'a' tag as it is most likely to be the actionable element
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
                self.navigator.ensure_on_list_page(page)

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
                        self.navigator.close_detail_modal(page)

                        # Verify we're back on the list page
                        self.navigator.ensure_on_list_page(page)

                        # VALIDATION: Ensure critical data exists
                        if not full_data.get('opening_date'):
                            raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                        self.logger.debug(f"Returned to list page after closing modal for {bid_no}")
                        return full_data

                except Exception as e_modal:
                    self.logger.debug(f"Modal check failed for {bid_no}: {e_modal}")

            # Method 3: In-page content load (SPA style)
            if not detail_opened:
                try:
                    self.logger.debug(f"Checking for in-page detail content for {bid_no}...")
                    
                    try:
                        self.logger.debug("Re-clicking link (JS) to ensure In-Page navigation triggers...")
                        link.evaluate("el => el.click()")
                    except: pass

                    # Wait for detail-specific content to appear
                    detail_selectors = [
                        '#mf_wfm_title_textbox:has-text("상세")', 
                        '#mf_wfm_title_textbox:has-text("Detail")',
                        '.w2textbox:has-text("입찰공고진행상세")',
                        'a[title="입찰공고일반"]',
                        '.w2tabcontrol_contents_wrapper_selected'
                    ]
                    
                    content_found = False
                    try:
                        selector = ", ".join(detail_selectors)
                        page.wait_for_selector(selector, state='visible', timeout=15000)
                        content_found = True
                        self.logger.debug("Found unique detail page indicator")
                    except Exception as e:
                        self.logger.debug(f"Detail page detection failed: {e}")
                        content_found = False

                    if content_found:
                        self.logger.info(f"Detail page opened (in-page) for {bid_no}")
                        detail_opened = True
                        time.sleep(2)

                        # Parse detail page from same page (Step 1: Main View)
                        full_data = self.detail_parser.parse_page(page, base_data)
                        self.logger.info(f"Parsed main detail view for {bid_no}")

                        # Step 2: "Announcement Detail" (공고상세) Modal
                        try:
                            detail_btn_selector = '#mf_wfm_container_btnBidPbancP'
                            self.navigator.close_modals(page) 
                            
                            detail_btn = page.locator(detail_btn_selector)
                            if detail_btn.is_visible():
                                self.logger.info("Found 'Announcement Detail' button, clicking...")
                                detail_btn.click()
                                page.wait_for_selector('.w2window_content', state='visible', timeout=5000)
                                self.logger.info("Announcement Detail modal opened")
                                
                                # Step 3: "Manager Contact" (담당자) Popup
                                try:
                                    manager_btn = page.locator("[id*='btnUsrDtail']").first
                                    if manager_btn.is_visible():
                                        self.logger.info("Found 'Manager View Detail' button, clicking...")
                                        manager_btn.scroll_into_view_if_needed()
                                        manager_btn.evaluate("el => el.click()")
                                        time.sleep(5) 
                                        
                                        contact_data = self.detail_parser.extract_contact_popup(page)
                                        if contact_data:
                                            self.logger.info(f"Extracted contact info: {contact_data}")
                                            if 'manager_phone' in contact_data:
                                                full_data['phone_number'] = contact_data['manager_phone']
                                            if 'manager_email' in contact_data:
                                                full_data['email'] = contact_data['manager_email']
                                            full_data.update(contact_data)
                                        
                                        self.navigator.close_modals(page, level=2) 
                                    else:
                                        self.logger.warning("Manager 'View Detail' button not found")
                                except Exception as e_manager:
                                    self.logger.warning(f"Failed to process Manager Contact popup: {e_manager}")

                                self.navigator.close_detail_modal(page)
                            else:
                                self.logger.warning("'Announcement Detail' button not found")

                        except Exception as e_step2:
                            self.logger.warning(f"Step 2 (Announcement Detail) failed: {e_step2}") 

                        # Step 3: "Base Price" (기준금액) Tab
                        try:
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
                                self.navigator.close_modals(page) 
                                
                                try:
                                    tab_btn.click(timeout=5000)
                                except Exception as e_click:
                                    self.logger.warning(f"Standard tab click failed: {e_click}. Trying JS click...")
                                    page.evaluate("arguments[0].click();", tab_btn.element_handle())
                                
                                time.sleep(1)
                                
                                try:
                                    page.wait_for_selector('th:has-text("배정예산"), label:has-text("배정예산"), th:has-text("기초금액")', timeout=3000)
                                except:
                                    time.sleep(1)
                                
                                tab_data = self.detail_parser.parse_page(page, base_data)
                                full_data.update(tab_data)
                            else:
                                self.logger.debug("'Base Price' tab not found")

                        except Exception as e_tab_step:
                            self.logger.warning(f"Failed to process 'Base Price' tab: {e_tab_step}")
                            self.navigator.close_modals(page)

                        # Return to list view
                        try:
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
                                        back_btn.click()
                                        time.sleep(2)
                                        clicked_back = True
                                        break
                                except:
                                    continue

                            if not clicked_back:
                                self.navigator.ensure_on_list_page(page)

                        except Exception as e:
                            self.logger.warning(f"Failed to return to list view: {e}")
                            self.navigator.ensure_on_list_page(page)

                        if not full_data.get('opening_date'):
                            raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                        self.logger.debug(f"Returned to list page after in-page detail for {bid_no}")
                        return full_data

                except Exception as e_spa:
                    self.logger.debug(f"In-page check failed for {bid_no}: {e_spa}")
                    if "Validation Failed" in str(e_spa):
                        raise e_spa

            self.logger.warning(f"Failed to open detail page for {bid_no} (tried all methods)")
            raise Exception(f"Failed to open detail page for {bid_no} (tried all methods)")

        except Exception as e:
            self.logger.error(f"Failed to fetch detail page: {e}")
            self.navigator.close_modals(page) # Safety cleanup
            raise e
