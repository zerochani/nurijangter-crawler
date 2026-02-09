
import time
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class Navigator:
    """
    Handles navigation and state management for NuriJangter SPA.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.wait_config = config.get('crawler', {}).get('wait', {})
        self.logger = logger  # Use module logger or pass a specific one

    def navigate_to_page(self, page, url: str) -> None:
        """
        Navigate to a URL with proper waiting.
        For NuriJangter SPA, this handles initial setup (popups, menu).
        """
        try:
            timeout = self.wait_config.get('navigation_timeout', 30000)
            page.goto(url, timeout=timeout, wait_until='domcontentloaded')
            # self.logger.log_page_visit(url) # Logger interface change needed

            # NuriJangter specific handling
            if "nuri.g2b.go.kr" in url:
                self.handle_nurijangter_spa(page)
        
        except Exception as e:
            self.logger.error(f"Failed to navigate to {url}: {e}")
            raise

    def handle_nurijangter_spa(self, page) -> None:
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

    def reload_list_page(self, page):
        """
        Reload the list page completely to ensure clean state.
        This is more reliable than trying to reset the view.
        """
        try:
            self.logger.debug("Reloading list page for clean state...")

            # 1. Close any modals
            self.close_modals(page)
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

    def soft_reset_list_view(self, page):
        """Force reset the list view by clicking Search or navigating."""
        try:
            # 1. Try closing modals first
            self.close_modals(page)

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
                    
                    # Also wait for pagination to ensure full load
                    try:
                        page.wait_for_selector("#mf_wfm_container_pagelist, .w2pageList", timeout=3000)
                    except:
                        self.logger.warning("Pagination container not found after soft reset")
                        
                    time.sleep(1) # Extra buffer
                except:
                    pass
            else:
                self.logger.debug("Could not find Search button for soft reset")

        except Exception as e:
            self.logger.debug(f"List view soft reset failed: {e}")

    def hard_reset_via_menu(self, page):
        """
        Hard reset: Click the sidebar menu '입찰공고목록' to fully reload the list context.
        This is more reliable than just clicking search if the page state is corrupted by modals.
        """
        self.logger.info("Triggering HARD RESET via Sidebar Menu...")
        try:
            # 1. Close any blocking modals first
            self.close_modals(page)
            
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

    def restore_pagination(self, page, target_page_num):
        """
        Restore the list to the specific page number after a reset.
        Assumes we are currently on page 1 (after reset).
        """
        if target_page_num <= 1:
            return

        self.logger.info(f"Restoring pagination to page {target_page_num}...")
        
        try:
            # 0. Ensure no blocking modals
            self.close_modals(page)
            time.sleep(1)

            # We need to jump from Page 1 to Target Page
            # The logic depends on how far we need to go.
            # Page groups are usually 10 pages (1-10, 11-20, etc.)
            
            # Current known state: Page 1
            current_group_start = 1
            target_group_start = ((target_page_num - 1) // 10) * 10 + 1
            
            # Navigate groups if needed
            while current_group_start < target_group_start:
                self.logger.debug(f"Jumping to next group from {current_group_start}...")
                
                # Check for blocking modals again before click
                if page.locator('.w2window_active, .w2window_cover').count() > 0:
                     self.close_modals(page)
                
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
                    raise Exception("Next group button not found")

            # Now we are in the correct group (or close to it)
            # Check for blocking modals again before click
            if page.locator('.w2window_active, .w2window_cover').count() > 0:
                    self.close_modals(page)

            # Click the specific page number
            page_btn_selector = f"#mf_wfm_container_pagelist_page_{target_page_num}"
            
            # Try ID first
            if page.locator(page_btn_selector).count() > 0:
                 page.locator(page_btn_selector).first.click(force=True) # Use force click
                 time.sleep(2)
                 self.logger.info(f"Restored to page {target_page_num}")
                 return

            # Fallback: Try finding by text
            # This is risky if multiple numbers exist, but usually pagination is unique in the footer
            try:
                # Find link/li inside pagelist
                page.locator(f".w2pageList_li:has-text('{target_page_num}')").first.click(force=True)
                time.sleep(2)
                self.logger.info(f"Restored to page {target_page_num} via text match")
            except:
                # Debugging: Log what is actually visible
                try:
                    visible_pages = page.locator(".w2pageList_li, .w2pageList_label").all_inner_texts()
                    self.logger.error(f"Visible pagination buttons: {visible_pages}")
                except:
                    pass
                self.logger.error(f"Failed to find button for page {target_page_num}")
                raise Exception(f"Page button {target_page_num} not found")

        except Exception as e:
            self.logger.error(f"Pagination restoration failed: {e}")
            raise # Re-raise to let Engine handle it (abort crawl)

    def get_list_frame(self, page):
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

    def close_detail_modal(self, page):
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

    def ensure_on_list_page(self, page):
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
                self.close_modals(page)
                
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

    def close_modals(self, page, level=None):
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

    def wait_for_page_load(self, page) -> None:
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

    def rate_limit(self) -> None:
        """Apply rate limiting delay."""
        delay = self.wait_config.get('between_pages', 1000)
        if delay > 0:
            time.sleep(delay / 1000)
