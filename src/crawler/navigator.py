
import time
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class Navigator:
    """
    누리장터 SPA(Single Page Application)의 네비게이션과 상태 관리를 담당합니다.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.wait_config = config.get('crawler', {}).get('wait', {})
        self.logger = logger  # Use module logger or pass a specific one

    def navigate_to_page(self, page, url: str) -> None:
        """
        적절한 대기 시간과 함께 URL로 이동합니다.
        누리장터 SPA의 경우 초기 설정(팝업 닫기, 메뉴 이동 등)을 처리합니다.
        """
        try:
            timeout = self.wait_config.get('navigation_timeout', 30000)
            page.goto(url, timeout=timeout, wait_until='domcontentloaded')
            # self.logger.log_page_visit(url) # Logger interface change needed

            # NuriJangter specific handling
            # 누리장터 전용 처리
            if "nuri.g2b.go.kr" in url:
                self.handle_nurijangter_spa(page)
        
        except Exception as e:
            self.logger.error(f"Failed to navigate to {url}: {e}")
            raise

    def handle_nurijangter_spa(self, page) -> None:
        """누리장터 SPA의 초기 네비게이션 시퀀스를 처리합니다."""
        self.logger.info("누리장터 SPA 상태 초기화 중...")
        
        # 1. Close Popups
        # 1. 팝업 닫기
        try:
            # Wait a moment for popups to render
            # 팝업 렌더링 대기
            time.sleep(2)
            page.evaluate("document.querySelectorAll('.w2window_close, .btn_cm.close').forEach(btn => btn.click());")
        except Exception:
            pass

        # 2. Navigate to Bid List (Click Menu)
        # 2. 입찰 공고 목록으로 이동 (메뉴 클릭)
        try:
            # Click '입찰공고'
            # '입찰공고' 클릭
            # XPath: //a[contains(@id, 'btn_menuLvl1') and .//span[text()='입찰공고']]
            menu_selector = "//a[contains(@id, 'btn_menuLvl1') and .//span[text()='입찰공고']]"
            
            # Wait for menu to be attached
            menu_link = page.locator(menu_selector).first
            menu_link.wait_for(state="attached", timeout=10000)
            
            # WebSquare sometimes requires force click if overlaid
            # WebSquare는 가려져 있을 때 강제 클릭(force click)이 필요할 수 있음
            try:
                menu_link.click(timeout=5000)
            except Exception:
                self.logger.warning("표준 클릭 실패, 강제 클릭 시도")
                menu_link.click(force=True)
            
            time.sleep(1)
            
            # Click '입찰공고목록'
            # '입찰공고목록' 클릭
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
            # 3. 목록을 채우기 위해 검색 버튼 클릭
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            search_btn.wait_for(state="visible", timeout=10000)
            search_btn.click()
            
            # Wait for grid to load
            # 그리드 로드 대기
            page.wait_for_selector('#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table', timeout=10000)
            time.sleep(2) # Extra buffer for grid render (그리드 렌더링을 위한 추가 버퍼)
            
        except Exception as e:
            self.logger.warning(f"SPA navigation warning: {e}")

    def reload_list_page(self, page):
        """
        목록 페이지를 완전히 새로고침하여 깨끗한 상태로 만듭니다.
        단순 뷰 초기화보다 더 확실한 방법입니다.
        """
        try:
            self.logger.debug("상태 정화를 위해 목록 페이지 새로고침 중...")

            # 1. Close any modals
            # 1. 모든 모달 닫기
            self.close_modals(page)
            time.sleep(1)

            # 2. Click on the list menu again to reload
            # 2. 목록 메뉴를 다시 클릭하여 새로고침
            # 이렇게 하면 목록 뷰로 확실히 돌아옵니다.
            menu_selector = "//a[contains(@id, 'btn_menuLvl3') and contains(., '입찰공고목록')]"

            # Try to find in all frames
            # 모든 프레임에서 찾기 시도
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
                time.sleep(2)  # Wait for page transition (페이지 전환 대기)

            # 3. Click search button to reload list
            # 3. 목록을 다시 로드하기 위해 검색 버튼 클릭
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            if search_btn.count() > 0 and search_btn.is_visible():
                search_btn.click(force=True)
                time.sleep(3)  # Wait for search results (검색 결과 대기)

                # Wait for grid
                # 그리드 대기
                try:
                    page.wait_for_selector("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table", timeout=5000)
                    time.sleep(1)
                    self.logger.debug("목록 페이지 새로고침 성공")
                except:
                    self.logger.warning("새로고침 후 그리드를 찾을 수 없음")
            else:
                self.logger.debug("새로고침을 위한 검색 버튼을 찾을 수 없음")

        except Exception as e:
            self.logger.debug(f"List page reload failed: {e}")

    def soft_reset_list_view(self, page):
        """검색 버튼 클릭 또는 네비게이션을 통해 목록 뷰를 강제로 초기화합니다."""
        try:
            # 1. Try closing modals first
            # 1. 모달 먼저 닫기 시도
            self.close_modals(page)

            # 2. Find Search button and click to refresh list
            # 2. 검색 버튼을 찾아 클릭하여 목록 새로고침
            # Common search button IDs in NuriJangter
            search_btn_selectors = [
                '#mf_wfm_container_btnS0001',
                '.btn_cm.search',
                'button[class*="search"]',
                '#mf_wfm_container_scBtn'
            ]

            # Try to find button in all frames
            # 모든 프레임에서 버튼 찾기 시도
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
                self.logger.debug("검색 버튼을 통한 목록 뷰 초기화 (Soft Reset)...")
                btn.click(force=True)
                time.sleep(3) # Wait for grid refresh (increased from 2s to 3s)

                # Wait for grid to actually appear AND contain rows
                # 그리드가 실제로 나타나고 행이 포함될 때까지 대기
                try:
                    # Wait for the table body to be visible
                    page.wait_for_selector("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table", timeout=5000)
                    # CRITICAL: Wait for actual rows to be present
                    # 중요: 실제 행이 나타날 때까지 대기
                    page.wait_for_selector("tr.grid_body_row", timeout=5000)
                    
                    # Also wait for pagination to ensure full load
                    # 페이지네이션도 대기하여 로드 완료 확인
                    try:
                        page.wait_for_selector("#mf_wfm_container_pagelist, .w2pageList", timeout=3000)
                    except:
                        self.logger.warning("Soft Reset 후 페이지네이션 컨테이너를 찾지 못함")
                        
                    time.sleep(1) # Extra buffer
                except:
                    pass
            else:
                self.logger.debug("Soft Reset을 위한 검색 버튼을 찾을 수 없음")

        except Exception as e:
            self.logger.debug(f"List view soft reset failed: {e}")

    def hard_reset_via_menu(self, page):
        """
        Hard reset: 사이드바 메뉴 '입찰공고목록'을 클릭하여 목록 컨텍스트를 완전히 새로고침합니다.
        모달에 의해 페이지 상태가 꼬였을 때 단순 검색보다 더 확실한 방법입니다.
        """
        self.logger.info("사이드바 메뉴를 통한 HARD RESET 트리거...")
        try:
            # 1. Close any blocking modals first
            # 1. 차단하는 모달 먼저 닫기
            self.close_modals(page)
            
            # 2. Click '입찰공고' (Level 1 Menu) just in case
            # 2. 혹시 모르니 '입찰공고' (1단계 메뉴) 클릭
            try:
                menu_l1 = page.locator("//a[contains(@id, 'btn_menuLvl1') and .//span[text()='입찰공고']]").first
                if menu_l1.is_visible():
                    menu_l1.click(force=True)
                    time.sleep(0.5)
            except: pass

            # 3. Click '입찰공고목록' (Level 3 Menu - The actual list link)
            # 3. '입찰공고목록' 클릭 (3단계 메뉴 - 실제 목록 링크)
            # Use specific ID pattern or text content
            menu_l3 = page.locator("//a[contains(@id, 'btn_menuLvl3') and contains(., '입찰공고목록')]").first
            menu_l3.click(force=True)
            self.logger.info("'입찰공고목록' 메뉴 클릭됨")
            
            time.sleep(2)
            
            # 4. Click Search to populate
            # 4. 목록 채우기 위해 검색 클릭
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            if search_btn.is_visible():
                search_btn.click(force=True)
                self.logger.info("메뉴 리셋 후 검색 버튼 클릭됨")
                
                # Wait for grid to actually populate - CRITICAL
                # 그리드가 실제로 채워질 때까지 대기 - 중요
                try:
                    # Wait for overlay to disappear
                    # 오버레이 사라짐 대기
                    page.wait_for_selector('div[id*="processbar"]', state='hidden', timeout=5000)
                    
                    # Wait for at least one row
                    # 최소 한 개의 행 대기
                    page.wait_for_selector("tr.grid_body_row", state='visible', timeout=15000)
                    
                    # Double check row count
                    # 행 개수 재확인
                    row_count = page.locator("tr.grid_body_row").count()
                    self.logger.info(f"Hard reset 완료. 그리드에 {row_count}개의 행이 채워짐.")
                    time.sleep(1)
                except Exception as e_wait:
                    self.logger.warning(f"Hard reset 후 그리드가 채워지지 않음: {e_wait}")
        
        except Exception as e:
            self.logger.warning(f"Hard reset 실패: {e}")

    def restore_pagination(self, page, target_page_num):
        """
        리셋 후 특정 페이지 번호로 목록을 복구합니다.
        (리셋 직후에는 1페이지에 있다고 가정합니다)
        """
        if target_page_num <= 1:
            return

        self.logger.info(f"페이지네이션 복구 중: {target_page_num}페이지로 이동...")
        
        try:
            # 0. Ensure no blocking modals
            # 0. 차단하는 모달 없는지 확인
            self.close_modals(page)
            time.sleep(1)

            # We need to jump from Page 1 to Target Page
            # The logic depends on how far we need to go.
            # Page groups are usually 10 pages (1-10, 11-20, etc.)
            
            # Current known state: Page 1
            current_group_start = 1
            target_group_start = ((target_page_num - 1) // 10) * 10 + 1
            
            # Navigate groups if needed
            # 필요한 경우 그룹 단위 이동 (10페이지씩 점프)
            while current_group_start < target_group_start:
                self.logger.debug(f"{current_group_start}에서 다음 그룹으로 점프...")
                
                # Check for blocking modals again before click
                if page.locator('.w2window_active, .w2window_cover').count() > 0:
                     self.close_modals(page)
                
                # Click next group button (>)
                # 다음 그룹 버튼(>) 클릭
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
                    self.logger.error("복구 중 다음 그룹 버튼을 찾을 수 없음")
                    raise Exception("Next group button not found")

            # Now we are in the correct group (or close to it)
            # 이제 올바른 그룹(또는 그 근처)에 도달함
            # Check for blocking modals again before click
            if page.locator('.w2window_active, .w2window_cover').count() > 0:
                    self.close_modals(page)

            # Click the specific page number
            # 특정 페이지 번호 클릭
            page_btn_selector = f"#mf_wfm_container_pagelist_page_{target_page_num}"
            
            # Try ID first
            # ID 우선 시도
            if page.locator(page_btn_selector).count() > 0:
                 page.locator(page_btn_selector).first.click(force=True) # Use force click
                 time.sleep(2)
                 self.logger.info(f"{target_page_num}페이지로 복구되었습니다")
                 return

            # Fallback: Try finding by text
            # 폴백: 텍스트로 찾기 시도
            # This is risky if multiple numbers exist, but usually pagination is unique in the footer
            try:
                # Find link/li inside pagelist
                page.locator(f".w2pageList_li:has-text('{target_page_num}')").first.click(force=True)
                time.sleep(2)
                self.logger.info(f"텍스트 매칭을 통해 {target_page_num}페이지로 복구되었습니다")
            except:
                # Debugging: Log what is actually visible
                try:
                    visible_pages = page.locator(".w2pageList_li, .w2pageList_label").all_inner_texts()
                    self.logger.error(f"보이는 페이지네이션 버튼들: {visible_pages}")
                except:
                    pass
                self.logger.error(f"{target_page_num}페이지 버튼을 찾을 수 없음")
                raise Exception(f"Page button {target_page_num} not found")

        except Exception as e:
            self.logger.error(f"페이지네이션 복구 실패: {e}")
            raise # Re-raise to let Engine handle it (abort crawl)

    def get_list_frame(self, page):
        """입찰 공고 목록이 있는 프레임을 찾습니다."""
        # 1. Check main page first
        # 1. 메인 페이지 먼저 확인
        if page.locator("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table").count() > 0:
            return page
            
        # 2. Check all frames
        # 2. 모든 프레임 확인
        for frame in page.frames:
            try:
                if frame.locator("#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table").count() > 0:
                    return frame
            except: continue
            
        return page # Fallback to page

    def close_detail_modal(self, page):
        """
        상세 페이지 모달을 더 확실하게 닫습니다.
        이 메서드는 모든 모달이 아니라 상세 모달을 닫는 데 집중합니다.
        """
        self.logger.debug("상세 모달 닫는 중...")

        try:
            # Strategy 1: Find and click the close button in the active modal
            # 전략 1: 활성 모달에서 닫기 버튼 찾아 클릭
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
                        self.logger.debug(f"닫기 버튼 발견 (선택자: {selector})")
                        close_btn.click(force=True)
                        time.sleep(1.5)
                        modal_closed = True
                        break
                except:
                    continue

            # Strategy 2: If no close button found, try ESC key
            # 전략 2: 닫기 버튼 없으면 ESC 키 시도
            if not modal_closed:
                self.logger.debug("닫기 버튼 없음, ESC 키 시도...")
                page.keyboard.press("Escape")
                time.sleep(1.5)

            # Strategy 3: If still visible, try JavaScript removal
            # 전략 3: 여전히 보이면 JavaScript로 강제 제거
            if page.locator('.w2window_active, .w2window_content_body').is_visible():
                self.logger.debug("모달이 여전히 보임, JS로 제거 시도...")
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

            self.logger.debug("상세 모달 닫기 완료")

        except Exception as e:
            self.logger.warning(f"상세 모달 닫기 중 에러: {e}")

    def ensure_on_list_page(self, page):
        """
        목록 페이지에 있고 목록 그리드가 보이는지 확인합니다.
        그렇지 않으면 복구를 시도합니다.
        """
        self.logger.debug("목록 페이지 확인 중...")

        try:
            # Check if list grid is visible
            # 목록 그리드가 보이는지 확인
            list_grid_selector = '#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table, tr.grid_body_row'

            # Give it a moment to appear
            # 나타날 때까지 잠시 대기
            try:
                # Use .first to avoid strict mode violation if multiple rows match
                page.wait_for_selector(list_grid_selector, timeout=3000, state='visible')
                self.logger.debug("✓ 목록 그리드 보임")
                return True
            except:
                self.logger.warning("목록 그리드가 보이지 않음, 복구 시도 중...")
                
                # FIRST ATTEMPT: Close any modals that might be blocking the view
                # 첫 번째 시도: 시야를 가리는 모달 닫기
                self.close_modals(page)
                
                # Check again
                # 재확인
                if page.locator(list_grid_selector).first.is_visible():
                    self.logger.debug("✓ 모달 닫기 후 목록 그리드가 보임")
                    return True

            # Recovery: Click on the list menu tab if it exists
            # 복구: 목록 메뉴 탭이 있으면 클릭
            list_tab_selectors = [
                "//a[contains(., '입찰공고목록')]",
                '.w2tabcontrol_tab:first-child',
                'a[id*="tab"]:first-child',
            ]

            for selector in list_tab_selectors:
                try:
                    tab = page.locator(selector).first
                    if tab.is_visible():
                        self.logger.debug(f"목록 탭 클릭: {selector}")
                        tab.click(force=True)
                        time.sleep(2)

                        # Check again if grid is visible
                        if page.locator(list_grid_selector).first.is_visible():
                            self.logger.debug("✓ 목록 페이지로 복귀 성공")
                            return True
                except:
                    continue

            # If still not visible, try search button to refresh
            # 여전히 안 보이면 검색 버튼을 눌러 새로고침 시도
            try:
                search_btn = page.locator('#mf_wfm_container_btnS0001')
                if search_btn.is_visible():
                    self.logger.debug("목록 새로고침을 위해 검색 버튼 클릭...")
                    search_btn.click(force=True)
                    time.sleep(3)

                    if page.locator(list_grid_selector).first.is_visible():
                        self.logger.debug("✓ 목록 새로고침 성공")
                        return True
            except:
                pass

            self.logger.warning("목록 페이지 가시성 확인 실패")
            return False

        except Exception as e:
            self.logger.warning(f"목록 페이지 확인 중 에러: {e}")
            return False

    def close_modals(self, page, level=None):
        """WebSquare 모달과 MDI 탭을 닫습니다."""
        self.logger.debug("모달을 닫고 목록 탭으로 복귀 중...")

        # Max retries to ensure we don't get stuck
        # 무한 루프 방지를 위한 최대 재시도
        for i in range(3):
            # Check for modals OR active MDI tabs (excluding the main list tab)
            # 모달 또는 활성 MDI 탭 확인 (메인 목록 탭 제외)
            has_modal = page.locator('.w2window, .w2window_active, .w2window_content_body, iframe[src*="popup"], div[id^="w2window"]').count() > 0
            has_overlay = page.locator('#_modal, .w2modal_popup, .w2window_mask, .w2window_cover').count() > 0

            # Simple MDI close buttons matching common WebSquare tab patterns
            has_tab = page.locator('.w2tabcontrol_tab_close, .close_tab, .tab_close').count() > 1

            if not has_modal and not has_overlay and not has_tab:
                # No modals or extra tabs, we're done
                # 모달이나 추가 탭이 없으면 종료
                break

            try:
                # 1. Try Escape key (often closes the top-most modal)
                # 1. ESC 키 시도 (보통 최상위 모달을 닫음)
                page.keyboard.press("Escape")
                time.sleep(0.5)

                # 2. Click standard modal close buttons (safe to click all)
                # 2. 표준 모달 닫기 버튼 클릭 (모두 클릭해도 안전)
                # FIX: Use double quotes for the JS string to avoid conflict with single quotes in selector
                modal_close_selector = ".w2window_close, .btn_cm.close, .w2window_close_icon, .close_button, iframe[src*='popup'] .close, div[id^='w2window'] .close"
                page.evaluate(f'document.querySelectorAll("{modal_close_selector}").forEach(btn => btn.click());')

                # 3. Force Remove Blocking Overlays (The "Hammer" approach)
                # 3. 차단 레이어 강제 제거 (강력한 방법)
                # If a modal overlay is intercepting clicks but has no close button, destroy it.
                page.evaluate("""
                    document.querySelectorAll('#_modal, .w2modal_popup, .w2window_mask, .w2window_cover').forEach(el => {
                        el.style.display = 'none';
                        el.remove();
                    });
                """)

                # 4. Handle Tab Close - CAREFULLY
                # 4. 탭 닫기 - 주의해서 처리
                # Only click the LAST tab's close button if there are multiple tabs
                # 탭이 여러 개일 때만 '마지막' 탭의 닫기 버튼 클릭
                tabs = page.locator('.w2tabcontrol_tab_close, .close_tab, .tab_close')
                if tabs.count() > 1:
                     self.logger.debug(f"추가 탭 닫는 중 ({tabs.count()}개 탭 존재)...")
                     tabs.last.click(force=True)

                time.sleep(1.0)

            except Exception as e:
                self.logger.debug(f"close_modals 반복 {i} 중 에러: {e}")
                pass

        # 5. Explicitly click on the FIRST tab to ensure we're on the list view
        # 5. 목록 뷰에 있는지 확인하기 위해 명시적으로 첫 번째 탭 클릭
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
                        self.logger.debug("첫 번째 탭(목록 뷰)으로 전환 중...")
                        first_tab.click(force=True)
                        time.sleep(1)
        except Exception as e:
            self.logger.debug(f"첫 번째 탭으로 전환 실패: {e}")

        self.logger.debug("모달 닫기 완료")

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
