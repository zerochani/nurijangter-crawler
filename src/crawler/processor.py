
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
    개별 입찰 공고 처리를 담당합니다 (상세 페이지 수집 포함).
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
        # 내부 상태
        self.consecutive_duplicates = 0
        self.early_exit_threshold = 30 # Default, should come from config

    def process_notice(self, page, notice_data: Dict[str, Any], current_page_num: int = 1) -> None:
        """
        단일 입찰 공고를 처리합니다 (필요 시 상세 페이지 수집).

        Args:
            page: Playwright 페이지 객체
            notice_data: 목록 페이지에서 추출한 데이터
            current_page_num: 복구를 위한 컨텍스트
        """
        try:
            bid_notice_number = notice_data.get('bid_notice_number', '')

            # Check if already processed
            # 이미 처리된 항목인지 확인
            if self.checkpoint_manager.is_item_processed(bid_notice_number):
                # self.logger.log_skip("Already processed", bid_notice_number) # Interface change needed
                self.logger.info(f"건너뜀 {bid_notice_number}: 이미 처리됨")
                self.stats['items_skipped'] += 1
                return

            # Check for duplicates
            # 중복 확인
            if self.dedup_manager.is_duplicate(notice_data):
                # self.logger.log_skip("Duplicate", bid_notice_number)
                self.logger.info(f"건너뜀 {bid_notice_number}: 중복됨")
                self.stats['items_skipped'] += 1
                self.checkpoint_manager.mark_item_processed(bid_notice_number)
                
                # Increment consecutive duplicates counter
                # 연속 중복 카운터 증가
                self.consecutive_duplicates += 1
                return
            
            # Reset counter since we found a new item
            # 새로운 항목을 찾았으므로 카운터 초기화
            self.consecutive_duplicates = 0

            # Fetch detail page if link available or has_detail flag set (SPA)
            # 링크가 있거나 has_detail 플래그가 설정된 경우 상세 페이지 수집 (SPA)
            detail_link = notice_data.get('detail_link')
            has_detail = notice_data.get('has_detail', False)
            
            if detail_link or has_detail:
                full_data = self.fetch_detail_page(page, detail_link or "", notice_data, current_page_num)
            else:
                full_data = notice_data

            # Create BidNotice object
            # BidNotice 객체 생성
            try:
                bid_notice = BidNotice(**full_data)
            except Exception as e:
                self.logger.warning(f"BidNotice 객체 생성 실패: {e}")
                # Store as-is in additional_info
                # additional_info에 원본 그대로 저장
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
            # 수집된 공고에 추가
            self.collected_notices.add_notice(bid_notice)

            # VALIDATION: Check for data quality
            # 유효성 검사: 데이터 품질 확인
            # If critical fields are missing, we might want to retry later
            # 필수 필드가 누락된 경우 나중에 재시도할 수 있음
            # User request: "null이 많은 데이터는 실패된 항목에 추가"
            null_count = 0
            critical_fields = ['budget_amount', 'base_price', 'opening_date', 'pre_qualification', 'contract_bond']
            for field in critical_fields:
                if not getattr(bid_notice, field):
                    null_count += 1
            
            # If mostly empty (heuristic: 3 or more critical fields missing), consider it a partial failure
            # 대부분 비어 있는 경우 (휴리스틱: 필수 필드 4개 이상 누락), 부분 실패로 간주
            # But we already added it to collection. 
            # Strategy: If it's REALLY bad, remove from collection and raise exception so it goes to failed_items
            if null_count >= 4 and not bid_notice.notes: # notes might explain why (e.g. cancelled)
                self.collected_notices.notices.pop() # Remove the last added item
                raise ValueError(f"누락된 필드가 너무 많음 ({null_count}/{len(critical_fields)} 주요 필드 누락). 재시도를 위해 실패로 처리합니다.")

            self.stats['items_extracted'] += 1

            # Mark as seen
            # 이미 본 항목으로 표시
            self.dedup_manager.mark_as_seen(notice_data)

            # Mark as processed
            # 처리 완료 표시
            self.checkpoint_manager.mark_item_processed(bid_notice_number)

            self.logger.debug(f"처리 완료: {bid_notice_number}")

        except Exception as e:
            self.logger.error(f"공고 처리에 실패했습니다: {e}")
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
        클릭-새 탭(Click-NewTab) 전략을 사용하여 상세 페이지를 가져오고 파싱합니다.
        """
        try:
            # Find the row corresponding to this notice
            # 이 공고에 해당하는 행 찾기
            bid_no = base_data.get('bid_notice_number')
            if not bid_no:
                return base_data

            self.logger.info(f"상세 페이지 수집 시도: {bid_no}")

            # Selector to find the specific row keys
            # 특정 행 키를 찾기 위한 선택자
            # Robust strategy: Find tr containing the bid number, then find the specific name column
            # 강력한 전략: 공고 번호를 포함하는 tr을 찾은 다음, 특정 공고명 컬럼 찾기
            # Use XPath to find row that has the bid number in one of its cells
            
            # Ensure no modals are blocking
            # 차단하는 모달이 없는지 확인
            self.navigator.close_modals(page)

            # Get the correct frame for the list
            # 목록에 대한 올바른 프레임 가져오기
            list_frame = self.navigator.get_list_frame(page)

            # Search in list frame and all frames
            # 목록 프레임과 모든 프레임에서 검색
            row_selector = f"//tr[contains(@class, 'grid_body_row')][.//td[@col_id='bidPbancNum'][contains(., '{bid_no}')]]"
            row = None
            
            # 1. List Frame search
            # 1. 목록 프레임 검색
            if list_frame.locator(row_selector).count() > 0:
                row = list_frame.locator(row_selector).first
                if not row.is_visible():
                    row = None
            
            # 2. Frame search if not found or hidden
            # 2. 찾지 못했거나 숨겨진 경우 프레임 검색
            if not row:
                for frame in page.frames:
                    try:
                        if frame.locator(row_selector).count() > 0:
                            possible_row = frame.locator(row_selector).first
                            if possible_row.is_visible():
                                row = possible_row
                                self.logger.debug(f"프레임에서 {bid_no}에 대한 행 발견: {frame.name or frame.url}")
                                break
                    except: continue

            # 3. IF ROW NOT FOUND: Force Reset Logic
            # 3. 행을 찾지 못한 경우: 강제 리셋 로직
            # If we can't find the row, the view might be stuck in a sub-frame or tab.
            # 행을 찾지 못하면 뷰가 하위 프레임이나 탭에 갇혀 있을 수 있습니다.
            # We force a refresh of the list view.
            # 목록 뷰의 새로고침을 강제합니다.
            if not row or not row.is_visible():
                self.logger.warning(f"{bid_no}에 대한 행을 찾을 수 없음, SOFT RESET 시도...")
                self.navigator.soft_reset_list_view(page)
                
                # RESTORE PAGINATION if needed
                # 필요한 경우 페이지네이션 복구
                if current_page_num > 1:
                    try:
                        self.navigator.restore_pagination(page, current_page_num)
                    except Exception as e:
                        self.logger.warning(f"Soft Reset 중 페이지네이션 복구 실패: {e}")
                        # Continue to check row, it will likely fail and trigger Hard Reset
                        # 행 확인을 계속합니다. 실패 시 Hard Reset이 트리거될 것입니다.

                # Re-try finding row after soft reset
                # Soft reset 후 행 찾기 재시도
                list_frame = self.navigator.get_list_frame(page)
                if list_frame.locator(row_selector).count() > 0:
                    row = list_frame.locator(row_selector).first
            
            # If STILL not found, try HARD RESET
            # 여전히 찾지 못한 경우, HARD RESET 시도
            if not row or not row.is_visible():
                self.logger.warning(f"{bid_no}에 대한 행을 여전히 찾을 수 없음, HARD RESET 시도...")
                self.navigator.hard_reset_via_menu(page)
                
                # RESTORE PAGINATION if needed (Critical step)
                # 필요한 경우 페이지네이션 복구 (중요 단계)
                if current_page_num > 1:
                    try:
                        self.navigator.restore_pagination(page, current_page_num)
                    except Exception as e:
                        self.logger.error(f"Hard Reset 중 페이지네이션 복구 실패: {e}")
                        # If this fails, we are truly lost for this page, but maybe next item will work
                        # 실패하면 이 페이지는 놓치게 되지만, 다음 항목은 작동할 수도 있습니다.

                # Re-try finding row after hard reset
                # Hard reset 후 행 찾기 재시도
                list_frame = self.navigator.get_list_frame(page)
                if list_frame.locator(row_selector).count() > 0:
                    row = list_frame.locator(row_selector).first

            if not row or not row.is_visible():
                self.logger.warning(f"HEAD RESET 및 복구 후에도 {bid_no}에 대한 행을 찾을 수 없음")
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
                self.logger.debug("특정 'a' 태그를 찾지 못함, 일반 자식 요소 시도...")
                link = name_cell.locator("span, div, nobr").first
            
            if not link.count() or not link.is_visible():
                # Fallback to cell itself if no inner container found
                self.logger.debug("내부 링크/컨테이너를 찾지 못함, 셀 자체 클릭으로 폴백")
                link = name_cell
            
            if not link.is_visible():
                self.logger.warning(f"{bid_no}에 대한 이름 셀/링크를 찾을 수 없음")
                return base_data

            self.logger.debug(f"{bid_no}에 대한 링크 요소 발견, 클릭 중...")


            # Click and wait for new page or modal
            # 클릭 후 새 페이지 또는 모달 대기
            new_page = None
            detail_opened = False

            # Method 1: Try new tab (shortest timeout since it usually works immediately)
            # 방법 1: 새 탭 시도 (보통 즉시 작동하므로 타임아웃 짧게 설정)
            try:
                with page.context.expect_page(timeout=3000) as new_page_info:
                    # Use JS click as native click might be swallowed by event handlers
                    # 원시 클릭이 이벤트 핸들러에 의해 삼켜질 수 있으므로 JS 클릭 사용
                    link.evaluate("el => el.click()")
                new_page = new_page_info.value
                new_page.wait_for_load_state()
                self.logger.info(f"{bid_no} 상세 페이지 열림 (새 탭)")
                detail_opened = True

                # Parse detail page from new tab
                # 새 탭에서 상세 페이지 파싱
                full_data = self.detail_parser.parse_page(new_page, base_data)

                # Close the detail tab
                # 상세 탭 닫기
                new_page.close()

                # CRITICAL: Explicitly return to list page tab
                # 중요: 목록 페이지 탭으로 명시적으로 복귀
                page.bring_to_front()
                time.sleep(1)

                # Verify we're back on the list page
                # 목록 페이지에 돌아왔는지 확인
                self.navigator.ensure_on_list_page(page)

                # VALIDATION: Ensure critical data exists
                # 유효성 검사: 필수 데이터 존재 확인
                if not full_data.get('opening_date'):
                    raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                self.logger.debug(f"{bid_no} 상세 탭 닫고 목록 페이지로 복귀함")
                return full_data

            except Exception:
                self.logger.debug(f"{bid_no} 새 탭으로 열리지 않음, 다른 방법 시도...")

            # Method 2: Check if it opened a modal
            # 방법 2: 모달이 열렸는지 확인
            if not detail_opened:
                try:
                    self.logger.debug(f"{bid_no}에 대한 모달 확인 중...")
                    time.sleep(2)

                    modal_selector = '.w2window_active, .w2window_content_body, div[id^="w2window"]'
                    modal = page.locator(modal_selector).last

                    if modal.count() > 0 and modal.is_visible():
                        self.logger.info(f"{bid_no} 상세 페이지 열림 (모달)")
                        detail_opened = True

                        full_data = self.detail_parser.parse_page(page, base_data)

                        # Close modal with improved method
                        # 개선된 방법으로 모달 닫기
                        self.navigator.close_detail_modal(page)

                        # Verify we're back on the list page
                        # 목록 페이지에 돌아왔는지 확인
                        self.navigator.ensure_on_list_page(page)

                        # VALIDATION: Ensure critical data exists
                        # 유효성 검사: 필수 데이터 존재 확인
                        if not full_data.get('opening_date'):
                            raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                        self.logger.debug(f"{bid_no} 모달 닫고 목록 페이지로 복귀함")
                        return full_data

                except Exception as e_modal:
                    self.logger.debug(f"{bid_no} 모달 확인 실패: {e_modal}")

            # Method 3: In-page content load (SPA style)
            # 방법 3: 페이지 내 콘텐츠 로드 (SPA 스타일)
            if not detail_opened:
                try:
                    self.logger.debug(f"{bid_no}에 대한 페이지 내 상세 콘텐츠 확인 중...")
                    
                    try:
                        self.logger.debug("페이지 내 네비게이션 트리거를 위해 링크 재클릭 (JS)...")
                        link.evaluate("el => el.click()")
                    except: pass

                    # Wait for detail-specific content to appear
                    # 상세 페이지 특정 콘텐츠가 나타날 때까지 대기
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
                        self.logger.debug("고유한 상세 페이지 표시자 발견")
                    except Exception as e:
                        self.logger.debug(f"상세 페이지 감지 실패: {e}")
                        content_found = False

                    if content_found:
                        self.logger.info(f"{bid_no} 상세 페이지 열림 (페이지 내)")
                        detail_opened = True
                        time.sleep(2)

                        # Parse detail page from same page (Step 1: Main View)
                        # 같은 페이지에서 상세 페이지 파싱 (1단계: 메인 뷰)
                        full_data = self.detail_parser.parse_page(page, base_data)
                        self.logger.info(f"{bid_no} 메인 상세 뷰 파싱 완료")

                        # Step 2: "Announcement Detail" (공고상세) Modal
                        # 2단계: "공고상세" 모달
                        try:
                            detail_btn_selector = '#mf_wfm_container_btnBidPbancP'
                            self.navigator.close_modals(page) 
                            
                            detail_btn = page.locator(detail_btn_selector)
                            if detail_btn.is_visible():
                                self.logger.info("'공고상세' 버튼 발견, 클릭 중...")
                                detail_btn.click()
                                page.wait_for_selector('.w2window_content', state='visible', timeout=5000)
                                self.logger.info("공고상세 모달 열림")
                                
                                # Step 3: "Manager Contact" (담당자) Popup
                                # 3단계: "담당자" 팝업
                                try:
                                    manager_btn = page.locator("[id*='btnUsrDtail']").first
                                    if manager_btn.is_visible():
                                        self.logger.info("'담당자 상세보기' 버튼 발견, 클릭 중...")
                                        manager_btn.scroll_into_view_if_needed()
                                        manager_btn.evaluate("el => el.click()")
                                        time.sleep(5) 
                                        
                                        contact_data = self.detail_parser.extract_contact_popup(page)
                                        if contact_data:
                                            self.logger.info(f"담당자 정보 추출: {contact_data}")
                                            if 'manager_phone' in contact_data:
                                                full_data['phone_number'] = contact_data['manager_phone']
                                            if 'manager_email' in contact_data:
                                                full_data['email'] = contact_data['manager_email']
                                            full_data.update(contact_data)
                                        
                                        self.navigator.close_modals(page, level=2) 
                                    else:
                                        self.logger.warning("'담당자 상세보기' 버튼을 찾을 수 없음")
                                except Exception as e_manager:
                                    self.logger.warning(f"담당자 팝업 처리 실패: {e_manager}")

                                self.navigator.close_detail_modal(page)
                            else:
                                self.logger.warning("'공고상세' 버튼을 찾을 수 없음")

                        except Exception as e_step2:
                            self.logger.warning(f"2단계 (공고상세) 실패: {e_step2}") 

                        # Step 3: "Base Price" (기준금액) Tab
                        # 3단계: "기준금액" 탭
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
                                self.logger.info("'기준금액' 탭 발견, 클릭 중...")
                                self.navigator.close_modals(page) 
                                
                                try:
                                    tab_btn.click(timeout=5000)
                                except Exception as e_click:
                                    self.logger.warning(f"기준금액 탭 클릭 실패: {e_click}. JS 클릭 시도...")
                                    page.evaluate("arguments[0].click();", tab_btn.element_handle())
                                
                                time.sleep(1)
                                
                                try:
                                    page.wait_for_selector('th:has-text("배정예산"), label:has-text("배정예산"), th:has-text("기초금액")', timeout=3000)
                                except:
                                    time.sleep(1)
                                
                                tab_data = self.detail_parser.parse_page(page, base_data)
                                full_data.update(tab_data)
                            else:
                                self.logger.debug("'기준금액' 탭을 찾을 수 없음")

                        except Exception as e_tab_step:
                            self.logger.warning(f"기준금액 탭 처리 실패: {e_tab_step}")
                            self.navigator.close_modals(page)

                        # Return to list view
                        # 목록 뷰로 복귀
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
                            self.logger.warning(f"목록 뷰 복귀 실패: {e}")
                            self.navigator.ensure_on_list_page(page)

                        if not full_data.get('opening_date'):
                            raise Exception(f"Validation Failed: opening_date is missing/invalid for {bid_no}")

                        self.logger.debug(f"{bid_no} 상세 뷰 처리 후 목록 페이지로 복귀함")
                        return full_data

                except Exception as e_spa:
                    self.logger.debug(f"{bid_no} 페이지 내 확인 실패: {e_spa}")
                    if "Validation Failed" in str(e_spa):
                        raise e_spa

            self.logger.warning(f"{bid_no} 상세 페이지 열기 실패 (모든 방법 시도)")
            raise Exception(f"Failed to open detail page for {bid_no} (tried all methods)")

        except Exception as e:
            self.logger.error(f"상세 페이지 가져오기 실패: {e}")
            self.navigator.close_modals(page) # Safety cleanup
            # 안전 정리
            raise e
