
import logging
import time
from typing import Dict, Any, List

from ..checkpoint import CheckpointManager
from ..utils import DeduplicationManager

logger = logging.getLogger(__name__)

class RetryManager:
    """
    실패한 항목 처리 및 특정 입찰 공고 검색을 담당하는 클래스입니다.
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
        이전 크롤링에서 실패한 항목들을 재시도합니다.
        검색 기능을 사용하여 ID로 항목을 찾습니다.
        """
        self.logger.info("실패한 항목 재시도 시작...")
        
        # Load checkpoint to get failed items
        # 체크포인트를 로드하여 실패한 항목 가져오기
        if not self.checkpoint_manager.load_checkpoint():
            self.logger.warning("체크포인트를 찾을 수 없음. 재시도 불가.")
            return

        failed_items = self.checkpoint_manager.get_failed_items()
        if not failed_items:
            self.logger.info("재시도할 실패 항목이 없습니다.")
            return

        self.logger.info(f"재시도할 실패 항목 {len(failed_items)}개 발견.")

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
        제공된 페이지 객체를 사용하여 재시도 루프를 실행합니다.
        이는 기존 retry_failed_items의 브라우저 관리 부분을 대체합니다.
        """
        # Load checkpoint to get failed items
        # 체크포인트를 로드하여 실패한 항목 가져오기
        if not self.checkpoint_manager.load_checkpoint():
            self.logger.warning("체크포인트를 찾을 수 없음. 재시도 불가.")
            return

        failed_items = self.checkpoint_manager.get_failed_items()
        if not failed_items:
            self.logger.info("재시도할 실패 항목이 없습니다.")
            return
            
        # Navigate to list page
        # 목록 페이지로 이동
        list_url = self.config.get('website', {}).get('list_page_url', '')
        self.navigator.navigate_to_page(page, list_url)

        # Process each failed item
        # 각 실패 항목 처리
        retry_count = 0
        success_count = 0
        
        for item in failed_items:
            bid_no = item.get('item_id')
            if not bid_no:
                continue
            
            retry_count += 1
            self.logger.info(f"재시도 {retry_count}/{len(failed_items)}: {bid_no}")
            
            try:
                # Search and process
                # 검색 및 처리
                if self.search_and_process_item(page, bid_no):
                    success_count += 1
                    # Remove from failed items in checkpoint
                    # 체크포인트에서 실패 항목 제거
                    self.checkpoint_manager.remove_failed_item(bid_no)
                    self.logger.info(f"재시도 성공 및 실패 목록에서 제거: {bid_no}")
                    
                    # Save deduplication state immediately to keep in sync
                    # 동기화 유지를 위해 중복 제거 상태 즉시 저장
                    self.dedup_manager.save()
                else:
                    self.logger.warning(f"{bid_no} 재시도 실패")
                    
            except Exception as e:
                self.logger.error(f"{bid_no} 재시도 중 오류 발생: {e}")

        self.logger.info(f"재시도 완료. 성공: {success_count}/{len(failed_items)}")
        
        # Save data
        # 데이터 저장
        if self.save_callback:
            self.save_callback()

    def search_and_process_item(self, page, bid_no: str) -> bool:
        """
        특정 입찰 공고 번호를 검색하고 처리합니다.
        
        Args:
            page: Playwright 페이지 객체
            bid_no: 검색할 입찰 공고 번호
            
        Returns:
            성공 시 True, 그렇지 않으면 False
        """
        try:
            # 1. Reset/Clear Search
            # 1. 검색 초기화
            # Click '입찰공고목록' to reset state
            # '입찰공고목록'을 클릭하여 상태 초기화
            self.logger.debug("목록 뷰로 초기화 중...")
            self.navigator.handle_nurijangter_spa(page) # Re-run nav sequence to ensure clean state (깨끗한 상태 보장을 위해 네비게이션 시퀀스 재실행)
            
            # 2. Enter Bid Number in Search Box
            # 2. 검색창에 입찰 공고 번호 입력
            self.logger.info(f"{bid_no} 검색 중...")
            
            # Find input box
            # 입력창 찾기
            input_box = None
            
            # Try specific ID first (most reliable if known)
            # 특정 ID 우선 시도 (알려진 경우 가장 신뢰성 높음)
            search_ids = [
                '#mf_wfm_container_tbxBidPbancNo', # Correct ID found via debug (디버그를 통해 찾은 정확한 ID)
                '#mf_wfm_container_txtBidPbancNum',
                'input[id*="BidPbancNo"]',
                'input[id*="bidPbancNo"]'
            ]
            
            # 모든 ID 선택자 순회
            for selector in search_ids:
                if page.locator(selector).count() > 0:
                    input_box = page.locator(selector).first
                    break
            
            # Fallback: Label strategy
            # 폴백: 라벨 전략
            if not input_box:
                self.logger.warning("특정 ID를 찾지 못함, 라벨 전략 시도...")
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
                self.logger.error("입찰 공고 번호 검색창을 찾을 수 없음")
                return False
                
            # 3. Click Search
            # 3. 검색 버튼 클릭
            search_btn = page.locator('#mf_wfm_container_btnS0001')
            search_btn.click()
            
            # Wait for grid to reload
            # 그리드 로드 대기
            time.sleep(2)
            page.wait_for_selector('#mf_wfm_container_grdBidPbancList_body_table, .w2grid_body_table', timeout=10000)
            
            # 4. Parse Result
            # 4. 결과 파싱
            # Should have 1 row
            # 1개의 행이 있어야 함
            notices_data = self.list_parser.parse_page(page)
            
            if not notices_data:
                self.logger.warning(f"{bid_no}에 대한 결과 없음")
                return False
                
            # Find the matching item (search might return partial matches?)
            # 일치하는 항목 찾기 (검색 결과에 부분 일치가 포함될 수 있음?)
            target_notice = None
            for notice in notices_data:
                if bid_no in notice.get('bid_notice_number', ''):
                    target_notice = notice
                    break
            
            if not target_notice:
                self.logger.warning(f"검색 결과에 {bid_no}가 포함되어 있지 않음")
                return False
                
            # 5. Process
            # 5. 처리
            self.logger.info(f"{bid_no} 발견, 상세 정보 처리 중...")
            self.processor.process_notice(page, target_notice, 1) # page 1 context (페이지 1 컨텍스트)
            
            return True
            
        except Exception as e:
            self.logger.error(f"{bid_no} 검색 및 처리 중 오류 발생: {e}")
            return False
