
"""
누리장터 메인 크롤러 엔진 (Main Crawler Engine for NuriJangter)

이 모듈은 페이지 탐색, 데이터 추출, 저장, 체크포인트 관리, 에러 처리 등
크롤링의 전체 프로세스를 조정(Orchestrate)합니다.
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
    전체 크롤링 프로세스를 조정하는 메인 크롤러 엔진입니다.

    다음 기능들을 관리합니다:
    - 브라우저 관리 (Browser Management)
    - 컴포넌트 조정 (Navigator, Processor, RetryManager)
    - 중복 제거 (Deduplication)
    - 체크포인트 관리 (Checkpoint Management)
    - 에러 처리 및 재시도 로직
    - 속도 제한 (Rate Limiting)
    - 데이터 저장 (Data Storage)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        크롤러 엔진을 초기화합니다.

        Args:
            config: 설정 딕셔너리
        """
        self.config = config
        self.logger = CrawlerLogger(__name__)

        # Initialize base components
        # 기본 컴포넌트 초기화
        self.browser_manager = BrowserManager(config.get('crawler', {}))
        self.list_parser = ListPageParser(config)
        self.detail_parser = DetailPageParser(config)

        # Initialize checkpoint manager
        # 체크포인트 매니저 초기화
        checkpoint_config = config.get('checkpoint', {})
        self.checkpoint_manager = CheckpointManager(
            checkpoint_dir=Path(checkpoint_config.get('directory', 'checkpoints')),
            checkpoint_file=checkpoint_config.get('filename', 'crawler_checkpoint.json'),
            save_interval=checkpoint_config.get('save_interval', 10)
        )

        # Initialize deduplication manager
        # 중복 제거 매니저 초기화
        dedup_config = config.get('deduplication', {})
        self.dedup_manager = DeduplicationManager(
            key_fields=dedup_config.get('key_fields', ['bid_notice_number']),
            storage_file=Path(dedup_config.get('storage_file', 'checkpoints/seen_items.json')),
            enabled=dedup_config.get('enabled', True)
        )

        # Initialize storage
        # 저장소 초기화
        storage_config = config.get('storage', {})
        output_dir = Path(storage_config.get('output_dir', 'data'))

        self.storages = []
        for format_type in storage_config.get('formats', ['json', 'csv']):
            if format_type == 'json':
                self.storages.append(JSONStorage(output_dir, storage_config.get('json', {})))
            elif format_type == 'csv':
                self.storages.append(CSVStorage(output_dir, storage_config.get('csv', {})))

        # Crawler settings
        # 크롤러 설정
        crawler_config = config.get('crawler', {})
        self.pagination_config = crawler_config.get('pagination', {})
        
        # Collected data
        # 수집된 데이터
        self.collected_notices = BidNoticeList()

        # Statistics
        # 통계
        self.stats = {
            'pages_crawled': 0,
            'items_extracted': 0,
            'items_skipped': 0,
            'errors': 0
        }

        # --- Refactored Components ---
        # --- 리팩토링된 컴포넌트 ---
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
        크롤러를 실행합니다.

        Args:
            resume: 이용 가능한 경우 체크포인트에서 재개할지 여부

        Returns:
            수집된 데이터를 포함한 BidNoticeList
        """
        start_time = time.time()
        self.logger.log_crawl_start(self.config.get('website', {}).get('base_url', 'NuriJangter'))

        try:
            # Parse pages argument if present
            # 페이지 인자가 있는지 확인
            pages_arg = self.pagination_config.get('pages')
            start_page = 1
            end_page = None
            
            if pages_arg:
                try:
                    if '-' in pages_arg:
                        start_str, end_str = pages_arg.split('-')
                        start_page = int(start_str) if start_str else 1
                        end_page = int(end_str) if end_str else None
                    else:
                        start_page = int(pages_arg)
                        end_page = start_page
                    
                    self.logger.info(f"특정 페이지 타게팅: 시작={start_page}, 종료={end_page}")
                    
                    # Override checkpoint if specific pages requested
                    # 특정 페이지가 요청된 경우, 체크포인트를 오버라이드합니다.
                    self.checkpoint_manager.current_page = start_page
                    
                except ValueError:
                    self.logger.error(f"잘못된 페이지 인자: {pages_arg}")
                    raise

            # Load checkpoint if resuming (and not overridden by specific pages)
            # 재개 모드이고 특정 페이지가 지정되지 않은 경우 체크포인트 로드
            if resume and not pages_arg and self.checkpoint_manager.load_checkpoint():
                self.logger.log_checkpoint_load(
                    f"페이지 {self.checkpoint_manager.current_page}"
                )
            elif not pages_arg:
                self.checkpoint_manager.initialize_crawl({
                    'target': 'NuriJangter',
                    'config': self.config.get('website', {})
                })
            else:
                # Initialize for specific pages
                # 특정 페이지 모드로 초기화
                 self.checkpoint_manager.initialize_crawl({
                    'target': 'NuriJangter',
                    'config': self.config.get('website', {}),
                    'pages_mode': True
                })
                 self.checkpoint_manager.current_page = start_page

            # Start browser
            # 브라우저 시작
            with self.browser_manager as browser:
                page = browser.get_page()

                # Navigate to list page
                # 목록 페이지로 이동
                list_url = self.config.get('website', {}).get('list_page_url', '')
                if not list_url:
                    raise ValueError("목록 페이지 URL이 설정되지 않았습니다")

                self.navigator.navigate_to_page(page, list_url)

                # Jump to start page if needed
                # 필요한 경우 시작 페이지로 점프
                if self.checkpoint_manager.current_page > 1:
                     try:
                        self.navigator.restore_pagination(page, self.checkpoint_manager.current_page)
                     except Exception as e:
                        self.logger.error(f"치명적 오류: 시작 페이지 {self.checkpoint_manager.current_page}로 점프 실패: {e}")
                        raise # 크롤링 중단

                # Crawl pages
                # 페이지 크롤링 시작
                self._crawl_list_pages(page, end_page=end_page)

            # Mark crawl as complete
            # 크롤링 완료 처리
            self.checkpoint_manager.complete_crawl(success=True)

            # Save collected data
            # 수집된 데이터 저장
            self._save_data()

            # Save deduplication data
            # 중복 제거 데이터 저장
            self.dedup_manager.save()

            # Calculate duration
            # 소요 시간 계산
            duration = time.time() - start_time
            self.logger.log_crawl_complete(
                total_items=len(self.collected_notices.notices),
                duration=duration
            )

            return self.collected_notices

        except KeyboardInterrupt:
            self.logger.warning("사용자에 의해 크롤링이 중단되었습니다")
            self.checkpoint_manager.set_state(CrawlState.PAUSED)
            self.checkpoint_manager.save_checkpoint(force=True)
            self.dedup_manager.save()
            raise

        except Exception as e:
            self.logger.log_error(e, "크롤링 실패")
            self.stats['errors'] += 1
            self.checkpoint_manager.complete_crawl(success=False)
            raise

    def retry_failed_items(self) -> None:
        """
        이전 크롤링에서 실패한 항목들을 재시도합니다.
        """
        # Start browser management here, delegate retry logic to manager
        # 브라우저 관리를 시작하고, 재시도 로직을 매니저에게 위임
        try:
            with self.browser_manager as browser:
                page = browser.get_page()
                self.retry_manager.process_retries(page)
        except Exception as e:
            self.logger.error(f"재시도 프로세스 실패: {e}")


    def _crawl_list_pages(self, page, end_page: Optional[int] = None) -> None:
        """
        모든 목록 페이지를 크롤링합니다.

        Args:
            page: Playwright 페이지 객체
            end_page: 선택적 종료 페이지 번호
        """
        max_pages = self.pagination_config.get('max_pages', 0)
        current_page_num = self.checkpoint_manager.current_page

        while True:
            try:
                # Check for specific end page
                # 지정된 종료 페이지 확인
                if end_page is not None and current_page_num > end_page:
                    self.logger.info(f"목표 종료 페이지 도달: {end_page}")
                    break

                # Check if we've reached max pages
                # 최대 페이지 제한 확인
                if max_pages > 0 and current_page_num > max_pages:
                    self.logger.info(f"최대 페이지 제한 도달: {max_pages}")
                    break

                self.logger.info(f"페이지 크롤링 중: {current_page_num}")

                # Wait for page to load
                # 페이지 로드 대기
                self.navigator.wait_for_page_load(page)

                # Extract notices from list page
                # 목록 페이지에서 공고 추출
                notices_data = self.list_parser.parse_page(page)

                self.logger.log_data_extracted("입찰 공고", len(notices_data))
                self.stats['pages_crawled'] += 1

                # Process each notice
                # 각 공고 처리
                for idx, notice_data in enumerate(notices_data):
                    self.processor.process_notice(page, notice_data, current_page_num)
                    
                    # Check for early exit (Accessed via processor state if needed, or moved to processor)
                    # 조기 종료 확인
                    if self.processor.consecutive_duplicates >= self.processor.early_exit_threshold:
                        self.logger.info(f"조기 종료 트리거: {self.processor.consecutive_duplicates} 연속 중복 발견.")
                        return  # Exit function completely

                # Check for next page
                # 다음 페이지 확인
                if self.list_parser.has_next_page(page):
                    # Navigate to next page
                    # 다음 페이지로 이동
                    if self.list_parser.go_to_next_page(page):
                        current_page_num += 1
                        self.checkpoint_manager.current_page = current_page_num
                        self.checkpoint_manager.advance_page()

                        # Rate limiting
                        # 속도 제한 적용
                        self.navigator.rate_limit()
                    else:
                        self.logger.warning("다음 페이지 이동 실패")
                        break
                else:
                    self.logger.info("크롤링할 페이지가 더 이상 없습니다")
                    break

            except Exception as e:
                self.logger.error(f"페이지 {current_page_num} 크롤링 에러: {e}")
                self.stats['errors'] += 1

                # Decide whether to continue or abort
                # 계속할지 중단할지 결정
                if self.stats['errors'] > 10:
                    self.logger.error("에러가 너무 많이 발생하여 크롤링을 중단합니다")
                    break

                # Try to continue with next page
                # 다음 페이지로 계속 시도
                current_page_num += 1
                continue

    def _save_data(self) -> None:
        """수집된 데이터를 저장소에 저장합니다."""
        if not self.collected_notices.notices:
            self.logger.warning("저장할 데이터가 없습니다")
            return

        # Sort notices by announcement_date (desc) then bid_notice_number (desc)
        # Using a stable sort sequence (primary key last)
        # 공고일시(내림차순) 그 다음 공고번호(내림차순) 정렬
        self.collected_notices.notices.sort(
            key=lambda x: (x.announcement_date or "", x.bid_notice_number), 
            reverse=True
        )

        # Convert to dictionaries
        # 딕셔너리로 변환
        data = [notice.to_dict() for notice in self.collected_notices.notices]

        # Save using all configured storages
        # 설정된 모든 저장소에 저장
        for storage in self.storages:
            try:
                file_path = storage.save(data)
                self.logger.info(f"데이터가 저장되었습니다: {file_path}")
            except Exception as e:
                self.logger.error(f"{storage.__class__.__name__} 저장 실패: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        크롤링 통계를 반환합니다.

        Returns:
            통계 정보를 담은 딕셔너리
        """
        return {
            **self.stats,
            'total_collected': len(self.collected_notices.notices),
            'checkpoint_info': self.checkpoint_manager.get_resume_info(),
            'dedup_info': self.dedup_manager.get_stats()
        }
