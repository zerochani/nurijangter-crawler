#!/usr/bin/env python3
"""
누리장터 크롤러 - 메인 진입점 (Main Entry Point)

누리장터의 입찰 공고 데이터를 수집하기 위한 프로덕션 등급의 웹 크롤러입니다.

사용법:
    python main.py [options]

옵션:
    --config PATH       설정 파일 경로 (기본값: config/config.yaml)
    --resume            마지막 체크포인트에서 재개 (기본값)
    --no-resume         처음부터 다시 시작 (체크포인트 무시)
    --scheduled         스케줄러 모드로 실행
    --clear-checkpoint  시작 전 체크포인트 초기화
    --output-dir PATH   출력 디렉토리 재정의
    --log-level LEVEL   로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR)
    --help              이 도움말 메시지 표시
"""

import sys
import sys
print("Starting crawler...", file=sys.stderr)
import argparse
from pathlib import Path
import yaml
import logging
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.crawler import CrawlerEngine
from src.utils import setup_logger
from src.scheduler import CronScheduler


def load_config(config_path: Path) -> dict:
    """
    YAML 파일에서 설정을 로드합니다.

    Args:
        config_path: 설정 파일 경로

    Returns:
        설정 딕셔너리
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)


def parse_arguments():
    """커맨드 라인 인자를 파싱합니다."""
    parser = argparse.ArgumentParser(
        description='누리장터 크롤러 - 누리장터 입찰 공고 데이터 수집',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--config',
        type=Path,
        default=Path('config/config.yaml'),
        help='설정 파일 경로 (기본값: config/config.yaml)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        default=False,
        help='마지막 체크포인트에서 재개 (기본값: False)'
    )

    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='(deprecated) 처음부터 다시 시작 (이제 기본값이므로 불필요)'
    )

    parser.add_argument(
        '--scheduled',
        action='store_true',
        help='스케줄러 모드로 실행 (스케줄러 설정 사용)'
    )

    parser.add_argument(
        '--clear-checkpoint',
        action='store_true',
        help='시작 전 체크포인트 초기화'
    )

    parser.add_argument(
        '--output-dir',
        type=Path,
        help='출력 디렉토리 재정의'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='로그 레벨 설정'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='드라이 런 모드 - 크롤링 없이 설정 파싱 및 설정값 표시'
    )

    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='체크포인트의 실패한 항목 재시도'
    )

    parser.add_argument(
        '--pages',
        type=str,
        help='특정 페이지 크롤링 (예: "5", "1-10", "5-")'
    )

    return parser.parse_args()


def setup_directories(config: dict) -> None:
    """
    필요한 디렉토리가 존재하는지 확인하고 생성합니다.

    Args:
        config: 설정 딕셔너리
    """
    # Create output directory
    # 출력 디렉토리 생성
    output_dir = Path(config.get('storage', {}).get('output_dir', 'data'))
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create logs directory
    # 로그 디렉토리 생성
    log_dir = Path(config.get('logging', {}).get('file', {}).get('directory', 'logs'))
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create checkpoint directory
    # 체크포인트 디렉토리 생성
    checkpoint_dir = Path(config.get('checkpoint', {}).get('directory', 'checkpoints'))
    checkpoint_dir.mkdir(parents=True, exist_ok=True)


def run_crawler(config: dict, resume: bool = True):
    """
    크롤러를 1회 실행합니다.

    Args:
        config: 설정 딕셔너리
        resume: 체크포인트에서 재개할지 여부

    Returns:
        수집된 데이터를 담은 BidNoticeList
    """
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("누리장터 크롤러 시작")
    logger.info("=" * 70)

    # Create and run crawler
    # 크롤러 생성 및 실행
    
    # Smart Resume Logic (스마트 재개 로직)
    # resume이 요청되었을 때, 이전 실행이 '완료(COMPLETED)' 상태라면
    # 처음부터 다시 시작(Start Fresh)하여 새로운 데이터를 수집합니다.
    # 이전 실행이 '일시중지' 또는 '진행중'이라면 이어서 진행(Resume)합니다.
    if resume:
        from src.checkpoint import CheckpointManager, CrawlState
        checkpoint_config = config.get('checkpoint', {})
        cm = CheckpointManager(
            checkpoint_dir=Path(checkpoint_config.get('directory', 'checkpoints')),
            checkpoint_file=checkpoint_config.get('filename', 'crawler_checkpoint.json')
        )
        if cm.load_checkpoint():
            logger.info(f"이전 크롤링 상태: {cm.state.value}. 체크포인트에서 재개합니다 (Page {cm.current_page})")
        else:
            logger.info("유효한 체크포인트가 없습니다. 처음부터 시작합니다.")

    crawler = CrawlerEngine(config)

    try:
        result = crawler.run(resume=resume)

        # Print statistics
        # 통계 출력
        stats = crawler.get_statistics()
        logger.info("=" * 70)
        logger.info("크롤링 통계:")
        logger.info(f"  크롤링한 페이지: {stats['pages_crawled']}")
        logger.info(f"  추출한 항목: {stats['items_extracted']}")
        logger.info(f"  건너뛴 항목: {stats['items_skipped']}")
        logger.info(f"  에러: {stats['errors']}")
        logger.info(f"  총 수집 개수: {stats['total_collected']}")
        logger.info("=" * 70)

        return result

    except KeyboardInterrupt:
        logger.warning("사용자에 의해 크롤러가 중단되었습니다.")
        logger.info("진행 상황이 저장되었습니다. --resume 옵션으로 이어서 진행할 수 있습니다.")
        sys.exit(130)

    except Exception as e:
        logger.error(f"Crawler failed: {e}", exc_info=True)
        sys.exit(1)


def run_scheduled(config: dict, resume: bool = True):
    """
    스케줄러 모드로 크롤러를 실행합니다.

    Args:
        config: 설정 딕셔너리
        resume: 체크포인트에서 재개할지 여부
    """
    logger = logging.getLogger(__name__)

    logger.info("스케줄러 모드로 크롤러를 시작합니다.")

    # Create crawler function
    # 크롤러 실행 함수 생성
    def crawler_func():
        return run_crawler(config, resume=resume)

    # Create and start scheduler
    # 스케줄러 생성 및 시작
    scheduler_config = config.get('scheduler', {})
    scheduler = CronScheduler(scheduler_config)

    try:
        scheduler.start(crawler_func)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 스케줄러가 중단되었습니다.")
        scheduler.stop()
    except Exception as e:
        logger.error(f"스케줄러 에러: {e}", exc_info=True)
        sys.exit(1)


def main():
    """메인 진입점 함수입니다."""
    # Parse arguments
    # 인자 파싱
    args = parse_arguments()

    # Load configuration
    # 설정 로드
    if not args.config.exists():
        print(f"에러: 설정 파일을 찾을 수 없습니다: {args.config}", file=sys.stderr)
        sys.exit(1)

    config = load_config(args.config)

    # Override configuration with command line arguments
    # 커맨드 라인 인자로 설정 덮어쓰기
    if args.output_dir:
        config.setdefault('storage', {})['output_dir'] = str(args.output_dir)

    if args.log_level:
        config.setdefault('logging', {})['level'] = args.log_level

    if args.scheduled:
        config.setdefault('scheduler', {})['enabled'] = True

    if args.pages:
        config.setdefault('crawler', {}).setdefault('pagination', {})['pages'] = args.pages

    # Setup directories
    # 디렉토리 설정
    setup_directories(config)

    # Setup logging
    # 로깅 설정
    log_config_path = Path('config/logging.yaml')
    if log_config_path.exists():
        setup_logger(
            config_path=log_config_path,
            log_level=config.get('logging', {}).get('level', 'INFO'),
            log_dir=Path(config.get('logging', {}).get('file', {}).get('directory', 'logs'))
        )
    else:
        setup_logger(
            log_level=config.get('logging', {}).get('level', 'INFO'),
            log_dir=Path(config.get('logging', {}).get('file', {}).get('directory', 'logs'))
        )

    logger = logging.getLogger(__name__)

    # Print configuration info
    # 설정 정보 출력
    logger.info(f"설정 로드됨: {args.config}")
    logger.info(f"출력 디렉토리: {config.get('storage', {}).get('output_dir', 'data')}")

    # Dry run mode
    # 드라이 런 모드
    if args.dry_run:
        logger.info("=" * 70)
        logger.info("DRY RUN MODE - 설정이 성공적으로 로드되었습니다")
        logger.info("=" * 70)
        logger.info(f"대상 URL: {config.get('website', {}).get('list_page_url', 'N/A')}")
        logger.info(f"재개(Resume) 활성화: {not args.no_resume}")
        logger.info(f"스케줄 모드: {args.scheduled}")
        logger.info(f"출력 형식: {config.get('storage', {}).get('formats', [])}")
        logger.info(f"중복 제거: {config.get('deduplication', {}).get('enabled', False)}")
        logger.info(f"체크포인트 활성화: {config.get('checkpoint', {}).get('enabled', False)}")
        logger.info("=" * 70)
        return

    # Clear checkpoint if requested
    # 요청 시 체크포인트 초기화
    if args.clear_checkpoint:
        from src.checkpoint import CheckpointManager
        checkpoint_config = config.get('checkpoint', {})
        checkpoint_manager = CheckpointManager(
            checkpoint_dir=Path(checkpoint_config.get('directory', 'checkpoints')),
            checkpoint_file=checkpoint_config.get('filename', 'crawler_checkpoint.json')
        )
        checkpoint_manager.clear_checkpoint()
        logger.info("체크포인트가 초기화되었습니다")

    # Determine resume mode
    # 재개 모드 결정
    resume = args.resume

    # Run crawler
    # 크롤러 실행
    if args.retry_failed:
        logger.info("체크포인트에서 실패한 항목들을 재시도합니다...")
        # Force resume=True to load the checkpoint containing failed items
        crawler = CrawlerEngine(config)
        crawler.retry_failed_items()
    elif args.scheduled or config.get('scheduler', {}).get('enabled', False):
        run_scheduled(config, resume=resume)
    else:
        run_crawler(config, resume=resume)


if __name__ == '__main__':
    main()
