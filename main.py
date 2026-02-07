#!/usr/bin/env python3
"""
NuriJangter Crawler - Main Entry Point

Production-grade web crawler for collecting bid notice data from NuriJangter.

Usage:
    python main.py [options]

Options:
    --config PATH       Path to configuration file (default: config/config.yaml)
    --resume            Resume from last checkpoint
    --no-resume         Start fresh, ignore checkpoints
    --scheduled         Run in scheduled mode
    --clear-checkpoint  Clear checkpoint before starting
    --output-dir PATH   Override output directory
    --log-level LEVEL   Set log level (DEBUG, INFO, WARNING, ERROR)
    --help              Show this help message
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
    Load configuration from YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='NuriJangter Crawler - Collect bid notice data from NuriJangter',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--config',
        type=Path,
        default=Path('config/config.yaml'),
        help='Path to configuration file (default: config/config.yaml)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        default=True,
        help='Resume from last checkpoint (default: True)'
    )

    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh, ignore checkpoints'
    )

    parser.add_argument(
        '--scheduled',
        action='store_true',
        help='Run in scheduled mode (uses scheduler configuration)'
    )

    parser.add_argument(
        '--clear-checkpoint',
        action='store_true',
        help='Clear checkpoint before starting'
    )

    parser.add_argument(
        '--output-dir',
        type=Path,
        help='Override output directory'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set log level'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode - parse config and show settings without crawling'
    )

    return parser.parse_args()


def setup_directories(config: dict) -> None:
    """
    Ensure required directories exist.

    Args:
        config: Configuration dictionary
    """
    # Create output directory
    output_dir = Path(config.get('storage', {}).get('output_dir', 'data'))
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create logs directory
    log_dir = Path(config.get('logging', {}).get('file', {}).get('directory', 'logs'))
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create checkpoint directory
    checkpoint_dir = Path(config.get('checkpoint', {}).get('directory', 'checkpoints'))
    checkpoint_dir.mkdir(parents=True, exist_ok=True)


def run_crawler(config: dict, resume: bool = True):
    """
    Run the crawler once.

    Args:
        config: Configuration dictionary
        resume: Whether to resume from checkpoint

    Returns:
        BidNoticeList with collected data
    """
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("Starting NuriJangter Crawler")
    logger.info("=" * 70)

    # Create and run crawler
    
    # Smart Resume Logic
    # If resume is requested, check if the previous run was COMPLETED.
    # If COMPLETED -> Start Fresh (Page 1) to get new data
    # If PAUSED/IN_PROGRESS -> Resume (Continue history collection)
    if resume:
        from src.checkpoint import CheckpointManager, CrawlState
        checkpoint_config = config.get('checkpoint', {})
        cm = CheckpointManager(
            checkpoint_dir=Path(checkpoint_config.get('directory', 'checkpoints')),
            checkpoint_file=checkpoint_config.get('filename', 'crawler_checkpoint.json')
        )
        if cm.load_checkpoint():
            if cm.state == CrawlState.COMPLETED:
                logger.info("Previous crawl was COMPLETED. Starting FRESH to catch new data (Smart Resume).")
                resume = False
            else:
                logger.info(f"Previous crawl state: {cm.state.value}. Resuming...")
        else:
            logger.info("No valid checkpoint found. Starting fresh.")

    crawler = CrawlerEngine(config)

    try:
        result = crawler.run(resume=resume)

        # Print statistics
        stats = crawler.get_statistics()
        logger.info("=" * 70)
        logger.info("Crawl Statistics:")
        logger.info(f"  Pages crawled: {stats['pages_crawled']}")
        logger.info(f"  Items extracted: {stats['items_extracted']}")
        logger.info(f"  Items skipped: {stats['items_skipped']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info(f"  Total collected: {stats['total_collected']}")
        logger.info("=" * 70)

        return result

    except KeyboardInterrupt:
        logger.warning("Crawler interrupted by user")
        logger.info("Progress has been saved. Use --resume to continue.")
        sys.exit(130)

    except Exception as e:
        logger.error(f"Crawler failed: {e}", exc_info=True)
        sys.exit(1)


def run_scheduled(config: dict, resume: bool = True):
    """
    Run the crawler in scheduled mode.

    Args:
        config: Configuration dictionary
        resume: Whether to resume from checkpoint
    """
    logger = logging.getLogger(__name__)

    logger.info("Starting crawler in scheduled mode")

    # Create crawler function
    def crawler_func():
        return run_crawler(config, resume=resume)

    # Create and start scheduler
    scheduler_config = config.get('scheduler', {})
    scheduler = CronScheduler(scheduler_config)

    try:
        scheduler.start(crawler_func)
    except KeyboardInterrupt:
        logger.info("Scheduler interrupted by user")
        scheduler.stop()
    except Exception as e:
        logger.error(f"Scheduler error: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Main entry point."""
    # Parse arguments
    args = parse_arguments()

    # Load configuration
    if not args.config.exists():
        print(f"Error: Configuration file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    config = load_config(args.config)

    # Override configuration with command line arguments
    if args.output_dir:
        config.setdefault('storage', {})['output_dir'] = str(args.output_dir)

    if args.log_level:
        config.setdefault('logging', {})['level'] = args.log_level

    if args.scheduled:
        config.setdefault('scheduler', {})['enabled'] = True

    # Setup directories
    setup_directories(config)

    # Setup logging
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
    logger.info(f"Configuration loaded from: {args.config}")
    logger.info(f"Output directory: {config.get('storage', {}).get('output_dir', 'data')}")

    # Dry run mode
    if args.dry_run:
        logger.info("=" * 70)
        logger.info("DRY RUN MODE - Configuration loaded successfully")
        logger.info("=" * 70)
        logger.info(f"Target URL: {config.get('website', {}).get('list_page_url', 'N/A')}")
        logger.info(f"Resume enabled: {not args.no_resume}")
        logger.info(f"Scheduled mode: {args.scheduled}")
        logger.info(f"Output formats: {config.get('storage', {}).get('formats', [])}")
        logger.info(f"Deduplication: {config.get('deduplication', {}).get('enabled', False)}")
        logger.info(f"Checkpoint enabled: {config.get('checkpoint', {}).get('enabled', False)}")
        logger.info("=" * 70)
        return

    # Clear checkpoint if requested
    if args.clear_checkpoint:
        from src.checkpoint import CheckpointManager
        checkpoint_config = config.get('checkpoint', {})
        checkpoint_manager = CheckpointManager(
            checkpoint_dir=Path(checkpoint_config.get('directory', 'checkpoints')),
            checkpoint_file=checkpoint_config.get('filename', 'crawler_checkpoint.json')
        )
        checkpoint_manager.clear_checkpoint()
        logger.info("Checkpoint cleared")

    # Determine resume mode
    resume = not args.no_resume

    # Run crawler
    if args.scheduled or config.get('scheduler', {}).get('enabled', False):
        run_scheduled(config, resume=resume)
    else:
        run_crawler(config, resume=resume)


if __name__ == '__main__':
    main()
