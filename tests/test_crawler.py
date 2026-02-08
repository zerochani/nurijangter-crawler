import pytest
from unittest.mock import MagicMock, Mock, patch
from src.crawler.engine import CrawlerEngine
from src.models import BidNotice

@pytest.fixture
def mock_config():
    return {
        'crawler': {
            'wait': {'navigation_timeout': 1000},
            'retry': {'max_attempts': 1},
            'pagination': {'max_pages': 2},
            'rate_limit': {'requests_per_minute': 60}
        },
        'website': {
            'base_url': 'http://test.com',
            'list_page_url': 'http://test.com/list'
        },
        'checkpoint': {'enabled': False},
        'deduplication': {'enabled': False},
        'storage': {'output_dir': '/tmp/test_data'}
    }

@pytest.fixture
def mock_managers():
    with patch('src.crawler.engine.BrowserManager') as mock_browser_cls, \
         patch('src.crawler.engine.CheckpointManager') as mock_checkpoint_cls, \
         patch('src.crawler.engine.DeduplicationManager') as mock_dedup_cls, \
         patch('src.crawler.engine.JSONStorage') as mock_storage_cls, \
         patch('src.crawler.engine.ListPageParser') as mock_list_parser_cls, \
         patch('src.crawler.engine.DetailPageParser') as mock_detail_parser_cls, \
         patch('src.crawler.engine.CrawlerLogger') as mock_logger_cls:
        
        yield {
            'browser_cls': mock_browser_cls,
            'checkpoint_cls': mock_checkpoint_cls,
            'dedup_cls': mock_dedup_cls,
            'storage_cls': mock_storage_cls,
            'list_parser_cls': mock_list_parser_cls,
            'detail_parser_cls': mock_detail_parser_cls,
            'logger_cls': mock_logger_cls
        }

def test_crawler_initialization(mock_config, mock_managers):
    """Test that crawler initializes all components correctly."""
    crawler = CrawlerEngine(mock_config)
    assert crawler.config == mock_config
    assert mock_managers['browser_cls'].called
    assert mock_managers['checkpoint_cls'].called
    assert mock_managers['dedup_cls'].called

def test_crawler_run_flow(mock_config, mock_managers):
    """Test the main execution flow of the crawler."""
    # Setup mocks
    mock_browser = mock_managers['browser_cls'].return_value
    mock_page = MagicMock()
    mock_browser.__enter__.return_value.get_page.return_value = mock_page
    
    crawler = CrawlerEngine(mock_config)
    
    # Mock list parser to return 1 item on first page, then nothing
    crawler.list_parser.parse_page.side_effect = [
        [{'bid_notice_number': '123'}], # Page 1
        [] # Page 2 (Stop)
    ]
    crawler.list_parser.has_next_page.return_value = True
    crawler.list_parser.go_to_next_page.return_value = True
    
    # Mock deduplication to say not duplicate
    crawler.dedup_manager.is_duplicate.return_value = False
    
    # Mock checkpoint to report success
    crawler.checkpoint_manager.load_checkpoint.return_value = False
    crawler.checkpoint_manager.is_item_processed.return_value = False
    crawler.checkpoint_manager.current_page = 1
    
    # Run crawler
    results = crawler.run()
    
    # Verification
    assert len(results.notices) == 1
    assert results.notices[0].bid_notice_number == '123'
    
    # Verify method calls
    crawler.browser_manager.__enter__.assert_called() # Context manager used
    mock_page.goto.assert_called() # Navigation happened
    assert crawler.list_parser.parse_page.call_count >= 1 # Parsing happened

def test_crawler_deduplication(mock_config, mock_managers):
    """Test that duplicates are skipped."""
    crawler = CrawlerEngine(mock_config)
    
    # Setup list parser
    crawler.list_parser.parse_page.return_value = [
        {'bid_notice_number': '123'}, 
        {'bid_notice_number': '456'}
    ]
    
    # Setup duplicates
    # First item is duplicate, second is new
    crawler.dedup_manager.is_duplicate.side_effect = [True, False]
    crawler.checkpoint_manager.is_item_processed.return_value = False
    crawler.checkpoint_manager.current_page = 1
    
    # Run processing manually for a page
    mock_page = MagicMock()
    crawler._crawl_list_pages(mock_page)
    
    # Only 1 item should be collected (456)
    assert len(crawler.collected_notices.notices) == 1
    assert crawler.collected_notices.notices[0].bid_notice_number == '456'
    assert crawler.stats['items_skipped'] == 1

def test_crawler_detail_fetch(mock_config, mock_managers):
    """Test fetching details for an item."""
    crawler = CrawlerEngine(mock_config)
    
    # Mock data
    base_data = {
        'bid_notice_number': '123',
        'has_detail': True
    }
    
    # Mock detail fetching
    crawler._fetch_detail_page = Mock(return_value={
        'bid_notice_number': '123',
        'bid_notice_name': 'Detail Name', # Enriched data
        'announcement_agency': 'Agency'
    })
    
    crawler.checkpoint_manager.is_item_processed.return_value = False
    crawler.dedup_manager.is_duplicate.return_value = False
    crawler.checkpoint_manager.current_page = 1
    
    # Process
    mock_page = MagicMock()
    crawler._process_notice(mock_page, base_data)
    
    # Verify
    # We loosen the restriction on arguments or verify specifically
    crawler._fetch_detail_page.assert_called()
    assert crawler.collected_notices.notices[0].bid_notice_name == 'Detail Name'

def test_crawler_error_handling(mock_config, mock_managers):
    """Test that individual item failure doesn't crash the crawler."""
    crawler = CrawlerEngine(mock_config)
    
    # List has 2 items
    # List has 2 items on first page, then empty
    crawler.list_parser.parse_page.side_effect = [
        [
            {'bid_notice_number': '1', 'has_detail': True},
            {'bid_notice_number': '2', 'has_detail': True}
        ],
        []
    ]
    crawler.list_parser.has_next_page.side_effect = [True, False]
    crawler.list_parser.go_to_next_page.return_value = True
    
    # Manually replace the method with a Mock to support side_effect
    crawler._fetch_detail_page = Mock(side_effect=[
        Exception("Network Error"),
        {'bid_notice_number': '2', 'announcement_agency': 'A'}
    ])
    
    crawler.checkpoint_manager.is_item_processed.return_value = False
    crawler.dedup_manager.is_duplicate.return_value = False
    crawler.checkpoint_manager.current_page = 1
    
    # Run
    mock_page = MagicMock()
    crawler._crawl_list_pages(mock_page)
    
    # Verify
    assert len(crawler.collected_notices.notices) == 1 # Only item 2 succeeded
    assert crawler.stats['errors'] == 1 # 1 error recorded
    crawler.checkpoint_manager.mark_item_failed.assert_called() # Failed item marked
