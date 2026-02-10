"""
Unit tests for parsers.
"""

import pytest
from unittest.mock import MagicMock, Mock, PropertyMock
from src.parser import ListPageParser, DetailPageParser

# --- Fixtures ---

@pytest.fixture
def mock_config():
    return {
        'extraction': {
            'list_fields': ['bid_notice_number', 'bid_notice_name'],
            'detail_fields': ['budget_amount']
        }
    }

@pytest.fixture
def mock_page():
    return MagicMock()

# --- ListPageParser Tests ---

def test_list_parser_parse_row(mock_config, mock_page):
    """Test parsing a single row in the list page."""
    parser = ListPageParser(mock_config)
    
    # Mock row element
    mock_row = MagicMock()
    
    # Helper to create mock cell
    def create_mock_cell(text):
        cell = MagicMock()
        cell.first = cell # Chainable .first
        cell.count.return_value = 1
        cell.inner_text.return_value = text
        return cell
        
    # Configure mock row to return specific cells based on selector
    def locator_side_effect(selector):
        if 'bidPbancNum' in selector:
            return create_mock_cell("20240101-00")
        elif 'bidPbancNm' in selector:
            return create_mock_cell("Test Notice")
        elif 'grpNm' in selector:
            return create_mock_cell("Test Agency")
        elif 'pbancPstgDt' in selector:
            return create_mock_cell("2024-01-01")
        elif 'slprRcptDdlnDt' in selector:
            return create_mock_cell("2024-01-10")
        return MagicMock()
        
    mock_row.locator.side_effect = locator_side_effect
    
    # Test _parse_row directly
    result = parser._parse_row(mock_row, mock_page)
    
    assert result is not None
    assert result['bid_notice_number'] == "20240101-00"
    assert result['bid_notice_name'] == "Test Notice"
    assert result['announcement_agency'] == "Test Agency"
    assert result['has_detail'] is True

def test_list_parser_parse_page_empty(mock_config, mock_page):
    """Test parsing an empty page."""
    parser = ListPageParser(mock_config)
    
    # Mock no rows
    mock_page.locator.return_value.all.return_value = []
    
    results = parser.parse_page(mock_page)
    assert results == []

# --- DetailPageParser Tests ---

def test_detail_parser_strategy_1_xpath(mock_config, mock_page):
    """Test Strategy 1: TH with following TD (XPath-like extraction)."""
    parser = DetailPageParser(mock_config)
    base_data = {'bid_notice_number': '123'}
    
    # Mock context to return frame/element
    # We mock _find_detail_context to return our mock object
    mock_context = MagicMock()
    parser._find_detail_context = Mock(return_value=mock_context)
    
    # Mock TH elements
    mock_th1 = MagicMock()
    mock_th1.inner_text.return_value = "배정예산"
    # evaluate returns the TD text
    mock_th1.evaluate.side_effect = [
        False, # is_search_filter check
        "1,000,000원" # td_text extraction
    ]
    
    mock_th2 = MagicMock()
    mock_th2.inner_text.return_value = "공고일시"
    mock_th2.evaluate.side_effect = [
        False, 
        "2024-01-01"
    ]
    
    mock_context.query_selector_all.side_effect = [
        [mock_th1, mock_th2], # First call for THs (Strategy 1)
        [] # Second call for Tables (Strategy 2)
    ]
    
    # Run
    result = parser.parse_page(mock_page, base_data)
    
    assert result['budget_amount'] == "1,000,000원"
    # announcement_date isn't in default mapping if it comes from base_data, 
    # but here it's extracted fresh. Let's check mapping.
    # '공고일시' maps to 'announcement_date' in _map_to_schema? 
    # Let's check schema mapping in code... 'announcement_date': ['게시일시'] is expected? 
    # Actually checking field_mappings: 'bid_date': ['입찰서접수시작일시'...] 
    # 'announcement_date' key doesn't seem to have '공고일시' directly in the provided code snippet unless fuzzy match catches it.
    # Wait, 'announcement_date' is mapped to? 
    # Looking at detail_parser.py: 'announcement_date' is NOT in the mapping list I saw earlier?
    # Ah, 'opening_date' is mapped. 'bid_date' is mapped.
    # 'budget_amount' is mapped to '배정예산'.
    
    # Let's verify budget_amount which is definitely mapped
    assert result['budget_amount'] == "1,000,000원"

def test_detail_parser_attached_files(mock_config, mock_page):
    """Test attached file extraction."""
    parser = DetailPageParser(mock_config)
    
    mock_context = MagicMock()
    parser._find_detail_context = Mock(return_value=mock_context)
    
    # Return empty dict for table data to focus on files
    parser._extract_all_table_data = Mock(return_value={})
    
    # Mock file divs
    mock_div = MagicMock()
    mock_link = MagicMock()
    mock_link.inner_text.return_value = "test_file.pdf"
    mock_link.get_attribute.return_value = "http://download.com/file"
    
    # Mock size extraction
    mock_row = MagicMock()
    mock_row.evaluate.return_value = "test_file.pdf (1.5MB)"
    mock_link.evaluate_handle.return_value = mock_row
    
    mock_div.query_selector_all.return_value = [mock_link]
    mock_context.query_selector_all.side_effect = [
        [mock_div], # Strategy 1: file divs
    ]
    
    # Run
    result = parser.parse_page(mock_page, {'bid_notice_number': '123'})
    
    assert 'attached_files' in result
    files = result['attached_files']
    assert len(files) == 1
    assert files[0].filename == "test_file.pdf"
    assert files[0].url == "http://download.com/file"
    assert files[0].size == "1.5MB"
    assert files[0].file_type == "pdf"

def test_clean_opening_date():
    """Test date cleaning logic."""
    parser = DetailPageParser({})
    
    # YYYY/MM/DD HH:MM
    assert parser._clean_opening_date("2024/01/01 10:00") == "2024/01/01 10:00"
    
    # Korean format
    assert parser._clean_opening_date("2024년 01월 01일 10:00") == "2024년 01월 01일 10:00" 
    
    # Garbage with valid date
    garbage = "Some garbage text 2024-01-01 10:00 more garbage"
    assert parser._clean_opening_date(garbage) == "2024-01-01 10:00"

def test_map_to_schema_fuzzy_matching():
    """Test fuzzy matching for labels."""
    parser = DetailPageParser({})
    
    raw_data = {
        '배정예산액 (원)': '1,000',
        '입찰공고번호 ': '12345',
        ' 개찰일시 : ': '2024/01/01 11:00'
    }
    
    mapped = parser._map_to_schema(raw_data)
    
    # '배정예산액' matches 'budget_amount'
    assert mapped['budget_amount'] == '1,000'
    
    # '개찰일시' matches 'opening_date'
    assert mapped['opening_date'] == '2024/01/01 11:00'
