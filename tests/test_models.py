import pytest
from datetime import datetime
from src.models import BidNotice, AttachedFile, BidNoticeList
from pydantic import ValidationError

def test_bid_notice_creation_valid():
    """Test creating a BidNotice with valid data."""
    data = {
        "bid_notice_number": "20240101234-00",
        "bid_notice_name": "Test Notice",
        "announcement_agency": "Test Agency",
        "announcement_date": "2024-01-01",
        "budget_amount": "10,000,000",
        "phone_number": "010-1234-5678"
    }
    notice = BidNotice(**data)
    
    assert notice.bid_notice_number == "20240101234-00"
    assert notice.bid_notice_name == "Test Notice"
    assert notice.announcement_agency == "Test Agency"
    assert notice.budget_amount == "10,000,000"
    assert notice.phone_number == "010-1234-5678"

def test_bid_notice_validation_error():
    """Test that missing required fields raises ValidationError."""
    data = {
        "bid_notice_name": "Test Notice",
        # Missing bid_notice_number and announcement_agency
    }
    
    with pytest.raises(ValidationError) as excinfo:
        BidNotice(**data)
    
    errors = excinfo.value.errors()
    error_fields = [e['loc'][0] for e in errors]
    assert "bid_notice_number" in error_fields
    assert "announcement_agency" in error_fields

def test_attached_file_model():
    """Test AttachedFile model."""
    file_data = {
        "filename": "test.pdf",
        "url": "http://example.com/test.pdf",
        "size": "1MB",
        "file_type": "pdf"
    }
    file_obj = AttachedFile(**file_data)
    assert file_obj.filename == "test.pdf"
    assert file_obj.url == "http://example.com/test.pdf"

def test_bid_notice_with_files():
    """Test BidNotice with attached files."""
    data = {
        "bid_notice_number": "20240101234-00",
        "bid_notice_name": "Test Notice",
        "announcement_agency": "Test Agency",
        "attached_files": [
            {
                "filename": "test.pdf",
                "url": "http://example.com/test.pdf"
            }
        ]
    }
    notice = BidNotice(**data)
    assert len(notice.attached_files) == 1
    assert notice.attached_files[0].filename == "test.pdf"

def test_bid_notice_to_flat_dict():
    """Test converting BidNotice to flat dictionary for CSV."""
    data = {
        "bid_notice_number": "20240101234-00",
        "bid_notice_name": "Test Notice",
        "announcement_agency": "Test Agency",
        "attached_files": [
            {"filename": "file1.txt", "size": "1KB"},
            {"filename": "file2.txt", "size": "2KB"}
        ],
        "additional_info": {"extra_field": "extra_value"}
    }
    notice = BidNotice(**data)
    flat = notice.to_flat_dict()
    
    assert flat['bid_notice_number'] == "20240101234-00"
    assert "file1.txt (1KB)" in flat['attached_files']
    assert "file2.txt (2KB)" in flat['attached_files']
    assert flat['extra_extra_field'] == "extra_value"
    assert 'additional_info' not in flat

def test_bid_notice_list():
    """Test BidNoticeList functionality."""
    notice_list = BidNoticeList()
    
    notice1 = BidNotice(
        bid_notice_number="1", 
        bid_notice_name="N1", 
        announcement_agency="A1"
    )
    notice2 = BidNotice(
        bid_notice_number="2", 
        bid_notice_name="N2", 
        announcement_agency="A2"
    )
    
    notice_list.add_notice(notice1)
    notice_list.add_notice(notice2)
    
    assert len(notice_list.notices) == 2
    assert notice_list.total_count == 2
