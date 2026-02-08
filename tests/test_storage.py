"""Tests for storage modules."""

import pytest
import json
import csv
from pathlib import Path
import tempfile
import shutil

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage import JSONStorage, CSVStorage
from src.models import BidNotice


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_data():
    """Create sample bid notice data."""
    return [
        {
            'bid_notice_number': '20240101-001',
            'bid_notice_name': 'Test Bid 1',
            'announcement_agency': 'Test Agency',
            'budget_amount': '10,000,000원'
        },
        {
            'bid_notice_number': '20240101-002',
            'bid_notice_name': 'Test Bid 2',
            'announcement_agency': 'Test Agency 2',
            'budget_amount': '20,000,000원'
        }
    ]


class TestJSONStorage:
    """Tests for JSONStorage."""

    def test_save_and_load(self, temp_dir, sample_data):
        """Test saving and loading JSON data."""
        config = {
            'indent': 2,
            'ensure_ascii': False,
            'filename_pattern': 'test_output.json'
        }

        storage = JSONStorage(temp_dir, config)

        # Save data
        file_path = storage.save(sample_data, 'test_output.json')

        assert file_path.exists()

        # Load data
        loaded_data = storage.load(file_path)

        assert len(loaded_data) == len(sample_data)
        assert loaded_data[0]['bid_notice_number'] == sample_data[0]['bid_notice_number']

    def test_append(self, temp_dir, sample_data):
        """Test appending to JSON file."""
        config = {
            'indent': 2,
            'ensure_ascii': False,
            'filename_pattern': 'test_append.json'
        }

        storage = JSONStorage(temp_dir, config)
        storage.file_path = temp_dir / 'test_append.json'

        # Append items
        for item in sample_data:
            storage.append(item)

        # Load and verify
        loaded_data = storage.load(storage.file_path)

        assert len(loaded_data) == len(sample_data)

    def test_empty_data(self, temp_dir):
        """Test handling empty data."""
        config = {'indent': 2}
        storage = JSONStorage(temp_dir, config)

        result = storage.save([], 'empty.json')
        assert result.exists()
        loaded = storage.load(result)
        assert loaded == []


class TestCSVStorage:
    """Tests for CSVStorage."""

    def test_save_and_load(self, temp_dir, sample_data):
        """Test saving and loading CSV data."""
        config = {
            'encoding': 'utf-8',
            'delimiter': ',',
            'filename_pattern': 'test_output.csv'
        }

        storage = CSVStorage(temp_dir, config)

        # Save data
        file_path = storage.save(sample_data, 'test_output.csv')

        assert file_path.exists()

        # Load data
        loaded_data = storage.load(file_path)

        assert len(loaded_data) == len(sample_data)
        assert loaded_data[0]['bid_notice_number'] == sample_data[0]['bid_notice_number']

    def test_append(self, temp_dir, sample_data):
        """Test appending to CSV file."""
        config = {
            'encoding': 'utf-8',
            'delimiter': ',',
            'filename_pattern': 'test_append.csv'
        }

        storage = CSVStorage(temp_dir, config)
        storage.file_path = temp_dir / 'test_append.csv'

        # Set fieldnames
        storage.fieldnames = list(sample_data[0].keys())

        # Append items
        for item in sample_data:
            storage.append(item)

        # Load and verify
        loaded_data = storage.load(storage.file_path)

        assert len(loaded_data) == len(sample_data)

    def test_flatten_nested_data(self, temp_dir):
        """Test flattening nested structures."""
        config = {'encoding': 'utf-8', 'delimiter': ','}
        storage = CSVStorage(temp_dir, config)

        data = [
            {
                'id': '1',
                'tags': ['tag1', 'tag2'],
                'metadata': {'key': 'value'}
            }
        ]

        file_path = storage.save(data, 'nested.csv')
        loaded_data = storage.load(file_path)

        # Check that nested structures were flattened
        assert 'tags' in loaded_data[0]
        assert 'metadata' in loaded_data[0]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
