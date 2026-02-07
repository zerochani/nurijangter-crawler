"""Tests for deduplication utilities."""

import pytest
from pathlib import Path
import tempfile
import shutil

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import DeduplicationManager


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_items():
    """Create sample items for testing."""
    return [
        {'bid_notice_number': '001', 'name': 'Item 1'},
        {'bid_notice_number': '002', 'name': 'Item 2'},
        {'bid_notice_number': '001', 'name': 'Item 1 Duplicate'},  # Duplicate
    ]


class TestDeduplicationManager:
    """Tests for DeduplicationManager."""

    def test_initialization(self, temp_dir):
        """Test manager initialization."""
        manager = DeduplicationManager(
            key_fields=['bid_notice_number'],
            storage_file=temp_dir / 'seen.json',
            enabled=True
        )

        assert manager.enabled
        assert manager.key_fields == ['bid_notice_number']
        assert len(manager) == 0

    def test_duplicate_detection(self, sample_items):
        """Test duplicate detection."""
        manager = DeduplicationManager(
            key_fields=['bid_notice_number'],
            enabled=True
        )

        # First item should not be duplicate
        assert not manager.is_duplicate(sample_items[0])

        # Mark as seen
        manager.mark_as_seen(sample_items[0])

        # Same item should now be duplicate
        assert manager.is_duplicate(sample_items[0])

        # Third item (different number) should not be duplicate
        assert not manager.is_duplicate(sample_items[1])

        # Item with same number should be duplicate
        assert manager.is_duplicate(sample_items[2])

    def test_save_and_load(self, temp_dir, sample_items):
        """Test saving and loading seen items."""
        storage_file = temp_dir / 'seen.json'

        # Create manager and mark items as seen
        manager1 = DeduplicationManager(
            key_fields=['bid_notice_number'],
            storage_file=storage_file,
            enabled=True
        )

        for item in sample_items[:2]:  # Mark first two as seen
            manager1.mark_as_seen(item)

        manager1.save()

        # Create new manager and load
        manager2 = DeduplicationManager(
            key_fields=['bid_notice_number'],
            storage_file=storage_file,
            enabled=True
        )

        # Check that items are recognized as duplicates
        assert manager2.is_duplicate(sample_items[0])
        assert manager2.is_duplicate(sample_items[1])

    def test_disabled_deduplication(self, sample_items):
        """Test that deduplication can be disabled."""
        manager = DeduplicationManager(
            key_fields=['bid_notice_number'],
            enabled=False
        )

        # Mark as seen
        manager.mark_as_seen(sample_items[0])

        # Should not be detected as duplicate when disabled
        assert not manager.is_duplicate(sample_items[0])

    def test_multiple_key_fields(self):
        """Test deduplication with multiple key fields."""
        manager = DeduplicationManager(
            key_fields=['bid_notice_number', 'name'],
            enabled=True
        )

        item1 = {'bid_notice_number': '001', 'name': 'Item 1'}
        item2 = {'bid_notice_number': '001', 'name': 'Item 2'}  # Same number, different name

        manager.mark_as_seen(item1)

        # item1 should be duplicate
        assert manager.is_duplicate(item1)

        # item2 should NOT be duplicate (different name)
        assert not manager.is_duplicate(item2)

    def test_get_stats(self, sample_items):
        """Test getting statistics."""
        manager = DeduplicationManager(
            key_fields=['bid_notice_number'],
            enabled=True
        )

        for item in sample_items:
            if not manager.is_duplicate(item):
                manager.mark_as_seen(item)

        stats = manager.get_stats()

        assert stats['enabled']
        assert stats['key_fields'] == ['bid_notice_number']
        assert stats['total_seen'] == 2  # Only unique items


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
