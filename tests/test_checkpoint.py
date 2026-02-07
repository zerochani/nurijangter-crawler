"""Tests for checkpoint manager."""

import pytest
from pathlib import Path
import tempfile
import shutil

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.checkpoint import CheckpointManager, CrawlState


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


class TestCheckpointManager:
    """Tests for CheckpointManager."""

    def test_initialization(self, temp_dir):
        """Test checkpoint manager initialization."""
        manager = CheckpointManager(
            checkpoint_dir=temp_dir,
            checkpoint_file='test_checkpoint.json',
            save_interval=5
        )

        assert manager.state == CrawlState.INITIALIZED
        assert manager.current_page == 1
        assert len(manager.processed_items) == 0

    def test_initialize_crawl(self, temp_dir):
        """Test crawl initialization."""
        manager = CheckpointManager(checkpoint_dir=temp_dir)

        metadata = {'target': 'test', 'start_time': '2024-01-01'}
        manager.initialize_crawl(metadata)

        assert manager.state == CrawlState.IN_PROGRESS
        assert manager.metadata == metadata
        assert 'start_time' in manager.statistics

    def test_save_and_load_checkpoint(self, temp_dir):
        """Test saving and loading checkpoint."""
        manager1 = CheckpointManager(
            checkpoint_dir=temp_dir,
            checkpoint_file='test.json'
        )

        # Initialize and add some data
        manager1.initialize_crawl({'target': 'test'})
        manager1.mark_item_processed('item1')
        manager1.mark_item_processed('item2')
        manager1.current_page = 5

        # Save checkpoint
        manager1.save_checkpoint(force=True)

        # Create new manager and load
        manager2 = CheckpointManager(
            checkpoint_dir=temp_dir,
            checkpoint_file='test.json'
        )

        success = manager2.load_checkpoint()

        assert success
        assert manager2.current_page == 5
        assert 'item1' in manager2.processed_items
        assert 'item2' in manager2.processed_items

    def test_mark_item_processed(self, temp_dir):
        """Test marking items as processed."""
        manager = CheckpointManager(
            checkpoint_dir=temp_dir,
            save_interval=2
        )

        manager.initialize_crawl()

        manager.mark_item_processed('item1')
        assert manager.is_item_processed('item1')
        assert not manager.is_item_processed('item2')

        manager.mark_item_processed('item2')
        assert manager.is_item_processed('item2')

    def test_mark_item_failed(self, temp_dir):
        """Test marking items as failed."""
        manager = CheckpointManager(checkpoint_dir=temp_dir)
        manager.initialize_crawl()

        manager.mark_item_failed('item1', 'Test error', {'detail': 'info'})

        assert len(manager.failed_items) == 1
        assert manager.failed_items[0]['item_id'] == 'item1'
        assert manager.failed_items[0]['error'] == 'Test error'

    def test_advance_page(self, temp_dir):
        """Test page advancement."""
        manager = CheckpointManager(checkpoint_dir=temp_dir)
        manager.initialize_crawl()

        assert manager.current_page == 1

        manager.advance_page()
        assert manager.current_page == 2

        manager.advance_page()
        assert manager.current_page == 3

    def test_complete_crawl(self, temp_dir):
        """Test crawl completion."""
        manager = CheckpointManager(checkpoint_dir=temp_dir)
        manager.initialize_crawl()

        manager.mark_item_processed('item1')
        manager.mark_item_processed('item2')

        manager.complete_crawl(success=True)

        assert manager.state == CrawlState.COMPLETED
        assert 'end_time' in manager.statistics
        assert 'duration_seconds' in manager.statistics

    def test_get_resume_info(self, temp_dir):
        """Test getting resume information."""
        manager = CheckpointManager(checkpoint_dir=temp_dir)
        manager.initialize_crawl()

        manager.current_page = 3
        manager.mark_item_processed('item1')

        info = manager.get_resume_info()

        assert info['current_page'] == 3
        assert info['total_processed'] == 1
        assert info['can_resume']

    def test_auto_save_on_interval(self, temp_dir):
        """Test automatic checkpoint saving on interval."""
        checkpoint_file = temp_dir / 'auto_save.json'

        manager = CheckpointManager(
            checkpoint_dir=temp_dir,
            checkpoint_file='auto_save.json',
            save_interval=3
        )

        manager.initialize_crawl()

        # Process items
        manager.mark_item_processed('item1')
        manager.mark_item_processed('item2')

        # Should not save yet (interval is 3)
        assert not checkpoint_file.exists()

        manager.mark_item_processed('item3')

        # Should auto-save now
        assert checkpoint_file.exists()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
