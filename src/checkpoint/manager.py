"""
Checkpoint management for resumable crawling.

This module provides functionality to save and restore crawl state,
enabling the crawler to resume from where it left off in case of
interruption or failure.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CrawlState(str, Enum):
    """Possible states of a crawl session."""
    INITIALIZED = "initialized"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class CheckpointManager:
    """
    Manages checkpoint creation and restoration for resumable crawling.

    Checkpoints include:
    - Current page/offset in pagination
    - List of processed item IDs
    - Crawl state and statistics
    - Error information (if any)
    """

    def __init__(
        self,
        checkpoint_dir: Path,
        checkpoint_file: str = "crawler_checkpoint.json",
        save_interval: int = 10
    ):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_dir: Directory to store checkpoint files
            checkpoint_file: Name of the checkpoint file
            save_interval: Save checkpoint every N processed items
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        self.checkpoint_file = self.checkpoint_dir / checkpoint_file
        self.save_interval = save_interval

        self.state: CrawlState = CrawlState.INITIALIZED
        self.current_page: int = 1
        self.processed_items: List[str] = []
        self.failed_items: List[Dict[str, Any]] = []
        self.statistics: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}
        self.last_save_time: Optional[datetime] = None

        self._items_since_save: int = 0

    def initialize_crawl(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize a new crawl session.

        Args:
            metadata: Optional metadata about the crawl
        """
        self.state = CrawlState.IN_PROGRESS
        self.current_page = 1
        self.processed_items = []
        self.failed_items = []
        self.statistics = {
            "start_time": datetime.now().isoformat(),
            "total_processed": 0,
            "total_failed": 0,
            "pages_crawled": 0
        }
        self.metadata = metadata or {}
        self._items_since_save = 0

        logger.info("Initialized new crawl session")

    def load_checkpoint(self) -> bool:
        """
        Load checkpoint from file.

        Returns:
            True if checkpoint was loaded successfully, False otherwise
        """
        if not self.checkpoint_file.exists():
            logger.info("No checkpoint file found, starting fresh")
            return False

        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.state = CrawlState(data.get("state", CrawlState.INITIALIZED))
            self.current_page = data.get("current_page", 1)
            self.processed_items = data.get("processed_items", [])
            self.failed_items = data.get("failed_items", [])
            self.statistics = data.get("statistics", {})
            self.metadata = data.get("metadata", {})

            logger.info(
                f"Loaded checkpoint: page {self.current_page}, "
                f"{len(self.processed_items)} processed items"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return False

    def save_checkpoint(self, force: bool = False) -> None:
        """
        Save current state to checkpoint file.

        Args:
            force: Force save even if save_interval not reached
        """
        if not force and self._items_since_save < self.save_interval:
            return

        try:
            data = {
                "state": self.state.value,
                "current_page": self.current_page,
                "processed_items": self.processed_items,
                "failed_items": self.failed_items,
                "statistics": self.statistics,
                "metadata": self.metadata,
                "last_updated": datetime.now().isoformat()
            }

            # Write to temporary file first, then rename (atomic operation)
            temp_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            temp_file.replace(self.checkpoint_file)

            self.last_save_time = datetime.now()
            self._items_since_save = 0

            logger.debug(f"Checkpoint saved: page {self.current_page}")

        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def mark_item_processed(self, item_id: str) -> None:
        """
        Mark an item as processed.

        Args:
            item_id: Unique identifier for the item
        """
        self.processed_items.append(item_id)
        self.statistics["total_processed"] = len(self.processed_items)
        self._items_since_save += 1

        # Auto-save if interval reached
        if self._items_since_save >= self.save_interval:
            self.save_checkpoint()

    def mark_item_failed(self, item_id: str, error: str, details: Optional[Dict] = None) -> None:
        """
        Mark an item as failed.

        Args:
            item_id: Unique identifier for the item
            error: Error message
            details: Optional additional details
        """
        failed_item = {
            "item_id": item_id,
            "error": error,
            "timestamp": datetime.now().isoformat()
        }
        if details:
            failed_item["details"] = details

        self.failed_items.append(failed_item)
        self.statistics["total_failed"] = len(self.failed_items)

    def remove_failed_item(self, item_id: str) -> None:
        """
        Remove an item from the failed items list.

        Args:
            item_id: Unique identifier for the item to remove
        """
        initial_count = len(self.failed_items)
        self.failed_items = [
            item for item in self.failed_items 
            if item.get("item_id") != item_id
        ]
        
        if len(self.failed_items) < initial_count:
            self.statistics["total_failed"] = len(self.failed_items)
            # Save immediately to persist the removal
            self.save_checkpoint(force=True)
            logger.info(f"Removed item {item_id} from failed items list")

    def is_item_processed(self, item_id: str) -> bool:
        """
        Check if an item has been processed.

        Args:
            item_id: Unique identifier for the item

        Returns:
            True if item was processed, False otherwise
        """
        return item_id in self.processed_items

    def advance_page(self) -> None:
        """Advance to the next page."""
        self.current_page += 1
        self.statistics["pages_crawled"] = self.current_page - 1
        logger.debug(f"Advanced to page {self.current_page}")

    def set_state(self, state: CrawlState) -> None:
        """
        Set the current crawl state.

        Args:
            state: New crawl state
        """
        old_state = self.state
        self.state = state
        logger.info(f"Crawl state changed: {old_state.value} -> {state.value}")

    def complete_crawl(self, success: bool = True) -> None:
        """
        Mark the crawl as completed.

        Args:
            success: Whether the crawl completed successfully
        """
        self.state = CrawlState.COMPLETED if success else CrawlState.FAILED
        self.statistics["end_time"] = datetime.now().isoformat()

        # Calculate duration
        if "start_time" in self.statistics:
            start = datetime.fromisoformat(self.statistics["start_time"])
            end = datetime.fromisoformat(self.statistics["end_time"])
            duration = (end - start).total_seconds()
            self.statistics["duration_seconds"] = duration

        # Save final checkpoint
        self.save_checkpoint(force=True)

        logger.info(
            f"Crawl {'completed' if success else 'failed'}: "
            f"{self.statistics.get('total_processed', 0)} items processed, "
            f"{self.statistics.get('total_failed', 0)} items failed"
        )

    def update_statistics(self, stats: Dict[str, Any]) -> None:
        """
        Update crawl statistics.

        Args:
            stats: Dictionary of statistics to update
        """
        self.statistics.update(stats)

    def get_resume_info(self) -> Dict[str, Any]:
        """
        Get information needed to resume the crawl.

        Returns:
            Dictionary with resume information
        """
        return {
            "state": self.state.value,
            "current_page": self.current_page,
            "total_processed": len(self.processed_items),
            "total_failed": len(self.failed_items),
            "can_resume": self.state in [CrawlState.IN_PROGRESS, CrawlState.PAUSED]
        }

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get current crawl statistics.

        Returns:
            Dictionary with statistics
        """
        return self.statistics.copy()

    def get_failed_items(self) -> List[Dict[str, Any]]:
        """
        Get list of failed items.

        Returns:
            List of failed item records
        """
        return self.failed_items.copy()

    def clear_checkpoint(self) -> None:
        """Remove checkpoint file."""
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
                logger.info("Checkpoint file removed")
        except Exception as e:
            logger.error(f"Failed to remove checkpoint file: {e}")

    def backup_checkpoint(self, suffix: Optional[str] = None) -> Path:
        """
        Create a backup of the current checkpoint.

        Args:
            suffix: Optional suffix for backup file

        Returns:
            Path to backup file
        """
        if not self.checkpoint_file.exists():
            raise FileNotFoundError("No checkpoint file to backup")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = suffix or timestamp
        backup_file = self.checkpoint_file.with_name(
            f"{self.checkpoint_file.stem}_backup_{suffix}.json"
        )

        try:
            import shutil
            shutil.copy2(self.checkpoint_file, backup_file)
            logger.info(f"Checkpoint backed up to: {backup_file}")
            return backup_file
        except Exception as e:
            logger.error(f"Failed to backup checkpoint: {e}")
            raise
