"""
Deduplication utilities for the NuriJangter crawler.

This module provides mechanisms to detect and skip duplicate entries
based on configurable key fields.
"""

import json
import hashlib
from typing import Dict, List, Set, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class DeduplicationManager:
    """
    Manages deduplication of crawled items.

    Uses a hash-based approach to track seen items and prevent
    duplicate processing and storage.
    """

    def __init__(
        self,
        key_fields: List[str],
        storage_file: Optional[Path] = None,
        enabled: bool = True
    ):
        """
        Initialize deduplication manager.

        Args:
            key_fields: List of field names to use for generating unique keys
            storage_file: Optional file path to persist seen items
            enabled: Whether deduplication is enabled
        """
        self.key_fields = key_fields
        self.storage_file = Path(storage_file) if storage_file else None
        self.enabled = enabled
        self.seen_hashes: Set[str] = set()
        self.seen_items: Dict[str, Dict[str, Any]] = {}

        if self.enabled and self.storage_file:
            self._load_seen_items()

    def _generate_hash(self, item: Dict[str, Any]) -> str:
        """
        Generate a unique hash for an item based on key fields.

        Args:
            item: Dictionary representing the item

        Returns:
            SHA256 hash string
        """
        # Extract values for key fields
        key_values = []
        for field in self.key_fields:
            value = item.get(field, "")
            # Normalize value
            if value is None:
                value = ""
            key_values.append(str(value).strip().lower())

        # Create hash from concatenated key values
        key_string = "|".join(key_values)
        return hashlib.sha256(key_string.encode('utf-8')).hexdigest()

    def is_duplicate(self, item: Dict[str, Any]) -> bool:
        """
        Check if an item is a duplicate.

        Args:
            item: Dictionary representing the item

        Returns:
            True if item is a duplicate, False otherwise
        """
        if not self.enabled:
            return False

        item_hash = self._generate_hash(item)
        return item_hash in self.seen_hashes

    def mark_as_seen(self, item: Dict[str, Any]) -> str:
        """
        Mark an item as seen.

        Args:
            item: Dictionary representing the item

        Returns:
            Hash of the item
        """
        if not self.enabled:
            return ""

        item_hash = self._generate_hash(item)
        self.seen_hashes.add(item_hash)

        # Store minimal info about the item
        key_info = {field: item.get(field) for field in self.key_fields}
        self.seen_items[item_hash] = key_info

        logger.debug(f"Marked item as seen: {item_hash[:8]}...")
        return item_hash

    def save(self) -> None:
        """Save seen items to storage file."""
        if not self.enabled or not self.storage_file:
            return

        try:
            # Ensure directory exists
            self.storage_file.parent.mkdir(parents=True, exist_ok=True)

            # Save to file
            data = {
                "key_fields": self.key_fields,
                "seen_items": self.seen_items
            }

            with open(self.storage_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved {len(self.seen_items)} seen items to {self.storage_file}")

        except Exception as e:
            logger.error(f"Failed to save seen items: {e}")

    def _load_seen_items(self) -> None:
        """Load seen items from storage file."""
        if not self.storage_file or not self.storage_file.exists():
            logger.info("No existing deduplication data found, starting fresh")
            return

        try:
            with open(self.storage_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Verify key fields match
            stored_key_fields = data.get("key_fields", [])
            if stored_key_fields != self.key_fields:
                logger.warning(
                    f"Key fields mismatch. Stored: {stored_key_fields}, "
                    f"Current: {self.key_fields}. Starting fresh."
                )
                return

            # Load seen items
            self.seen_items = data.get("seen_items", {})
            self.seen_hashes = set(self.seen_items.keys())

            logger.info(f"Loaded {len(self.seen_items)} seen items from {self.storage_file}")

        except Exception as e:
            logger.error(f"Failed to load seen items: {e}")
            self.seen_items = {}
            self.seen_hashes = set()

    def clear(self) -> None:
        """Clear all seen items."""
        self.seen_hashes.clear()
        self.seen_items.clear()
        logger.info("Cleared all seen items")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about deduplication.

        Returns:
            Dictionary with statistics
        """
        return {
            "enabled": self.enabled,
            "key_fields": self.key_fields,
            "total_seen": len(self.seen_items),
            "storage_file": str(self.storage_file) if self.storage_file else None
        }

    def __len__(self) -> int:
        """Return number of seen items."""
        return len(self.seen_items)

    def __contains__(self, item: Dict[str, Any]) -> bool:
        """Check if item has been seen using 'in' operator."""
        return self.is_duplicate(item)


class BatchDeduplicator:
    """
    Utility for deduplicating a batch of items.

    Useful for post-processing collected data.
    """

    @staticmethod
    def deduplicate_list(
        items: List[Dict[str, Any]],
        key_fields: List[str],
        keep: str = "first"
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate a list of items.

        Args:
            items: List of items to deduplicate
            key_fields: Fields to use for identifying duplicates
            keep: Which duplicate to keep - "first" or "last"

        Returns:
            Deduplicated list of items
        """
        manager = DeduplicationManager(key_fields=key_fields, enabled=True)
        deduplicated = []
        duplicates_found = 0

        if keep == "first":
            for item in items:
                if not manager.is_duplicate(item):
                    manager.mark_as_seen(item)
                    deduplicated.append(item)
                else:
                    duplicates_found += 1
        elif keep == "last":
            # Process in reverse, then reverse result
            for item in reversed(items):
                if not manager.is_duplicate(item):
                    manager.mark_as_seen(item)
                    deduplicated.append(item)
                else:
                    duplicates_found += 1
            deduplicated.reverse()
        else:
            raise ValueError(f"Invalid keep value: {keep}. Must be 'first' or 'last'")

        logger.info(
            f"Deduplicated {len(items)} items: "
            f"kept {len(deduplicated)}, removed {duplicates_found} duplicates"
        )

        return deduplicated

    @staticmethod
    def find_duplicates(
        items: List[Dict[str, Any]],
        key_fields: List[str]
    ) -> Dict[str, List[int]]:
        """
        Find all duplicate items in a list.

        Args:
            items: List of items to check
            key_fields: Fields to use for identifying duplicates

        Returns:
            Dictionary mapping item hash to list of indices
        """
        manager = DeduplicationManager(key_fields=key_fields, enabled=True)
        hash_to_indices: Dict[str, List[int]] = {}

        for idx, item in enumerate(items):
            item_hash = manager._generate_hash(item)
            if item_hash not in hash_to_indices:
                hash_to_indices[item_hash] = []
            hash_to_indices[item_hash].append(idx)

        # Filter to only duplicates (more than one occurrence)
        duplicates = {
            h: indices for h, indices in hash_to_indices.items()
            if len(indices) > 1
        }

        return duplicates
