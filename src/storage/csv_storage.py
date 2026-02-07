"""
CSV storage implementation.

This module provides CSV-based storage for crawled data.
"""

import csv
from typing import List, Dict, Any, Set
from pathlib import Path
import logging

from .base import BaseStorage

logger = logging.getLogger(__name__)


class CSVStorage(BaseStorage):
    """
    CSV storage backend.

    Stores data in CSV format with automatic header detection and
    support for nested structures.
    """

    def __init__(self, output_dir: Path, config: Dict[str, Any]):
        """
        Initialize CSV storage.

        Args:
            output_dir: Directory for output files
            config: Configuration dictionary with CSV-specific options
        """
        super().__init__(output_dir, config)
        self.encoding = config.get('encoding', 'utf-8-sig')  # BOM for Excel
        self.delimiter = config.get('delimiter', ',')
        self.filename_pattern = config.get('filename_pattern', 'bid_notices_{timestamp}.csv')
        self.fieldnames: List[str] = None

    def _flatten_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested structures in an item for CSV export.

        Args:
            item: Item to flatten

        Returns:
            Flattened item
        """
        flattened = {}

        for key, value in item.items():
            if value is None:
                flattened[key] = ""
            elif isinstance(value, (list, tuple)):
                # Convert lists to semicolon-separated strings
                if value and isinstance(value[0], dict):
                    # List of dicts - join with semicolon
                    flattened[key] = "; ".join([str(v) for v in value])
                else:
                    # List of primitives
                    flattened[key] = "; ".join([str(v) for v in value])
            elif isinstance(value, dict):
                # Nested dict - join key-value pairs
                flattened[key] = "; ".join([f"{k}: {v}" for k, v in value.items()])
            else:
                flattened[key] = str(value)

        return flattened

    def _collect_fieldnames(self, data: List[Dict[str, Any]]) -> List[str]:
        """
        Collect all unique field names from data.

        Args:
            data: List of items

        Returns:
            List of field names
        """
        fieldnames_set: Set[str] = set()

        for item in data:
            flattened = self._flatten_item(item)
            fieldnames_set.update(flattened.keys())

        # Sort fieldnames for consistency, but prioritize common fields
        priority_fields = [
            'bid_notice_number',
            'bid_notice_name',
            'announcement_agency',
            'bid_method',
            'announcement_date',
            'deadline_date',
            'budget_amount',
            'status'
        ]

        # Start with priority fields that exist
        fieldnames = [f for f in priority_fields if f in fieldnames_set]

        # Add remaining fields alphabetically
        remaining = sorted(fieldnames_set - set(fieldnames))
        fieldnames.extend(remaining)

        return fieldnames

    def save(self, data: List[Dict[str, Any]], filename: str = None) -> Path:
        """
        Save data to CSV file.

        Args:
            data: List of items to save
            filename: Optional custom filename

        Returns:
            Path to the saved file
        """
        if not data:
            logger.warning("No data to save")
            return None

        if not filename:
            filename = self.get_output_filename(self.filename_pattern)

        file_path = self.output_dir / filename

        try:
            # Collect all fieldnames
            fieldnames = self._collect_fieldnames(data)
            self.fieldnames = fieldnames

            # Write CSV
            with open(file_path, 'w', encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    delimiter=self.delimiter,
                    extrasaction='ignore'
                )

                writer.writeheader()

                for item in data:
                    flattened = self._flatten_item(item)
                    writer.writerow(flattened)

            logger.info(f"Saved {len(data)} items to {file_path}")
            self.file_path = file_path
            return file_path

        except Exception as e:
            logger.error(f"Failed to save CSV file: {e}")
            raise

    def append(self, item: Dict[str, Any]) -> None:
        """
        Append a single item to CSV file.

        Args:
            item: Item to append
        """
        file_path = self.get_file_path()

        try:
            # Check if file exists to determine if we need to write header
            file_exists = file_path.exists()

            # If file doesn't exist, we need to initialize fieldnames
            if not file_exists:
                if not self.fieldnames:
                    # Initialize fieldnames from first item
                    self.fieldnames = list(self._flatten_item(item).keys())

            # Open in append mode
            with open(file_path, 'a', encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=self.fieldnames,
                    delimiter=self.delimiter,
                    extrasaction='ignore'
                )

                # Write header if new file
                if not file_exists:
                    writer.writeheader()

                # Write item
                flattened = self._flatten_item(item)
                writer.writerow(flattened)

            logger.debug(f"Appended item to {file_path}")

        except Exception as e:
            logger.error(f"Failed to append to CSV file: {e}")
            raise

    def load(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Load data from CSV file.

        Args:
            file_path: Path to the CSV file

        Returns:
            List of loaded items
        """
        try:
            data = []

            with open(file_path, 'r', encoding=self.encoding, newline='') as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)

                for row in reader:
                    # Convert empty strings to None
                    cleaned_row = {
                        k: (v if v != "" else None)
                        for k, v in row.items()
                    }
                    data.append(cleaned_row)

            logger.info(f"Loaded {len(data)} items from {file_path}")
            return data

        except Exception as e:
            logger.error(f"Failed to load CSV file: {e}")
            raise
