"""
JSON storage implementation.

This module provides JSON-based storage for crawled data.
"""

import json
from typing import List, Dict, Any
from pathlib import Path
import logging

from .base import BaseStorage

logger = logging.getLogger(__name__)


class JSONStorage(BaseStorage):
    """
    JSON storage backend.

    Stores data in JSON format with configurable formatting options.
    """

    def __init__(self, output_dir: Path, config: Dict[str, Any]):
        """
        Initialize JSON storage.

        Args:
            output_dir: Directory for output files
            config: Configuration dictionary with JSON-specific options
        """
        super().__init__(output_dir, config)
        self.indent = config.get('indent', 2)
        self.ensure_ascii = config.get('ensure_ascii', False)
        self.filename_pattern = config.get('filename_pattern', 'bid_notices_{timestamp}.json')

    def save(self, data: List[Dict[str, Any]], filename: str = None) -> Path:
        """
        Save data to JSON file.

        Args:
            data: List of items to save
            filename: Optional custom filename

        Returns:
            Path to the saved file
        """
        if not filename:
            filename = self.get_output_filename(self.filename_pattern)

        file_path = self.output_dir / filename

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    data,
                    f,
                    indent=self.indent,
                    ensure_ascii=self.ensure_ascii,
                    default=str  # Handle non-serializable types
                )

            logger.info(f"Saved {len(data)} items to {file_path}")
            self.file_path = file_path
            return file_path

        except Exception as e:
            logger.error(f"Failed to save JSON file: {e}")
            raise

    def append(self, item: Dict[str, Any]) -> None:
        """
        Append a single item to JSON file.

        Note: This is inefficient for JSON as it requires reading and rewriting
        the entire file. Consider using JSONLines format for append operations.

        Args:
            item: Item to append
        """
        file_path = self.get_file_path()

        try:
            # Load existing data
            if file_path.exists():
                data = self.load(file_path)
            else:
                data = []

            # Append new item
            data.append(item)

            # Save back
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    data,
                    f,
                    indent=self.indent,
                    ensure_ascii=self.ensure_ascii,
                    default=str
                )

            logger.debug(f"Appended item to {file_path}")

        except Exception as e:
            logger.error(f"Failed to append to JSON file: {e}")
            raise

    def load(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Load data from JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            List of loaded items
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle both single dict and list of dicts
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                raise ValueError(f"Expected list or dict, got {type(data)}")

            logger.info(f"Loaded {len(data)} items from {file_path}")
            return data

        except Exception as e:
            logger.error(f"Failed to load JSON file: {e}")
            raise


class JSONLinesStorage(BaseStorage):
    """
    JSON Lines storage backend.

    Stores data in JSON Lines format (one JSON object per line),
    which is more efficient for append operations and streaming.
    """

    def __init__(self, output_dir: Path, config: Dict[str, Any]):
        """
        Initialize JSON Lines storage.

        Args:
            output_dir: Directory for output files
            config: Configuration dictionary
        """
        super().__init__(output_dir, config)
        self.ensure_ascii = config.get('ensure_ascii', False)
        self.filename_pattern = config.get('filename_pattern', 'bid_notices_{timestamp}.jsonl')

    def save(self, data: List[Dict[str, Any]], filename: str = None) -> Path:
        """
        Save data to JSON Lines file.

        Args:
            data: List of items to save
            filename: Optional custom filename

        Returns:
            Path to the saved file
        """
        if not filename:
            filename = self.get_output_filename(self.filename_pattern)

        file_path = self.output_dir / filename

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                for item in data:
                    line = json.dumps(
                        item,
                        ensure_ascii=self.ensure_ascii,
                        default=str
                    )
                    f.write(line + '\n')

            logger.info(f"Saved {len(data)} items to {file_path}")
            self.file_path = file_path
            return file_path

        except Exception as e:
            logger.error(f"Failed to save JSON Lines file: {e}")
            raise

    def append(self, item: Dict[str, Any]) -> None:
        """
        Append a single item to JSON Lines file.

        This is efficient as it only appends a new line without
        reading the entire file.

        Args:
            item: Item to append
        """
        file_path = self.get_file_path()

        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                line = json.dumps(
                    item,
                    ensure_ascii=self.ensure_ascii,
                    default=str
                )
                f.write(line + '\n')

            logger.debug(f"Appended item to {file_path}")

        except Exception as e:
            logger.error(f"Failed to append to JSON Lines file: {e}")
            raise

    def load(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Load data from JSON Lines file.

        Args:
            file_path: Path to the JSON Lines file

        Returns:
            List of loaded items
        """
        try:
            data = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        item = json.loads(line)
                        data.append(item)

            logger.info(f"Loaded {len(data)} items from {file_path}")
            return data

        except Exception as e:
            logger.error(f"Failed to load JSON Lines file: {e}")
            raise
