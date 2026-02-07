"""
Base storage interface.

This module defines the abstract base class for all storage implementations.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class BaseStorage(ABC):
    """
    Abstract base class for storage implementations.

    All storage backends must implement this interface.
    """

    def __init__(self, output_dir: Path, config: Dict[str, Any]):
        """
        Initialize storage backend.

        Args:
            output_dir: Directory for output files
            config: Configuration dictionary
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.config = config
        self.file_path: Path = None

    @abstractmethod
    def save(self, data: List[Dict[str, Any]], filename: str = None) -> Path:
        """
        Save data to storage.

        Args:
            data: List of items to save
            filename: Optional custom filename

        Returns:
            Path to the saved file
        """
        pass

    @abstractmethod
    def append(self, item: Dict[str, Any]) -> None:
        """
        Append a single item to storage.

        Args:
            item: Item to append
        """
        pass

    @abstractmethod
    def load(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Load data from storage.

        Args:
            file_path: Path to the file to load

        Returns:
            List of loaded items
        """
        pass

    def get_output_filename(self, pattern: str) -> str:
        """
        Generate output filename from pattern.

        Args:
            pattern: Filename pattern (may include {timestamp})

        Returns:
            Generated filename
        """
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return pattern.format(timestamp=timestamp)

    def get_file_path(self, filename: str = None) -> Path:
        """
        Get full file path for output.

        Args:
            filename: Optional filename

        Returns:
            Full path to output file
        """
        if filename:
            return self.output_dir / filename
        elif self.file_path:
            return self.file_path
        else:
            # Generate default filename
            default_pattern = self.config.get('filename_pattern', 'output_{timestamp}.dat')
            filename = self.get_output_filename(default_pattern)
            return self.output_dir / filename
