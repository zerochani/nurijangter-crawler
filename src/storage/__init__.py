"""Storage layer for saving crawled data."""

from .json_storage import JSONStorage
from .csv_storage import CSVStorage
from .base import BaseStorage

__all__ = ["JSONStorage", "CSVStorage", "BaseStorage"]
