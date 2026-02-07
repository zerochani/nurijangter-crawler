"""Parser modules for extracting data from NuriJangter pages."""

from .list_parser import ListPageParser
from .detail_parser import DetailPageParser

__all__ = ["ListPageParser", "DetailPageParser"]
