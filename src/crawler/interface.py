"""
Base interface for NuriJangter crawler.

This module defines the abstract base class that all crawler implementations
(synchronous or asynchronous) must inherit from.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from ..models import BidNoticeList

class BaseCrawler(ABC):
    """
    Abstract base class for NuriJangter crawler.
    
    Defines the contract that all crawler implementations must follow.
    """
    
    @abstractmethod
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the crawler with configuration.
        
        Args:
            config: Configuration dictionary
        """
        pass
        
    @abstractmethod
    def run(self, resume: bool = True) -> BidNoticeList:
        """
        Run the crawler.
        
        Args:
            resume: Whether to resume from checkpoint if available
            
        Returns:
            BidNoticeList with collected data
        """
        pass
