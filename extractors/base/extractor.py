from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

class BaseExtractor(ABC):
    """Base class for all data extractors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def extract(self, endpoint: str, **kwargs) -> pd.DataFrame:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test connection to source"""
        pass
    
    def save_to_file(self, data: pd.DataFrame, filepath: str) -> None:
        """Save data to local file"""
        data.to_csv(filepath, index=False)
        self.logger.info(f"Saved {len(data)} records to {filepath}")