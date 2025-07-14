import os
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
from ..base.extractor import BaseExtractor
from .client import AcumaticaClient
from .endpoints import get_endpoint_config, get_all_endpoints


class AcumaticaExtractor(BaseExtractor):
    """Acumatica data extractor"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self.client = AcumaticaClient(
            base_url=config['base_url'],
            username=config['username'],
            password=config['password'],
            company=config.get('company'),
            branch=config.get('branch')
        )
    
    def test_connection(self) -> bool:
        """Test connection to Acumatica"""
        try:
            return self.client.authenticate()
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def _flatten_acumatica_record(self, record: Dict) -> Dict:
        """Flatten Acumatica's nested {'value': 'data'} structure"""
        flattened = {}
        
        for key, value in record.items():
            if isinstance(value, dict) and 'value' in value:
                # Extract the actual value from {'value': 'data'}
                flattened[key] = value['value']
            elif isinstance(value, dict) and not value:
                # Empty dict
                flattened[key] = None
            else:
                # Keep as is (like id, rowNumber, etc.)
                flattened[key] = value
                
        return flattened
    
    def extract(self, endpoint_name: str, save_to_file: bool = True) -> pd.DataFrame:
        """Extract data from Acumatica endpoint"""
        
        endpoint_config = get_endpoint_config(endpoint_name)
        if not endpoint_config:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")
        
        endpoint = endpoint_config['endpoint']
        
        try:
            # Get data from API
            self.logger.info(f"Extracting data from {endpoint}")
            raw_data = self.client.get_paginated_data(endpoint)
            
            if not raw_data:
                self.logger.warning(f"No data found for endpoint: {endpoint}")
                return pd.DataFrame()
            
            # Flatten the nested JSON structure
            flattened_data = []
            for record in raw_data:
                flattened_record = self._flatten_acumatica_record(record)
                flattened_data.append(flattened_record)
            
            # Convert to DataFrame
            df = pd.DataFrame(flattened_data)
            
            # Add metadata columns
            df['_extracted_at'] = datetime.now()
            df['_source_system'] = 'acumatica'
            df['_endpoint'] = endpoint_name
            
            self.logger.info(f"Extracted {len(df)} records from {endpoint}")
            
            # Save to file if requested
            if save_to_file:
                self._save_extraction(df, endpoint_name)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract data from {endpoint}: {e}")
            raise
    
    def extract_all_endpoints(self, save_to_file: bool = True) -> Dict[str, pd.DataFrame]:
        """Extract data from all configured endpoints"""
        results = {}
        
        for endpoint_name in get_all_endpoints():
            try:
                df = self.extract(endpoint_name, save_to_file)
                results[endpoint_name] = df
            except Exception as e:
                self.logger.error(f"Failed to extract {endpoint_name}: {e}")
                results[endpoint_name] = pd.DataFrame()
        
        return results
    
    def _save_extraction(self, df: pd.DataFrame, endpoint_name: str) -> None:
        """Save extracted data to local file"""
        
        # Create directory if it doesn't exist
        output_dir = "data/raw/acumatica"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{endpoint_name}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        self.logger.info(f"Saved {len(df)} records to {filepath}")