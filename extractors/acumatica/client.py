import requests
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import time

class AcumaticaClient:
    """Acumatica API Client with authentication and rate limiting"""
    
    def __init__(self, base_url: str, username: str, password: str, 
                 company: str = None, branch: str = None):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.company = company
        self.branch = branch
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self._access_token = None
        self._token_expires_at = None
        
    def authenticate(self) -> bool:
        """Authenticate with Acumatica API"""
        try:
            login_url = f"{self.base_url}/entity/auth/login"
            
            login_data = {
                "name": self.username,
                "password": self.password
            }
            
            if self.company:
                login_data["company"] = self.company
            if self.branch:
                login_data["branch"] = self.branch
            
            response = self.session.post(login_url, json=login_data)
            response.raise_for_status()
            
            # Acumatica typically returns session cookies
            self.logger.info("Successfully authenticated with Acumatica")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
    
    def make_request(self, endpoint: str, method: str = "GET", 
                    params: Dict = None, data: Dict = None) -> requests.Response:
        """Make authenticated request to Acumatica API"""
        
        # Ensure we're authenticated
        if not self._is_authenticated():
            if not self.authenticate():
                raise Exception("Failed to authenticate")
        
        url = f"{self.base_url}/entity/Default/23.200.001/{endpoint}"
        
        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params)
            elif method.upper() == "POST":
                response = self.session.post(url, json=data, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise
    
    def _is_authenticated(self) -> bool:
        """Check if current session is authenticated"""
        # For simplicity, we'll re-authenticate for each session
        # In production, you might want to implement token checking
        return False
    
    def get_paginated_data(self, endpoint: str, page_size: int = 100) -> List[Dict]:
        """Get all data from paginated endpoint"""
        all_data = []
        skip = 0
        
        while True:
            params = {
                '$top': page_size,
                '$skip': skip
            }
            
            response = self.make_request(endpoint, params=params)
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and 'value' in data:
                records = data['value']
            else:
                records = [data] if data else []
            
            if not records:
                break
                
            all_data.extend(records)
            
            # If we got less than page_size, we're done
            if len(records) < page_size:
                break
                
            skip += page_size
            
            # Rate limiting - be nice to the API
            time.sleep(0.1)
        
        self.logger.info(f"Retrieved {len(all_data)} records from {endpoint}")
        return all_data