"""Base API client with common HTTP functionality."""

import requests
import time
from typing import Dict, Any, Optional
from config.settings import Config
from utils.logger import setup_logger

class BaseAPIClient:
    """Base class for API interactions."""
    
    def __init__(self, base_url: str, logger=None):
        self.base_url = base_url
        self.logger = logger or setup_logger()
        self.session = requests.Session()
        
    def _get(self, resource: str, **params) -> Dict[str, Any]:
        """Execute GET request with error handling."""
        url = f"{self.base_url}{resource}"
        self.logger.debug("GET %s params=%s", url, params)  # Check if params are truncated here
        
        try:
            r = self.session.get(url, params=params, timeout=Config.TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            self.logger.error("HTTP %s – %s", r.status_code, r.text[:200])
            raise
        except Exception as e:
            self.logger.error("Request failed: %s", e)
            raise
    
    def rate_limit_wait(self):
        """Apply rate limiting between requests."""
        time.sleep(Config.RATE_LIMIT_DELAY)
