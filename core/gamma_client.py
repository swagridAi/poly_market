
"""Gamma API client for market metadata."""

import json
from typing import List, Dict, Optional
from core.api_client import BaseAPIClient
from config.settings import Config

class GammaClient(BaseAPIClient):
    """Client for Gamma API (market metadata)."""
    
    def __init__(self, logger=None):
        super().__init__(Config.GAMMA_BASE, logger)
    
    def get_event_markets(self, slug: str) -> List[Dict]:
        """Fetch markets for an event slug."""
        # Try direct market lookup first
        rows = self._get("/markets", slug=slug, limit=1)
        if rows:
            return [rows[0]]
        
        # Fall back to event lookup
        events = self._get("/events", slug=slug, limit=1)
        if not events:
            raise ValueError(f"No market or event found for slug: {slug}")
        return events[0].get("markets", [])
    
    def get_market_metadata(self, market_id: str) -> Dict:
        """Fetch detailed market metadata."""
        return self._get(f"/markets/{market_id}")
