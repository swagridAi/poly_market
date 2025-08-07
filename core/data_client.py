"""Data API client for trade history."""

import pandas as pd
import time
from typing import Optional
from core.api_client import BaseAPIClient
from config.settings import Config

class DataClient(BaseAPIClient):
    """Client for Data API (trades)."""
    
    def __init__(self, logger=None):
        super().__init__(Config.DATA_BASE, logger)
    
    def fetch_trades(self, token_id: str,
                    start: Optional[int] = None,
                    end: Optional[int] = None,
                    limit: int = Config.DEFAULT_TRADE_LIMIT,
                    max_pages: int = Config.MAX_TRADE_PAGES) -> pd.DataFrame:
        """Fetch trade history with pagination."""
        params = {"asset": token_id, "limit": limit}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        
        dfs = []
        page = 0
        
        while page < max_pages:
            data = self._get("/trades", **params)
            if not data:
                break
            
            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            dfs.append(df)
            
            if len(data) < limit:
                break
            
            # Paginate backwards
            last_ts = data[-1]["timestamp"]
            params["endTime"] = last_ts - 1
            page += 1
            self.rate_limit_wait()
        
        return pd.concat(dfs).set_index("timestamp") if dfs else pd.DataFrame()
