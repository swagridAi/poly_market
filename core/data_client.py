import pandas as pd
import time
from typing import Optional
from core.api_client import BaseAPIClient
from config.settings import Config
from utils.token_utils import convert_token_id

class DataClient(BaseAPIClient):
    """Client for Data API (trades) with proper filtering."""
    
    def __init__(self, logger=None):
        super().__init__(Config.DATA_BASE, logger)
    
    def fetch_trades(self, token_id: str,
                    start: Optional[int] = None,
                    end: Optional[int] = None,
                    limit: int = Config.DEFAULT_TRADE_LIMIT,
                    max_pages: int = Config.MAX_TRADE_PAGES) -> pd.DataFrame:
        """Fetch trade history with pagination and FILTERING."""
        
        # The Data API seems to work with decimal format
        # But we should be prepared for both
        token_decimal = convert_token_id(token_id, "decimal")
        token_hex = convert_token_id(token_id, "hex")
        
        self.logger.info(f"Fetching trades for token (decimal): {token_decimal}")
        self.logger.debug(f"Token hex equivalent: {token_hex}")
    
        # Use decimal format for the API call
        params = {"asset": token_decimal, "limit": limit}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        
        dfs = []
        page = 0
        total_fetched = 0
        filtered_count = 0
        
        while page < max_pages:
            data = self._get("/trades", **params)
            if not data:
                break
            
            total_fetched += len(data)
            
            # Log sample assets to debug format issues
            if data and self.logger:
                unique_assets = list(set(str(trade.get("asset", "NONE")) for trade in data[:3]))
                self.logger.debug(f"Sample assets in response: {unique_assets}")
            
            # Filter trades to only include the requested token
            # Check both decimal and hex formats
            filtered_data = [
                trade for trade in data 
                if (str(trade.get("asset")) == token_decimal or 
                    str(trade.get("asset")) == token_hex or
                    str(trade.get("asset")) == token_id)
            ]
            
            filtered_count += len(filtered_data)
            
            if filtered_data:
                df = pd.DataFrame(filtered_data)
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                dfs.append(df)
            
            # Log filtering stats
            if len(data) != len(filtered_data):
                self.logger.debug(
                    f"API returned {len(data)} trades but only {len(filtered_data)} "
                    f"matched token"
                )
            
            # Check if we should continue paginating
            if len(data) < limit:
                break
            
            # Pagination logic
            if filtered_data:
                last_ts = filtered_data[-1]["timestamp"]
            elif data:
                last_ts = data[-1]["timestamp"]
            else:
                break
            
            params["endTime"] = last_ts - 1
            page += 1
            self.rate_limit_wait()
        
        self.logger.info(
            f"Trade fetch complete: {total_fetched} total trades fetched, "
            f"{filtered_count} matched token"
        )
        
        if not dfs:
            self.logger.warning(f"No trades found for token")
            return pd.DataFrame()
        
        result = pd.concat(dfs).set_index("timestamp")
        
        return result