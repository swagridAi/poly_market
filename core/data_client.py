import pandas as pd
import time
from typing import Optional
from core.api_client import BaseAPIClient
from config.settings import Config

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
        params = {"asset": token_id, "limit": limit}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        
        dfs = []
        page = 0
        total_fetched = 0
        filtered_count = 0
        
        self.logger.info(f"Fetching trades for token: {token_id}")
        
        while page < max_pages:
            data = self._get("/trades", **params)
            if not data:
                break
            
            total_fetched += len(data)
            
            # CRITICAL FIX: Filter trades to only include the requested token
            # The API returns all trades, so we must filter client-side
            filtered_data = [
                trade for trade in data 
                if trade.get("asset") == token_id
            ]
            
            filtered_count += len(filtered_data)
            
            if filtered_data:
                df = pd.DataFrame(filtered_data)
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                dfs.append(df)
            
            # Log filtering stats
            if len(data) != len(filtered_data):
                self.logger.warning(
                    f"API returned {len(data)} trades but only {len(filtered_data)} "
                    f"matched token {token_id}"
                )
            
            # Check if we should continue paginating
            if len(data) < limit:
                break
            
            # If we got no matching trades in this batch but API returned data,
            # continue paginating to find matching trades
            if filtered_data:
                # Paginate backwards using the last matching trade
                last_ts = filtered_data[-1]["timestamp"]
            else:
                # Use the last trade from unfiltered data to continue pagination
                last_ts = data[-1]["timestamp"]
            
            params["endTime"] = last_ts - 1
            page += 1
            self.rate_limit_wait()
        
        self.logger.info(
            f"Trade fetch complete: {total_fetched} total trades fetched, "
            f"{filtered_count} matched token {token_id}"
        )
        
        if not dfs:
            self.logger.warning(f"No trades found for token {token_id}")
            return pd.DataFrame()
        
        result = pd.concat(dfs).set_index("timestamp")
        
        # Additional validation: ensure all trades have the correct asset
        unique_assets = result["asset"].unique() if "asset" in result.columns else []
        if len(unique_assets) > 1:
            self.logger.error(
                f"Multiple assets found in filtered trades: {unique_assets}"
            )
        elif len(unique_assets) == 1 and unique_assets[0] != token_id:
            self.logger.error(
                f"Wrong asset in trades: expected {token_id}, got {unique_assets[0]}"
            )
        
        return result