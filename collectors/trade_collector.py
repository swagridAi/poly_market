"""Trade data collection module with FIXED token handling."""

from typing import Dict, Tuple
import pandas as pd
from core.data_client import DataClient
from utils.token_utils import parse_clob_token_ids

class TradeCollector:
    """Collects trade data for markets with error handling."""
    
    def __init__(self, data_client: DataClient = None, logger=None):
        self.data = data_client or DataClient()
        self.logger = logger
    
    def collect_market_trades(self, market: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Collect YES/NO trade data for a market with error handling."""
        tid_str = market.get("clobTokenIds", "")
        if not tid_str:
            if self.logger:
                self.logger.debug("No clobTokenIds for market %s", market.get("id"))
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            # Parse tokens (returns decimal format)
            tok_yes, tok_no = parse_clob_token_ids(tid_str)
            
            if self.logger:
                self.logger.debug(f"YES token (decimal): {tok_yes}")
                self.logger.debug(f"NO token (decimal): {tok_no}")
                
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to parse clobTokenIds: %s", e)
            return pd.DataFrame(), pd.DataFrame()
        
        # Fetch YES trades with error handling (Data API uses decimal format)
        df_yes = pd.DataFrame()
        try:
            df_yes = self.data.fetch_trades(tok_yes)
            if not df_yes.empty:
                df_yes["outcome"] = "YES"
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to fetch YES trades: %s", str(e)[:100])
        
        # Fetch NO trades with error handling
        df_no = pd.DataFrame()
        try:
            df_no = self.data.fetch_trades(tok_no)
            if not df_no.empty:
                df_no["outcome"] = "NO"
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to fetch NO trades: %s", str(e)[:100])
        
        return df_yes, df_no