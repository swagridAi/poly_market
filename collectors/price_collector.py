"""Price data collection module."""

import json
import pandas as pd
from typing import Dict, Optional
from core.clob_client import CLOBClient

class PriceCollector:
    """Collects price data for markets with error handling."""
    
    def __init__(self, clob_client: CLOBClient = None, logger=None):
        self.clob = clob_client or CLOBClient()
        self.logger = logger
    
    def collect_market_prices(self, market: Dict,
                            interval: str = "max",
                            fidelity: int = 1) -> Optional[pd.DataFrame]:
        """Collect YES/NO price data for a market with error handling."""
        tid_str = market.get("clobTokenIds", "")
        if not tid_str:
            if self.logger:
                self.logger.debug("No clobTokenIds for market %s", market.get("id"))
            return None
        
        try:
            tok_yes, tok_no = json.loads(tid_str)
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to parse clobTokenIds: %s", e)
            return None
        
        # Fetch price histories with error handling
        df_yes = pd.DataFrame()
        df_no = pd.DataFrame()
        
        try:
            df_yes = self.clob.fetch_price_history(tok_yes, interval, fidelity)
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to fetch YES prices: %s", str(e)[:100])
        
        try:
            df_no = self.clob.fetch_price_history(tok_no, interval, fidelity)
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to fetch NO prices: %s", str(e)[:100])
        
        if df_yes.empty and df_no.empty:
            return None
        
        # Join available data
        if not df_yes.empty and not df_no.empty:
            df = df_yes.join(df_no, how="outer", rsuffix="_no")
            df.columns = ["price_yes", "price_no"]
        elif not df_yes.empty:
            df = df_yes
            df.columns = ["price_yes"]
        else:
            df = df_no
            df.columns = ["price_no"]
        
        return df.sort_index()
