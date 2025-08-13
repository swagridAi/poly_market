"""Price data collection module with FIXED token handling."""

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
            # CRITICAL FIX: Parse as string, not JSON
            # The clobTokenIds might be in different formats:
            # 1. JSON array: '["token1","token2"]'
            # 2. Simple string: '"token1","token2"'
            # 3. Already parsed array (from some API responses)
            
            if isinstance(tid_str, list):
                # Already parsed
                tok_yes, tok_no = tid_str[0], tid_str[1]
            elif tid_str.startswith('['):
                # JSON array format
                tokens = json.loads(tid_str)
                tok_yes, tok_no = tokens[0], tokens[1]
            else:
                # Simple comma-separated format
                cleaned = tid_str.strip('[]"')
                tokens = [t.strip('" ') for t in cleaned.split(',')]
                tok_yes, tok_no = tokens[0], tokens[1]
                
            if self.logger:
                self.logger.debug(f"Parsed tokens - YES: {tok_yes[:50]}..., NO: {tok_no[:50]}...")
                
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