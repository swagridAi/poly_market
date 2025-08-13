"""Price data collection module with FIXED token handling."""

import pandas as pd
from typing import Dict, Optional
from core.clob_client import CLOBClient
from utils.token_utils import parse_clob_token_ids, convert_token_id

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
            # Parse tokens (returns decimal format)
            tok_yes_decimal, tok_no_decimal = parse_clob_token_ids(tid_str)
            
            # Convert to hex for CLOB API
            tok_yes_hex = convert_token_id(tok_yes_decimal, "hex")
            tok_no_hex = convert_token_id(tok_no_decimal, "hex")
            
            if self.logger:
                self.logger.debug(f"YES token: decimal={tok_yes_decimal[:20]}..., hex={tok_yes_hex[:20]}...")
                self.logger.debug(f"NO token: decimal={tok_no_decimal[:20]}..., hex={tok_no_hex[:20]}...")
                
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to parse clobTokenIds: %s", e)
            return None
        
        # Fetch price histories with error handling
        df_yes = pd.DataFrame()
        df_no = pd.DataFrame()
        
        try:
            df_yes = self.clob.fetch_price_history(tok_yes_hex, interval, fidelity)
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to fetch YES prices: %s", str(e)[:100])
        
        try:
            df_no = self.clob.fetch_price_history(tok_no_hex, interval, fidelity)
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