"""Order book data collection module with FIXED token handling."""

import json
from typing import Dict, Tuple
import pandas as pd
from core.clob_client import CLOBClient
from config.settings import Config

class OrderBookCollector:
    """Collects order book snapshots with error handling."""
    
    def __init__(self, clob_client: CLOBClient = None, logger=None):
        self.clob = clob_client or CLOBClient()
        self.logger = logger
    
    def collect_market_orderbook(self, market: Dict,
                                depth: int = Config.DEFAULT_BOOK_DEPTH) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Collect YES/NO order book data for a market with error handling."""
        tid_str = market.get("clobTokenIds", "")
        if not tid_str:
            if self.logger:
                self.logger.debug("No clobTokenIds for market %s", market.get("id"))
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            # CRITICAL FIX: Parse as string, not JSON
            # The clobTokenIds is a string like '"token1","token2"'
            cleaned = tid_str.strip('[]"')
            tokens = [t.strip('" ') for t in cleaned.split(',')]
            
            if len(tokens) != 2:
                if self.logger:
                    self.logger.warning(f"Expected 2 tokens, got {len(tokens)}: {tokens}")
                return pd.DataFrame(), pd.DataFrame()
            
            tok_yes, tok_no = tokens[0], tokens[1]
            
            if self.logger:
                self.logger.debug(f"YES token (full): {tok_yes}")
                self.logger.debug(f"NO token (full): {tok_no}")
                
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to parse clobTokenIds: %s", e)
            return pd.DataFrame(), pd.DataFrame()
        
        # Try to fetch YES order book with error handling
        df_yes = pd.DataFrame()
        try:
            df_yes = self.clob.fetch_order_book(tok_yes, depth)
            if not df_yes.empty:
                df_yes["outcome"] = "YES"
        except Exception as e:
            if self.logger:
                self.logger.warning("No order book for YES token (might be resolved/inactive): %s", str(e)[:100])
        
        # Try to fetch NO order book with error handling
        df_no = pd.DataFrame()
        try:
            df_no = self.clob.fetch_order_book(tok_no, depth)
            if not df_no.empty:
                df_no["outcome"] = "NO"
        except Exception as e:
            if self.logger:
                self.logger.warning("No order book for NO token (might be resolved/inactive): %s", str(e)[:100])
        
        return df_yes, df_no