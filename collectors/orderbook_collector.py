"""Order book data collection module with FIXED token handling."""

from typing import Dict, Tuple
import pandas as pd
from core.clob_client import CLOBClient
from config.settings import Config
from utils.token_utils import parse_clob_token_ids, convert_token_id

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
            # Parse tokens (returns decimal format)
            tok_yes_decimal, tok_no_decimal = parse_clob_token_ids(tid_str)
            
            # Convert to hex for CLOB API
            tok_yes_hex = convert_token_id(tok_yes_decimal, "hex")
            tok_no_hex = convert_token_id(tok_no_decimal, "hex")
            
            if self.logger:
                self.logger.debug(f"YES token: hex={tok_yes_hex}...")
                self.logger.debug(f"NO token: hex={tok_no_hex}...")
                
        except Exception as e:
            if self.logger:
                self.logger.warning("Failed to parse clobTokenIds: %s", e)
            return pd.DataFrame(), pd.DataFrame()
        
        # Try to fetch YES order book with error handling
        df_yes = pd.DataFrame()
        try:
            df_yes = self.clob.fetch_order_book(tok_yes_hex, depth)
            if not df_yes.empty:
                df_yes["outcome"] = "YES"
        except Exception as e:
            if self.logger:
                self.logger.warning("No order book for YES token (might be resolved/inactive): %s", str(e)[:100])
        
        # Try to fetch NO order book with error handling
        df_no = pd.DataFrame()
        try:
            df_no = self.clob.fetch_order_book(tok_no_hex, depth)
            if not df_no.empty:
                df_no["outcome"] = "NO"
        except Exception as e:
            if self.logger:
                self.logger.warning("No order book for NO token (might be resolved/inactive): %s", str(e)[:100])
        
        return df_yes, df_no