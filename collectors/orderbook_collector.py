"""Order book data collection module."""

import json
from typing import Dict, Tuple
import pandas as pd
from core.clob_client import CLOBClient
from config.settings import Config

class OrderBookCollector:
    """Collects order book snapshots."""
    
    def __init__(self, clob_client: CLOBClient = None):
        self.clob = clob_client or CLOBClient()
    
    def collect_market_orderbook(self, market: Dict,
                                depth: int = Config.DEFAULT_BOOK_DEPTH) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Collect YES/NO order book data for a market."""
        tid_str = market.get("clobTokenIds", "")
        if not tid_str:
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            tok_yes, tok_no = json.loads(tid_str)
        except Exception:
            return pd.DataFrame(), pd.DataFrame()
        
        df_yes = self.clob.fetch_order_book(tok_yes, depth)
        df_no = self.clob.fetch_order_book(tok_no, depth)
        
        if not df_yes.empty:
            df_yes["outcome"] = "YES"
        if not df_no.empty:
            df_no["outcome"] = "NO"
        
        return df_yes, df_no

