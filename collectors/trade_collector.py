"""Trade data collection module."""

import json
from typing import Dict, List, Tuple
import pandas as pd
from core.data_client import DataClient

class TradeCollector:
    """Collects trade data for markets."""
    
    def __init__(self, data_client: DataClient = None):
        self.data = data_client or DataClient()
    
    def collect_market_trades(self, market: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Collect YES/NO trade data for a market."""
        tid_str = market.get("clobTokenIds", "")
        if not tid_str:
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            tok_yes, tok_no = json.loads(tid_str)
        except Exception:
            return pd.DataFrame(), pd.DataFrame()
        
        df_yes = self.data.fetch_trades(tok_yes)
        df_no = self.data.fetch_trades(tok_no)
        
        if not df_yes.empty:
            df_yes["outcome"] = "YES"
        if not df_no.empty:
            df_no["outcome"] = "NO"
        
        return df_yes, df_no
