"""Price data collection module."""

import json
import pandas as pd
from typing import Dict, Optional
from core.clob_client import CLOBClient

class PriceCollector:
    """Collects price data for markets."""
    
    def __init__(self, clob_client: CLOBClient = None):
        self.clob = clob_client or CLOBClient()
    
    def collect_market_prices(self, market: Dict,
                            interval: str = "max",
                            fidelity: int = 1) -> Optional[pd.DataFrame]:
        """Collect YES/NO price data for a market."""
        tid_str = market.get("clobTokenIds", "")
        if not tid_str:
            return None
        
        try:
            tok_yes, tok_no = json.loads(tid_str)
        except Exception:
            return None
        
        df_yes = self.clob.fetch_price_history(tok_yes, interval, fidelity)
        df_no = self.clob.fetch_price_history(tok_no, interval, fidelity)
        
        if df_yes.empty and df_no.empty:
            return None
        
        df = df_yes.join(df_no, how="outer", rsuffix="_no")
        df.columns = ["price_yes", "price_no"]
        return df.sort_index()

