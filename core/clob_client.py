"""CLOB API client for prices and order book data."""

import pandas as pd
from typing import Dict, Optional
from core.api_client import BaseAPIClient
from config.settings import Config

class CLOBClient(BaseAPIClient):
    """Client for CLOB API (prices and order book)."""
    
    def __init__(self, logger=None):
        super().__init__(Config.CLOB_BASE, logger)
    
    def fetch_price_history(self, token_id: str,
                           interval: str = "max",
                           fidelity: int = 1) -> pd.DataFrame:
        """Fetch historical price data for a token."""
        data = self._get("/prices-history",
                        market=token_id,
                        interval=interval,
                        fidelity=fidelity)
        
        if not data.get("history"):
            return pd.DataFrame()
        
        df = pd.DataFrame(data["history"])
        df["t"] = pd.to_datetime(df["t"], unit="s", utc=True)
        df.set_index("t", inplace=True)
        return df.rename(columns={"p": "price"})
    
    def fetch_order_book(self, token_id: str, 
                        depth: int = Config.DEFAULT_BOOK_DEPTH) -> pd.DataFrame:
        """Fetch current order book snapshot."""
        print(f"DEBUG fetch_order_book: token_id length={len(token_id)}, value={token_id}")
        ob = self._get("/book", token_id=token_id)
        
        rows = []
        t = pd.to_datetime(ob["timestamp"], unit="ms", utc=True)
        
        for side, ladder in [("bid", ob.get("bids", [])),
                            ("ask", ob.get("asks", []))]:
            for level, entry in enumerate(ladder[:depth], 1):
                rows.append({
                    "timestamp": t,
                    "side": side,
                    "level": level,
                    "price": entry["price"],
                    "size": entry["size"]
                })
        
        return pd.DataFrame(rows).set_index("timestamp")
