"""File storage module for collected data."""

import os
import json
import pandas as pd
from typing import Dict, Optional
from utils.logger import setup_logger

class FileWriter:
    """Handles writing data to files."""
    
    def __init__(self, output_dir: str, logger=None):
        self.output_dir = output_dir
        self.logger = logger or setup_logger()
    
    def write_prices(self, parent_slug: str, market_slug: str,
                    df: pd.DataFrame) -> str:
        """Write price data to CSV."""
        if df is None or df.empty:
            return None
        
        filepath = os.path.join(self.output_dir,
                               f"{parent_slug}-{market_slug}-prices.csv")
        df.to_csv(filepath, index_label="timestamp_utc")
        self.logger.info("  ✓ prices → %s (%d rows)", filepath, len(df))
        return filepath
    
    def write_trades(self, parent_slug: str, market_slug: str,
                    df: pd.DataFrame, outcome: str) -> str:
        """Write trade data to CSV."""
        if df.empty:
            return None
        
        filepath = os.path.join(self.output_dir,
                               f"{parent_slug}-{market_slug}-trades_{outcome.lower()}.csv")
        df.to_csv(filepath, index_label="timestamp_utc")
        self.logger.info("  ✓ trades %s → %s (%d rows)", outcome, filepath, len(df))
        return filepath
    
    def write_orderbook(self, parent_slug: str, market_slug: str,
                       df: pd.DataFrame, outcome: str) -> str:
        """Write order book data to CSV."""
        if df.empty:
            return None
        
        filepath = os.path.join(self.output_dir,
                               f"{parent_slug}-{market_slug}-orderbook_{outcome.lower()}.csv")
        df.to_csv(filepath, index_label="timestamp_utc")
        self.logger.info("  ✓ book %s → %s (%d rows)", outcome, filepath, len(df))
        return filepath
    
    def write_metadata(self, parent_slug: str, market_slug: str,
                      market: Dict) -> str:
        """Write market metadata to JSON."""
        filepath = os.path.join(self.output_dir,
                               f"{parent_slug}-{market_slug}-metadata.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(market, f, indent=2)
        self.logger.info("  ✓ metadata → %s", filepath)
        return filepath