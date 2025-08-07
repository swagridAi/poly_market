"""Logging utilities for Polymarket modules."""

import logging
import os
from datetime import datetime

def setup_logger(name="polymarket", level=logging.INFO):
    """Create and configure logger instance."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(ch)
    
    return logger

def add_file_handler(logger, log_path):
    """Add file handler to existing logger."""
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
    )
    logger.addHandler(fh)
    return logger

# ============================================
# utils/file_utils.py
# ============================================
"""File and directory management utilities."""

import os
from datetime import datetime

def make_run_dirs(base_dir="runs"):
    """
    Create a unique run folder: runs/YYYYmmdd_HHMMSS/
    with a nested logs/ folder.
    Returns (run_dir, log_path).
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(base_dir, ts)
    log_dir = os.path.join(run_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_path = os.path.join(log_dir, "polymarket.log")
    return run_dir, log_path

def extract_slug_from_url(url: str) -> str:
    """Extract event slug from Polymarket URL."""
    import re
    m = re.search(r"/event/([^/?#]+)", url)
    if not m:
        raise ValueError(f"URL must contain /event/<slug>: {url}")
    return m.group(1).lower()

# ============================================
# core/api_client.py
# ============================================
"""Base API client with common HTTP functionality."""

import requests
import time
from typing import Dict, Any, Optional
from config.settings import Config
from utils.logger import setup_logger

class BaseAPIClient:
    """Base class for API interactions."""
    
    def __init__(self, base_url: str, logger=None):
        self.base_url = base_url
        self.logger = logger or setup_logger()
        self.session = requests.Session()
        
    def _get(self, resource: str, **params) -> Dict[str, Any]:
        """Execute GET request with error handling."""
        url = f"{self.base_url}{resource}"
        self.logger.debug("GET %s params=%s", url, params)
        
        try:
            r = self.session.get(url, params=params, timeout=Config.TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            self.logger.error("HTTP %s – %s", r.status_code, r.text[:200])
            raise
        except Exception as e:
            self.logger.error("Request failed: %s", e)
            raise
    
    def rate_limit_wait(self):
        """Apply rate limiting between requests."""
        time.sleep(Config.RATE_LIMIT_DELAY)

# ============================================
# core/gamma_client.py
# ============================================
"""Gamma API client for market metadata."""

import json
from typing import List, Dict, Optional
from core.api_client import BaseAPIClient
from config.settings import Config

class GammaClient(BaseAPIClient):
    """Client for Gamma API (market metadata)."""
    
    def __init__(self, logger=None):
        super().__init__(Config.GAMMA_BASE, logger)
    
    def get_event_markets(self, slug: str) -> List[Dict]:
        """Fetch markets for an event slug."""
        # Try direct market lookup first
        rows = self._get("/markets", slug=slug, limit=1)
        if rows:
            return [rows[0]]
        
        # Fall back to event lookup
        events = self._get("/events", slug=slug, limit=1)
        if not events:
            raise ValueError(f"No market or event found for slug: {slug}")
        return events[0].get("markets", [])
    
    def get_market_metadata(self, market_id: str) -> Dict:
        """Fetch detailed market metadata."""
        return self._get(f"/markets/{market_id}")

# ============================================
# core/clob_client.py
# ============================================
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
        ob = self._get("/book", market=token_id)
        
        rows = []
        t = pd.to_datetime(ob["timestamp"], unit="s", utc=True)
        
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

# ============================================
# core/data_client.py
# ============================================
"""Data API client for trade history."""

import pandas as pd
import time
from typing import Optional
from core.api_client import BaseAPIClient
from config.settings import Config

class DataClient(BaseAPIClient):
    """Client for Data API (trades)."""
    
    def __init__(self, logger=None):
        super().__init__(Config.DATA_BASE, logger)
    
    def fetch_trades(self, token_id: str,
                    start: Optional[int] = None,
                    end: Optional[int] = None,
                    limit: int = Config.DEFAULT_TRADE_LIMIT,
                    max_pages: int = Config.MAX_TRADE_PAGES) -> pd.DataFrame:
        """Fetch trade history with pagination."""
        params = {"asset": token_id, "limit": limit}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        
        dfs = []
        page = 0
        
        while page < max_pages:
            data = self._get("/trades", **params)
            if not data:
                break
            
            df = pd.DataFrame(data)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            dfs.append(df)
            
            if len(data) < limit:
                break
            
            # Paginate backwards
            last_ts = data[-1]["timestamp"]
            params["endTime"] = last_ts - 1
            page += 1
            self.rate_limit_wait()
        
        return pd.concat(dfs).set_index("timestamp") if dfs else pd.DataFrame()

# ============================================
# collectors/price_collector.py
# ============================================
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

# ============================================
# collectors/trade_collector.py
# ============================================
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

# ============================================
# collectors/orderbook_collector.py
# ============================================
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

# ============================================
# storage/file_writer.py
# ============================================
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

# ============================================
# cli.py - NEW MAIN ENTRY POINT
# ============================================
"""Main CLI interface for Polymarket data collection."""

import os
import sys
import pandas as pd
from typing import List

from config.settings import Config
from core.gamma_client import GammaClient
from core.clob_client import CLOBClient
from core.data_client import DataClient
from collectors.price_collector import PriceCollector
from collectors.trade_collector import TradeCollector
from collectors.orderbook_collector import OrderBookCollector
from storage.file_writer import FileWriter
from utils.logger import setup_logger, add_file_handler
from utils.file_utils import make_run_dirs, extract_slug_from_url

class PolymarketCLI:
    """Main CLI application."""
    
    def __init__(self):
        self.logger = setup_logger()
        
        # Initialize API clients
        self.gamma = GammaClient(self.logger)
        self.clob = CLOBClient(self.logger)
        self.data = DataClient(self.logger)
        
        # Initialize collectors
        self.price_collector = PriceCollector(self.clob)
        self.trade_collector = TradeCollector(self.data)
        self.orderbook_collector = OrderBookCollector(self.clob)
    
    def process_market(self, parent_slug: str, market: Dict, writer: FileWriter,
                      interval: str, fidelity: int,
                      want_trades: bool, want_book: bool):
        """Process a single market."""
        mslug = market.get("slug", f"market-{market.get('id','unknown')}")
        self.logger.info("• %s", market.get("question", mslug))
        
        # Collect prices
        df_prices = self.price_collector.collect_market_prices(market, interval, fidelity)
        writer.write_prices(parent_slug, mslug, df_prices)
        
        # Collect trades
        if want_trades:
            df_yes, df_no = self.trade_collector.collect_market_trades(market)
            if not df_yes.empty:
                writer.write_trades(parent_slug, mslug, df_yes, "YES")
            if not df_no.empty:
                writer.write_trades(parent_slug, mslug, df_no, "NO")
        
        # Collect order book
        if want_book:
            ob_yes, ob_no = self.orderbook_collector.collect_market_orderbook(market)
            if not ob_yes.empty:
                writer.write_orderbook(parent_slug, mslug, ob_yes, "YES")
            if not ob_no.empty:
                writer.write_orderbook(parent_slug, mslug, ob_no, "NO")
        
        # Write metadata
        writer.write_metadata(parent_slug, mslug, market)
    
    def run(self, urls: List[str], **options):
        """Main execution method."""
        # Setup run directory
        run_dir, log_path = make_run_dirs()
        add_file_handler(self.logger, log_path)
        
        # Log configuration
        self.logger.info("=" * 80)
        self.logger.info("Polymarket batch fetcher started")
        self.logger.info("URLs      : %d", len(urls))
        self.logger.info("interval  : %s | fidelity: %s",
                        options.get('interval', Config.DEFAULT_INTERVAL),
                        options.get('fidelity', Config.DEFAULT_FIDELITY))
        self.logger.info("trades    : %s | book: %s",
                        options.get('want_trades', False),
                        options.get('want_book', False))
        self.logger.info("batch dir : %s", run_dir)
        self.logger.info("=" * 80)
        
        # Process each URL
        for n, url in enumerate(urls, 1):
            self.logger.info("----- [%d / %d] %s", n, len(urls), url)
            try:
                slug = extract_slug_from_url(url)
                markets = self.gamma.get_event_markets(slug)
                
                if not markets:
                    raise RuntimeError("No markets returned")
                
                # Create event subdirectory
                event_dir = os.path.join(run_dir, slug)
                os.makedirs(event_dir, exist_ok=True)
                writer = FileWriter(event_dir, self.logger)
                
                # Process each market
                for i, market in enumerate(markets, 1):
                    self.logger.info("Market %d / %d", i, len(markets))
                    self.process_market(parent_slug=slug,
                                      market=market,
                                      writer=writer,
                                      **options)
                    
            except Exception as e:
                self.logger.exception("Fatal error for %s: %s", url, e)
        
        self.logger.info("Batch complete. Logs → %s", log_path)
        print(f"All done. Outputs in: {run_dir}")
        print(f"Run log saved to: {log_path}")

def main():
    """Entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Polymarket Data Collector")
    parser.add_argument("input", help="Input CSV file or URL")
    parser.add_argument("--interval", default=Config.DEFAULT_INTERVAL)
    parser.add_argument("--fidelity", type=int, default=Config.DEFAULT_FIDELITY)
    parser.add_argument("--trades", action="store_true")
    parser.add_argument("--book", action="store_true")
    parser.add_argument("--debug", action="store_true")
    
    args = parser.parse_args()
    
    # Parse input
    if args.input.endswith('.csv'):
        df = pd.read_csv(args.input)
        urls = df.get("url", df.iloc[:, 0]).dropna().tolist()
    else:
        urls = [args.input]
    
    # Configure logging level
    if args.debug:
        import logging
        logging.getLogger("polymarket").setLevel(logging.DEBUG)
    
    # Run CLI
    cli = PolymarketCLI()
    cli.run(urls,
           interval=args.interval,
           fidelity=args.fidelity,
           want_trades=args.trades,
           want_book=args.book)

if __name__ == "__main__":
    main()

# ============================================
# legacy_cli.py - BACKWARD COMPATIBILITY
# ============================================
"""
Wrapper to maintain backward compatibility with using_clob_api.py
Usage: python legacy_cli.py [same arguments as before]
"""

import sys
from cli import main

if __name__ == "__main__":
    # This allows the old command to still work:
    # python using_clob_api.py input.csv --trades --book
    main()