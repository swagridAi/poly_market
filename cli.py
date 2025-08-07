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