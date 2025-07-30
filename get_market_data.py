#!/usr/bin/env python3
"""polymarket_minute_prices.py — v5 (with better error handling and debugging)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pull minute-level price history for Polymarket markets with improved debugging.
"""

from __future__ import annotations

import logging
import re
import sys
import json
import os
from datetime import datetime
from typing import Dict, List, Union, Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# CONFIG & logging
# ---------------------------------------------------------------------------
GAMMA_BASE = "https://gamma-api.polymarket.com"   # metadata + GMP history
CLOB_BASE  = "https://clob.polymarket.com"        # binary order‑book engine
TIMEOUT    = 30                                   # seconds per HTTP call

# Set up logging to both console and file
logger = logging.getLogger("polymarket")
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(console_handler)

# File handler - will be added in main() with timestamp

# ---------------------------------------------------------------------------
# Helpers – generic HTTP wrappers with better debugging
# ---------------------------------------------------------------------------

def _gamma_get(resource: str, **params):
    url = f"{GAMMA_BASE}{resource}"
    logger.debug("GET %s params=%s", url, params)
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        logger.debug("Response: %s", json.dumps(data, indent=2)[:500] + "..." if len(str(data)) > 500 else json.dumps(data, indent=2))
        return data
    except requests.HTTPError as e:
        logger.error("HTTP Error %s: %s", e.response.status_code, e.response.text[:200])
        raise


def _clob_get(path: str):
    url = f"{CLOB_BASE}{path}"
    logger.debug("GET %s", url)
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        logger.debug("Response: %s", json.dumps(data, indent=2)[:500] + "..." if len(str(data)) > 500 else json.dumps(data, indent=2))
        return data
    except requests.HTTPError as e:
        logger.error("HTTP Error %s: %s", e.response.status_code, e.response.text[:200])
        raise

# ---------------------------------------------------------------------------
# Slug extraction
# ---------------------------------------------------------------------------

def extract_slug(url: str) -> str:
    m = re.search(r"/event/([^/?#]+)", url)
    if not m:
        raise ValueError("URL must contain /event/<slug>")
    slug = m.group(1).lower()
    logger.debug("Extracted slug %s", slug)
    return slug

# ---------------------------------------------------------------------------
# Market metadata fetchers with better debugging
# ---------------------------------------------------------------------------

def _fetch_clob_market(condition_id: str) -> Dict:
    """Return rich dict for a *binary* market from CLOB."""
    try:
        data = _clob_get(f"/markets/{condition_id}")
        return data.get("market", data)
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise
        # maybe we were given the row‑ID; translate
        rows = _gamma_get("/markets", ids=condition_id, limit=1)
        if rows:
            true_id = rows[0].get("condition_id") or rows[0].get("id")
            if true_id and str(true_id) != str(condition_id):
                return _fetch_clob_market(str(true_id))
        raise  # propagate original 404


def get_market_meta(slug: str) -> Union[Dict, List[Dict]]:
    """Return binary market dict OR list of sub‑market dicts for an event."""
    # 1️⃣ direct binary lookup
    logger.info("Searching for binary market with slug: %s", slug)
    rows = _gamma_get("/markets", slug=slug, limit=1)
    if rows:
        logger.info("Found binary market")
        return _fetch_clob_market(rows[0]["id"])

    # 2️⃣ event wrapper
    logger.info("Searching for event with slug: %s", slug)
    ev = _gamma_get("/events", slug=slug, limit=1)
    if not ev:
        raise ValueError("Slug not found in /markets or /events")

    logger.info("Found event with %d markets", len(ev[0].get("markets", [])))
    markets: List[Dict] = []
    
    for entry in ev[0].get("markets", []):
        logger.debug("Processing market entry: %s", json.dumps(entry, indent=2))
        
        # Binary sub‑market?  (tokens key available via Gamma row lookup)
        cond = entry.get("condition_id")
        if cond:
            try:
                logger.debug("Attempting to fetch binary market with condition_id: %s", cond)
                markets.append(_fetch_clob_market(str(cond)))
                continue
            except requests.HTTPError:
                logger.debug("Failed to fetch as binary market, treating as GMP")
                pass  # fallthrough to GMP
                
        # Otherwise treat as GMP row (needs only row‑ID)
        market_data = {
            "engine": "gmp",
            "id": str(entry["id"]),
            "slug": entry.get("slug", f"gmp-{entry['id']}"),
            "question": entry.get("question", "Unknown"),
            "_raw": entry  # Store raw data for debugging
        }
        logger.info("Adding GMP market: %s (ID: %s)", market_data["question"], market_data["id"])
        markets.append(market_data)
    
    return markets

# ---------------------------------------------------------------------------
# Data fetchers with better error handling
# ---------------------------------------------------------------------------

def fetch_binary_prices(token_yes: str, token_no: str, fidelity: int = 1) -> pd.DataFrame:
    def _one(tok: str):
        js = _clob_get(f"/prices-history?market={tok}&interval=max&fidelity={fidelity}")
        df = pd.DataFrame(js["history"])
        df["t"] = pd.to_datetime(df["t"], unit="s", utc=True)
        df.set_index("t", inplace=True)
        return df.rename(columns={"p": tok})
    df_yes = _one(token_yes)
    df_no  = _one(token_no)
    out = df_yes.join(df_no, how="outer").sort_index()
    out.columns = [f"price_yes", "price_no"]
    return out


def fetch_gmp_prices(row_id: str, fidelity: int = 1) -> Optional[pd.DataFrame]:
    """Fetch GMP prices with multiple fallback approaches."""
    
    # Try different API endpoints and parameters
    attempts = [
        # Original endpoint
        ("/gmp/market-history", {"marketId": row_id, "resolution": fidelity * 60}),
        # Try without resolution
        ("/gmp/market-history", {"marketId": row_id}),
        # Try different parameter names
        ("/gmp/market-history", {"market_id": row_id, "resolution": fidelity * 60}),
        ("/gmp/market-history", {"id": row_id, "resolution": fidelity * 60}),
        # Try markets endpoint with history flag
        ("/markets/history", {"id": row_id}),
        # Try direct market endpoint
        (f"/markets/{row_id}/history", {}),
    ]
    
    for endpoint, params in attempts:
        try:
            logger.debug("Attempting GMP fetch: %s with params %s", endpoint, params)
            js = _gamma_get(endpoint, **params) if params else _gamma_get(endpoint)
            
            # Handle different response structures
            history = js.get("history") or js.get("data") or js.get("prices") or js
            
            if isinstance(history, list) and history:
                df = pd.DataFrame(history)
                
                # Handle different timestamp column names
                time_col = next((col for col in ["t", "timestamp", "time"] if col in df.columns), None)
                if time_col:
                    df["t"] = pd.to_datetime(df[time_col], unit="s", utc=True)
                    
                # Check if we need to pivot the data
                if "outcome" in df.columns and "p" in df.columns:
                    df = df.pivot(index="t", columns="outcome", values="p")
                elif "price" in df.columns and "outcome" in df.columns:
                    df = df.pivot(index="t", columns="outcome", values="price")
                else:
                    # Already in wide format or single outcome
                    df.set_index("t", inplace=True)
                    
                df.sort_index(inplace=True)
                df = df.add_prefix("price_")
                logger.info("Successfully fetched GMP prices with %d rows", len(df))
                return df
                
        except Exception as e:
            logger.debug("Attempt failed: %s", str(e))
            continue
    
    # If all attempts fail, try to get current price as a fallback
    logger.warning("Could not fetch historical prices for GMP market %s", row_id)
    try:
        # Try to get current market data
        market_data = _gamma_get(f"/markets/{row_id}")
        logger.info("Retrieved current market data instead of history")
        # Create a minimal DataFrame with current data
        return pd.DataFrame()  # Return empty DataFrame for now
    except:
        return None

# ---------------------------------------------------------------------------
# CSV builder with better error handling
# ---------------------------------------------------------------------------

def build_csv(parent_slug: str, market: Dict):
    try:
        if market.get("engine") == "gmp":  # new multi‑outcome engine
            logger.info("Building CSV for GMP market: %s", market.get("question", market["slug"]))
            df = fetch_gmp_prices(market["id"])
            
            if df is None or df.empty:
                logger.warning("No data available for GMP market %s (ID: %s)", market["slug"], market["id"])
                # Save market metadata instead
                meta_path = f"{parent_slug}-{market['slug']}-metadata.json"
                with open(meta_path, 'w') as f:
                    json.dump(market, f, indent=2)
                logger.info("Saved market metadata to %s", meta_path)
                return
                
            out_path = f"{parent_slug}-{market['slug']}-minute-prices.csv"
            df.to_csv(out_path, index_label="timestamp_utc")
            logger.info("✓ GMP CSV %s rows → %s", len(df), out_path)
            return

        # else binary legacy
        if "tokens" not in market:
            logger.error("Binary market missing tokens: %s", json.dumps(market, indent=2))
            return
            
        token_yes, token_no = market["tokens"]
        df = fetch_binary_prices(token_yes["token_id"], token_no["token_id"])
        suffix = f"-{market['slug']}" if parent_slug != market["slug"] else ""
        out_path = f"{parent_slug}{suffix}-minute-prices.csv"
        df.to_csv(out_path, index_label="timestamp_utc")
        logger.info("✓ Binary CSV %s rows → %s", len(df), out_path)
        
    except Exception as e:
        logger.error("Failed to build CSV for market %s: %s", market.get("slug", "unknown"), str(e))
        # Save debug info
        debug_path = f"{parent_slug}-{market.get('slug', 'unknown')}-debug.json"
        with open(debug_path, 'w') as f:
            json.dump({
                "error": str(e),
                "market": market
            }, f, indent=2)
        logger.info("Saved debug info to %s", debug_path)

# ---------------------------------------------------------------------------
# CLI with additional debug options
# ---------------------------------------------------------------------------

def main(argv: List[str]):
    if len(argv) < 2:
        print("Usage: polymarket_minute_prices.py <url> [--debug] [--test-api]", file=sys.stderr)
        sys.exit(1)

    url = argv[1]
    
    # Parse flags
    debug = "--debug" in argv
    test_api = "--test-api" in argv
    
    # Set up file logging with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"polymarket_log_{timestamp}.txt"
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    log_filepath = os.path.join("logs", log_filename)
    
    # Add file handler
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s"))
    logger.addHandler(file_handler)
    
    logger.info("=" * 80)
    logger.info("Polymarket Price Fetcher Started")
    logger.info("URL: %s", url)
    logger.info("Debug mode: %s", debug)
    logger.info("Log file: %s", log_filepath)
    logger.info("=" * 80)
    
    if debug:
        logger.setLevel(logging.DEBUG)
        # Update handlers to use debug level
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
        
    # Test API connectivity if requested
    if test_api:
        logger.info("Testing API connectivity...")
        try:
            # Test Gamma API
            gamma_test = _gamma_get("/markets", limit=1)
            logger.info("✓ Gamma API is accessible")
            
            # Test CLOB API
            clob_test = _clob_get("/markets")
            logger.info("✓ CLOB API is accessible")
        except Exception as e:
            logger.error("API test failed: %s", str(e))
            return

    slug = extract_slug(url)
    logger.info("Processing slug %s", slug)

    try:
        meta = get_market_meta(slug)
        if isinstance(meta, list):
            logger.info("Found %d sub-markets", len(meta))
            for i, m in enumerate(meta):
                logger.info("Processing market %d/%d", i+1, len(meta))
                build_csv(slug, m)
        else:
            build_csv(slug, meta)
    except Exception as e:
        logger.error("Fatal error: %s", str(e))
        if debug:
            import traceback
            error_trace = traceback.format_exc()
            logger.error("Full traceback:\n%s", error_trace)

    logger.info("=" * 80)
    logger.info("Process completed. Log saved to: %s", log_filepath)
    logger.info("Done.")
    
    # Also print log location to console for easy reference
    print(f"\nLog file saved to: {log_filepath}")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main(sys.argv)