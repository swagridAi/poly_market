from __future__ import annotations
import logging, re, sys, json, os, time
from datetime import datetime
from typing   import Dict, List, Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
GAMMA_BASE = "https://gamma-api.polymarket.com"     # metadata
CLOB_BASE  = "https://clob.polymarket.com"          # prices & book
DATA_BASE  = "https://data-api.polymarket.com"      # trades
TIMEOUT    = 30                                     # seconds per HTTP call
MAX_TRADE_PAGES = 50                                # safety cap (50*1 000 rows)

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logger = logging.getLogger("polymarket")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(ch)

# ---------------------------------------------------------------------------
# OUTPUT DIRECTORIES
# ---------------------------------------------------------------------------

def make_run_dirs() -> tuple[str, str]:
    """
    Create a unique run folder:   runs/YYYYmmdd_HHMMSS/
    with a nested logs/ folder.   Returns (run_dir, log_path).
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join("runs", ts)
    log_dir = os.path.join(run_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, "polymarket.log")
    return run_dir, log_path

# ---------------------------------------------------------------------------
# INTERNAL HELPERS
# ---------------------------------------------------------------------------
def _get(base: str, resource: str, **params):
    url = f"{base}{resource}"
    logger.debug("GET %s params=%s", url, params)
    r = requests.get(url, params=params, timeout=TIMEOUT)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        logger.error("HTTP %s – %s", r.status_code, r.text[:200])
        raise
    return r.json()

_gamma_get = lambda r, **p: _get(GAMMA_BASE, r, **p)
_clob_get  = lambda r, **p: _get(CLOB_BASE,  r, **p)
_data_get  = lambda r, **p: _get(DATA_BASE,  r, **p)

# ---------------------------------------------------------------------------
# CORE FETCHERS
# ---------------------------------------------------------------------------
def extract_slug(url: str) -> str:
    m = re.search(r"/event/([^/?#]+)", url)
    if not m:
        raise ValueError("URL must contain /event/<slug>")
    return m.group(1).lower()

def get_event_markets(slug: str) -> List[Dict]:
    rows = _gamma_get("/markets", slug=slug, limit=1)
    if rows:
        return [rows[0]]

    events = _gamma_get("/events", slug=slug, limit=1)
    if not events:
        raise ValueError(f"No market or event found for slug: {slug}")
    return events[0].get("markets", [])

# ---------- Prices ---------------------------------------------------------
def fetch_token_price_history(token_id: str,
                              interval: str = "max",
                              fidelity: int = 1) -> pd.DataFrame:
    data = _clob_get("/prices-history", market=token_id,
                     interval=interval, fidelity=fidelity)
    if not data.get("history"):
        return pd.DataFrame()
    df = pd.DataFrame(data["history"])
    df["t"] = pd.to_datetime(df["t"], unit="s", utc=True)
    df.set_index("t", inplace=True)
    return df.rename(columns={"p": "price"})

def fetch_market_prices(market: Dict,
                        interval="max", fidelity=1) -> Optional[pd.DataFrame]:
    tid_str = market.get("clobTokenIds", "")
    if not tid_str:
        return None
    tok_yes, tok_no = json.loads(tid_str)
    df_yes = fetch_token_price_history(tok_yes, interval, fidelity)
    df_no  = fetch_token_price_history(tok_no,  interval, fidelity)
    if df_yes.empty and df_no.empty:
        return None
    df = df_yes.join(df_no, how="outer", rsuffix="_no")
    df.columns = ["price_yes", "price_no"]
    return df.sort_index()

# ---------- Trades ---------------------------------------------------------
def fetch_trades(token_id: str,
                 start: int | None = None,
                 end:   int | None = None,
                 limit: int = 1_000,
                 max_pages: int = MAX_TRADE_PAGES) -> pd.DataFrame:
    params = {"asset": token_id, "limit": limit}
    if start: params["startTime"] = start
    if end:   params["endTime"]   = end

    dfs, page = [], 0
    while page < max_pages:
        data = _data_get("/trades", **params)
        if not data:
            break
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        dfs.append(df)
        if len(data) < limit:
            break
        last_ts = data[-1]["timestamp"]
        params["endTime"] = last_ts - 1   # paginate backwards
        page += 1
        time.sleep(0.25)                  # be polite
    return pd.concat(dfs).set_index("timestamp") if dfs else pd.DataFrame()

# ---------- Order‑book snapshot -------------------------------------------
def fetch_order_book(token_id: str, depth: int = 20) -> pd.DataFrame:
    ob = _clob_get("/book", market=token_id)
    rows = []
    t = pd.to_datetime(ob["timestamp"], unit="s", utc=True)
    for side, ladder in (("bid", ob.get("bids", [])),
                         ("ask", ob.get("asks", []))):
        for level, entry in enumerate(ladder[:depth], 1):
            rows.append({"timestamp": t, "side": side,
                         "level": level,
                         "price": entry["price"],
                         "size":  entry["size"]})
    return pd.DataFrame(rows).set_index("timestamp")

# ---------------------------------------------------------------------------
# CSV / PARQUET BUILDER
# ---------------------------------------------------------------------------
def write_market_files(parent_slug: str, market: Dict, outdir: str,
                       interval="max", fidelity=1, 
                       want_trades=False, want_book=False):
    mslug = market.get("slug", f"market-{market.get('id','unknown')}")
    logger.info("• %s", market.get("question", mslug))

    # token ids
    try:
        tok_yes, tok_no = json.loads(market["clobTokenIds"])
    except Exception:
        logger.error("Missing or mal‑formed clobTokenIds for %s", mslug)
        tok_yes = tok_no = None

    # ------- prices
    df_price = fetch_market_prices(market, interval, fidelity)
    if df_price is not None:
        pfile = os.path.join(outdir, f"{parent_slug}-{mslug}-prices.csv")
        df_price.to_csv(pfile, index_label="timestamp_utc")
        logger.info("  ✓ prices → %s (%d rows)", pfile, len(df_price))
    else:
        logger.warning("  ! no price data")

    # ------- trades
    if want_trades and tok_yes and tok_no:
        for tok, lab in ((tok_yes, "YES"), (tok_no, "NO")):
            df_tr = fetch_trades(tok)
            if not df_tr.empty:
                df_tr["outcome"] = lab
                tfile = os.path.join(outdir, f"{parent_slug}-{mslug}-trades_{lab.lower()}.csv")
                df_tr.to_csv(tfile, index_label="timestamp_utc")
                logger.info("  ✓ trades %s → %s (%d rows)", lab, tfile, len(df_tr))

    # ------- order‑book
    if want_book and tok_yes and tok_no:
        for tok, lab in ((tok_yes, "YES"), (tok_no, "NO")):
            df_ob = fetch_order_book(tok)
            if not df_ob.empty:
                df_ob["outcome"] = lab
                bfile = os.path.join(outdir, f"{parent_slug}-{mslug}-orderbook_{lab.lower()}.csv")
                df_ob.to_csv(bfile, index_label="timestamp_utc")
                logger.info("  ✓ book   %s → %s (%d rows)", lab, bfile, len(df_ob))

    # ------- metadata
    meta_path = os.path.join(outdir, f"{parent_slug}-{mslug}-metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(market, f, indent=2)
    logger.info("  ✓ metadata → %s", meta_path)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main(argv: List[str]):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 1️⃣  Parse top‑level switches
    # ------------------------------------------------------------------ #
    interval     = "max"
    fidelity     = 1
    want_trades  = "--trades" in argv
    want_book    = "--book"   in argv
    input_csv    = None
    urls: List[str] = []

    for arg in argv[1:]:
        if arg.startswith("--interval="):
            interval = arg.split("=", 1)[1]
        elif arg.startswith("--fidelity="):
            fidelity = int(arg.split("=", 1)[1])
        elif arg.lower().endswith(".csv"):
            input_csv = arg
        elif not arg.startswith("--"):
            urls.append(arg)

    # If the user didn’t supply URLs but an input CSV exists, read it
    if input_csv and not urls:
        df_in = pd.read_csv(input_csv)
        # Expect a column called ‘url’; otherwise treat each cell as the URL
        urls = df_in.get("url", df_in.iloc[:, 0]).dropna().tolist()

    # Fallback: If the first positional argument is literally “input”
    # we assume ./input.csv
    if urls == ["input"]:
        df_in = pd.read_csv("input.csv")
        urls = df_in.get("url", df_in.iloc[:, 0]).dropna().tolist()

    if not urls:
        logger.error("No URLs provided – nothing to do.")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 2️⃣  Prep per‑run folders and logging
    # ------------------------------------------------------------------ #
    run_dir, log_path = make_run_dirs()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s"))
    logger.addHandler(fh)

    logger.info("=" * 80)
    logger.info("Polymarket batch fetcher started")
    logger.info("URLs      : %d", len(urls))
    logger.info("interval  : %s | fidelity: %s", interval, fidelity)
    logger.info("trades    : %s | book: %s", want_trades, want_book)
    logger.info("batch dir : %s", run_dir)
    logger.info("=" * 80)

    # ------------------------------------------------------------------ #
    # 3️⃣  Process each URL in turn
    # ------------------------------------------------------------------ #
    for n, url in enumerate(urls, 1):
        logger.info("----- [%d / %d] %s", n, len(urls), url)
        try:
            slug = extract_slug(url)
            markets = get_event_markets(slug)
            if not markets:
                raise RuntimeError("No markets returned")
            # create a subfolder per event to keep files tidy
            event_dir = os.path.join(run_dir, slug)
            os.makedirs(event_dir, exist_ok=True)

            for i, m in enumerate(markets, 1):
                logger.info("Market %d / %d", i, len(markets))
                write_market_files(slug, m, event_dir,
                                   interval, fidelity,
                                   want_trades, want_book)
        except Exception as e:
            logger.exception("Fatal error for %s: %s", url, e)

    # ------------------------------------------------------------------ #
    logger.info("Batch complete.  Logs → %s", log_path)
    print(f"All done.  Outputs in: {run_dir}")
    print(f"Run log saved to     : {log_path}")

if __name__ == "__main__":
    main(sys.argv)
