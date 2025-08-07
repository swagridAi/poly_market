
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
