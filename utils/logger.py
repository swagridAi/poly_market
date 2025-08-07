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