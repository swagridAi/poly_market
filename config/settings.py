"""Configuration settings for Polymarket API client."""

class Config:
    """Centralized configuration."""
    
    # API Endpoints
    GAMMA_BASE = "https://gamma-api.polymarket.com"  # metadata
    CLOB_BASE = "https://clob.polymarket.com"       # prices & book
    DATA_BASE = "https://data-api.polymarket.com"    # trades
    
    # Request Settings
    TIMEOUT = 30  # seconds per HTTP call
    MAX_TRADE_PAGES = 50  # safety cap (50*1000 rows)
    
    # Rate Limiting
    RATE_LIMIT_DELAY = 0.25  # seconds between requests
    
    # Defaults
    DEFAULT_INTERVAL = "max"
    DEFAULT_FIDELITY = 1
    DEFAULT_BOOK_DEPTH = 20
    DEFAULT_TRADE_LIMIT = 1000

