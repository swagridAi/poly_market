"""Token ID conversion utilities for Polymarket APIs."""

def convert_token_id(token_id: str, to_format: str = "hex") -> str:
    """
    Convert token ID between decimal and hexadecimal formats.
    
    Args:
        token_id: Token ID in either decimal or hex format
        to_format: Target format ("hex" or "decimal")
    
    Returns:
        Converted token ID string
    """
    # Clean the input
    token_id = str(token_id).strip()
    
    if to_format == "hex":
        if token_id.startswith("0x"):
            # Already hex
            return token_id.lower()
        else:
            # Convert decimal to hex
            # These are 256-bit numbers, handle as strings
            try:
                # Convert decimal string to hex with 0x prefix
                hex_value = hex(int(token_id))
                return hex_value.lower()
            except ValueError:
                # If conversion fails, return as-is
                return token_id
    
    elif to_format == "decimal":
        if token_id.startswith("0x"):
            # Convert hex to decimal
            try:
                decimal_value = str(int(token_id, 16))
                return decimal_value
            except ValueError:
                return token_id
        else:
            # Already decimal
            return token_id
    
    return token_id


def parse_clob_token_ids(tid_str: str) -> tuple[str, str]:
    """
    Parse clobTokenIds from various formats.
    
    Args:
        tid_str: Raw clobTokenIds string from API
        
    Returns:
        Tuple of (yes_token, no_token) as decimal strings
    """
    import json
    
    if not tid_str:
        raise ValueError("Empty clobTokenIds")
    
    # Handle different formats
    if isinstance(tid_str, list):
        # Already parsed
        tokens = tid_str
    elif tid_str.startswith('['):
        # JSON array format
        try:
            tokens = json.loads(tid_str)
        except json.JSONDecodeError:
            # Fallback to string parsing
            cleaned = tid_str.strip('[]"')
            tokens = [t.strip('" ') for t in cleaned.split(',')]
    else:
        # Simple comma-separated format
        cleaned = tid_str.strip('[]"')
        tokens = [t.strip('" ') for t in cleaned.split(',')]
    
    if len(tokens) != 2:
        raise ValueError(f"Expected 2 tokens, got {len(tokens)}: {tokens}")
    
    # Return as decimal strings (the format they come in)
    return str(tokens[0]), str(tokens[1])