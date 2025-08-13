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
    print(f"DEBUG convert_token_id: input={token_id[:30]}..., to_format={to_format}")
    token_id = str(token_id).strip()
    
    if to_format == "hex":
        if token_id.startswith("0x"):
            # Already hex - ensure it's lowercase and properly padded
            hex_value = token_id.lower()
            # Remove 0x prefix for padding calculation
            hex_digits = hex_value[2:]
            # Pad to 64 characters (256 bits / 4 bits per hex digit)
            hex_digits = hex_digits.zfill(64)
            print(f"DEBUG convert_token_id: output={"0x" + hex_digits}")
            return "0x" + hex_digits
        else:
            # Convert decimal to hex
            # These are 256-bit numbers, handle as strings
            try:
                # Convert decimal string to integer, then to hex
                decimal_int = int(token_id)
                # Format as hex without 0x prefix
                hex_digits = format(decimal_int, '064x')  # 064x = 64 chars, lowercase hex
                print(f"DEBUG convert_token_id: output={"0x" + hex_digits}")
                return "0x" + hex_digits
            except ValueError as e:
                # If conversion fails, log error and return as-is
                print(f"Warning: Failed to convert token ID to hex: {e}")
                return token_id
    
    elif to_format == "decimal":
        if token_id.startswith("0x"):
            # Convert hex to decimal
            try:
                decimal_value = str(int(token_id, 16))
                print(f"DEBUG convert_token_id: output={decimal_value}")
                return decimal_value
            except ValueError as e:
                print(f"Warning: Failed to convert token ID to decimal: {e}")
                return token_id
        else:
            # Already decimal
            print(f"DEBUG convert_token_id: output={token_id}")
            return token_id
    print(f"DEBUG convert_token_id: output={token_id}")
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
    
    print(f"DEBUG: Raw tokens before conversion: {tokens}")
    
    tok_yes = str(tokens[0]).strip()
    tok_no = str(tokens[1]).strip()
    
    print(f"DEBUG: tok_yes before: {tok_yes}")
    
    if tok_yes.startswith('0x'):
        tok_yes = str(int(tok_yes, 16))
        print(f"DEBUG: tok_yes after conversion: {tok_yes}")
    if tok_no.startswith('0x'):
        tok_no = str(int(tok_no, 16))
        print(f"DEBUG: tok_no after conversion: {tok_no}")
    
    return tok_yes, tok_no


# Optional: Add a validation function to check token format
def validate_hex_token(token_id: str) -> bool:
    """
    Validate that a hex token ID is properly formatted.
    
    Args:
        token_id: Token ID to validate
        
    Returns:
        True if valid hex token ID format
    """
    if not token_id.startswith("0x"):
        return False
    
    hex_part = token_id[2:]
    
    # Should be 64 hex characters for a 256-bit number
    if len(hex_part) != 64:
        return False
    
    # Check if all characters are valid hex
    try:
        int(hex_part, 16)
        return True
    except ValueError:
        return False


# Optional: Test function to verify conversions
def test_token_conversion():
    """Test token ID conversions with sample values."""
    # Test cases
    test_cases = [
        # Short decimal that was causing issues
        ("720831724410737641305294174414", "hex"),
        # Already hex (short)
        ("0xfefc3529459a46d28d7", "hex"),
        # Full-length decimal example
        ("115792089237316195423570985008687907853269984665640564039457584007913129639935", "hex"),
    ]
    
    print("Token Conversion Tests:")
    print("-" * 80)
    
    for token, target_format in test_cases:
        result = convert_token_id(token, target_format)
        print(f"Input:  {token[:50]}...")
        print(f"Output: {result}")
        if target_format == "hex":
            print(f"Length: {len(result) - 2} hex digits (should be 64)")
            print(f"Valid:  {validate_hex_token(result)}")
        print("-" * 40)


if __name__ == "__main__":
    # Run tests if executed directly
    test_token_conversion()