import re
from typing import List, Dict, Any, Optional, Tuple, Set

def normalize_column_name(s: str) -> str:
    """
    Normalizes column names across naming conventions into a canonical space-separated token string.
    Handles:
    - Lowercase: customer_name
    - Uppercase: CUSTOMER_NAME
    - PascalCase: CustomerName
    - camelCase: customerName
    - Spaces: Customer Name
    - Underscores: customer_name
    - Hyphens: customer-name
    - Repeated spaces and separators: customer   name, customer___name
    - Acronyms: CustomerID, customerID, orderURL, IPAddress, XMLParser
    - Leading and trailing whitespace
    """
    if not s:
        return ""
    text = str(s).strip()
    if not text:
        return ""

    # Replace hyphens, underscores, dots, and slashes with spaces
    text = re.sub(r'[\s_\-\.\/]+', ' ', text)

    # Acronym followed by PascalCase word (e.g. 'XMLParser' -> 'XML Parser', 'IPAddress' -> 'IP Address')
    text = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', text)

    # Lowercase/digit followed by Uppercase (e.g. 'customerName' -> 'customer Name', 'customerID' -> 'customer ID')
    text = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', text)

    # Remove non-alphanumeric except space
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)

    # Collapse repeated whitespace and lowercase
    tokens = [t.lower() for t in text.split() if t]
    return " ".join(tokens)

def tokenize_column_name(s: str) -> List[str]:
    """Tokenizes column name into lowercase word tokens."""
    return normalize_column_name(s).split()

# Test normalization
test_cases = [
    ("Customer Name", "customer name"),
    ("CUSTOMER_NAME", "customer name"),
    ("customerName", "customer name"),
    ("CustomerName", "customer name"),
    ("customer-name", "customer name"),
    ("customer___name", "customer name"),
    ("  customer   name  ", "customer name"),
    ("CustomerID", "customer id"),
    ("customerId", "customer id"),
    ("customer_id", "customer id"),
    ("CUSTOMER_ID", "customer id"),
    ("Customer_Phone", "customer phone"),
    ("customerPhone", "customer phone"),
    ("orderURL", "order url"),
    ("IPAddress", "ip address"),
]

for orig, expected in test_cases:
    norm = normalize_column_name(orig)
    assert norm == expected, f"Failed for {orig}: got '{norm}', expected '{expected}'"

print("All normalization tests passed!")
