"""Hashing utilities for content addressing."""

from __future__ import annotations

import hashlib
import json

__all__ = ["product_id_hash", "content_hash", "params_hash", "input_set_hash"]

# Try blake3 first, fall back to sha256
try:
    import blake3  # type: ignore[import-untyped]
    HAS_BLAKE3 = True
except ImportError:
    HAS_BLAKE3 = False


def product_id_hash(base_type: str, identity: dict) -> str:
    """
    Generate stable product ID from canonical JSON representation.

    Uses blake3 if available, otherwise sha256. The hash is deterministic:
    same inputs always produce the same hash.

    Parameters
    ----------
    base_type : str
        Product base type (e.g., 'raw_obs', 'reduced_obs')
    identity : dict
        Identity fields uniquely identifying the product

    Returns
    -------
    str
        Hexadecimal hash string (64 characters for blake3, 64 for sha256)

    Examples
    --------
    >>> identity = {"master": "TEL", "obsnum": 12345}
    >>> hash1 = product_id_hash("raw_obs", identity)
    >>> hash2 = product_id_hash("raw_obs", identity)
    >>> hash1 == hash2
    True
    >>> len(hash1)
    64
    """
    canonical = json.dumps(
        {"base_type": base_type, **identity},
        sort_keys=True,
        separators=(',', ':')
    )
    if HAS_BLAKE3:
        return blake3.blake3(canonical.encode()).hexdigest()
    return hashlib.sha256(canonical.encode()).hexdigest()


def content_hash(data: bytes) -> str:
    """
    Compute content hash of file data.

    Uses blake3 if available, otherwise sha256.

    Parameters
    ----------
    data : bytes
        File content to hash

    Returns
    -------
    str
        Hexadecimal hash string prefixed with algorithm (blake3: or sha256:)

    Examples
    --------
    >>> data = b"test content"
    >>> hash_value = content_hash(data)
    >>> hash_value.startswith(("blake3:", "sha256:"))
    True
    """
    if HAS_BLAKE3:
        return f"blake3:{blake3.blake3(data).hexdigest()}"
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def params_hash(params: dict) -> str:
    """
    Compute stable hash of reduction parameters.

    Parameters
    ----------
    params : dict
        Reduction parameters dictionary

    Returns
    -------
    str
        Hexadecimal hash string (first 32 characters)

    Examples
    --------
    >>> params = {"threshold": 5.0, "method": "standard"}
    >>> hash1 = params_hash(params)
    >>> hash2 = params_hash(params)
    >>> hash1 == hash2
    True
    >>> len(hash1)
    32
    """
    canonical = json.dumps(params, sort_keys=True, separators=(',', ':'))
    if HAS_BLAKE3:
        return blake3.blake3(canonical.encode()).hexdigest()[:32]
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


def input_set_hash(product_ids: list[str]) -> str:
    """
    Compute stable hash of input product ID set.

    Parameters
    ----------
    product_ids : list[str]
        List of product IDs (will be sorted for determinism)

    Returns
    -------
    str
        Hexadecimal hash string (first 32 characters)

    Examples
    --------
    >>> ids = ["abc123", "def456", "ghi789"]
    >>> hash1 = input_set_hash(ids)
    >>> hash2 = input_set_hash(ids[::-1])  # reversed
    >>> hash1 == hash2  # order doesn't matter
    True
    >>> len(hash1)
    32
    """
    # Sort for determinism
    canonical = json.dumps(sorted(product_ids), separators=(',', ':'))
    if HAS_BLAKE3:
        return blake3.blake3(canonical.encode()).hexdigest()[:32]
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]
