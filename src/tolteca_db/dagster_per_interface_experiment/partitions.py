"""Partition definitions for Dagster pipeline.

Implements 2D dynamic partitions (quartet × interface) for per-interface validation tracking.
Each TolTEC observation creates 13 database entries (one per interface: toltec0-12).

References
----------
- ADR-011: 2D Partitions for TolTEC Interface Validation Tracking
- TolTEC interface definitions: refs/dpdb_code_snippets/tolteca_v2/src/tolteca_datamodels/toltec/types.py
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

__all__ = [
    "quartet_partitions",
    "quartet_interface_partitions",
    "TOLTEC_INTERFACES",
    "tags_for_partition_fn",
]

from dagster import DynamicPartitionsDefinition, MultiPartitionsDefinition, StaticPartitionsDefinition

# TolTEC interfaces (13 total: toltec0 through toltec12)
# Mapping: toltec0-6 → a1100, toltec7-10 → a1400, toltec11-12 → a2000
TOLTEC_INTERFACES = [
    "toltec0", "toltec1", "toltec2", "toltec3",
    "toltec4", "toltec5", "toltec6", "toltec7",
    "toltec8", "toltec9", "toltec10", "toltec11", "toltec12",
]

# 1D dynamic partitions for quartet-level operations
quartet_partitions = DynamicPartitionsDefinition(name="quartet")

# 2D partitions for per-interface tracking (quartet × quartet_interface)
# Used by: raw_obs_metadata, raw_obs_product
# Partition key format: {"quartet": "toltec-1-123456-0-0", "quartet_interface": "toltec5"}
# Note: Dimension names sorted alphabetically - quartet before quartet_interface
quartet_interface_partitions = MultiPartitionsDefinition({
    "quartet": quartet_partitions,
    "quartet_interface": StaticPartitionsDefinition(TOLTEC_INTERFACES),
})


def tags_for_partition_fn(partition_key: str) -> dict[str, str]:
    """
    Extract metadata from partition key for tag-based filtering.
    
    Enables flexible queries:
    - Master filtering: --tags master=toltec
    - Time filtering: --tags obs_date=2024-10-31
    - Range queries: --tags obs_date>=2024-10-24 obs_date<=2024-10-31
    - Combined: --tags master=toltec obs_date=2024-10-31
    
    Parameters
    ----------
    partition_key : str
        Partition key in format: "master-obsnum-subobsnum-scannum"
    
    Returns
    -------
    dict[str, str]
        Tags dictionary with master, obsnum, obs_date, obs_timestamp, etc.
    
    Examples
    --------
    >>> tags_for_partition_fn("toltec-123456-0-1")
    {
        'master': 'toltec',
        'obsnum': '123456',
        'subobsnum': '0',
        'scannum': '1',
        'obs_date': '2024-10-31',
        'obs_timestamp': '2024-10-31T15:30:00',
        'obs_year': '2024',
        'obs_month': '2024-10',
    }
    
    Notes
    -----
    Queries toltec_db for timestamp information. This function is called
    by Dagster when creating RunRequests with partition keys.
    """
    from tolteca_db.utils.uid import parse_raw_obs_uid
    
    # Parse partition key
    identity = parse_raw_obs_uid(partition_key)
    
    # Query toltec_db for timestamp (using helper function, not @op)
    # This would normally query the external database, but for now we'll
    # use a placeholder until toltec_db connection is configured
    try:
        from tolteca_db.dagster.helpers import query_obs_timestamp
        obs_timestamp = query_obs_timestamp(**identity)
    except (ImportError, Exception):
        # Fallback: use current time if helper not available
        from datetime import datetime, timezone
        obs_timestamp = datetime.now(timezone.utc)
    
    # Build rich tag set
    return {
        "master": identity["master"],
        "obsnum": str(identity["obsnum"]),
        "subobsnum": str(identity["subobsnum"]),
        "scannum": str(identity["scannum"]),
        "obs_date": obs_timestamp.date().isoformat(),
        "obs_timestamp": obs_timestamp.isoformat(),
        "obs_year": str(obs_timestamp.year),
        "obs_month": f"{obs_timestamp.year}-{obs_timestamp.month:02d}",
    }


def validate_partition_key(partition_key: str) -> bool:
    """
    Validate partition key format (1D quartet key).
    
    Parameters
    ----------
    partition_key : str
        Partition key to validate
    
    Returns
    -------
    bool
        True if valid format
    
    Examples
    --------
    >>> validate_partition_key("toltec-1-123456-0-1")
    True
    >>> validate_partition_key("invalid")
    False
    """
    try:
        from tolteca_db.utils.uid import parse_raw_obs_uid
        parse_raw_obs_uid(partition_key)
        return True
    except ValueError:
        return False


def get_interface_roach_index(interface: str) -> int:
    """
    Get RoachIndex from interface name.
    
    Parameters
    ----------
    interface : str
        Interface name (e.g., "toltec5")
    
    Returns
    -------
    int
        RoachIndex (0-12)
    
    Examples
    --------
    >>> get_interface_roach_index("toltec5")
    5
    >>> get_interface_roach_index("toltec12")
    12
    
    Raises
    ------
    ValueError
        If interface name is invalid
    """
    if not interface.startswith("toltec"):
        raise ValueError(f"Invalid interface name: {interface}")
    
    try:
        roach_idx = int(interface[6:])  # Extract number after "toltec"
        if roach_idx < 0 or roach_idx > 12:
            raise ValueError(f"RoachIndex out of range: {roach_idx}")
        return roach_idx
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid interface name: {interface}") from e


def get_array_name_for_interface(interface: str) -> str:
    """
    Get array name for interface.
    
    Based on TolTEC interface to array mapping:
    - toltec0-6 → a1100 (7 interfaces)
    - toltec7-10 → a1400 (4 interfaces)
    - toltec11-12 → a2000 (2 interfaces)
    
    Parameters
    ----------
    interface : str
        Interface name (e.g., "toltec5")
    
    Returns
    -------
    str
        Array name: "a1100", "a1400", or "a2000"
    
    Examples
    --------
    >>> get_array_name_for_interface("toltec5")
    'a1100'
    >>> get_array_name_for_interface("toltec9")
    'a1400'
    >>> get_array_name_for_interface("toltec12")
    'a2000'
    """
    roach_idx = get_interface_roach_index(interface)
    
    if roach_idx <= 6:
        return "a1100"
    elif roach_idx <= 10:
        return "a1400"
    else:  # 11-12
        return "a2000"
