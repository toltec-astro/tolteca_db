"""Partition definitions for TolTEC data processing.

Simple 1D dynamic partitioning by quartet identifier.
Time-based partitioning for association generation.
"""

from __future__ import annotations

import re

from dagster import DailyPartitionsDefinition, DynamicPartitionsDefinition, MultiPartitionsDefinition, StaticPartitionsDefinition

__all__ = [
    "quartet_partitions",
    "quartet_interface_partitions",
    "daily_partitions",
    "tags_for_partition_fn",
    "TOLTEC_INTERFACES",
    "get_interface_roach_index",
    "get_array_name_for_interface",
    "validate_partition_key",
]

# TolTEC has 13 roach boards (toltec0 through toltec12)
TOLTEC_INTERFACES: list[str] = [f"toltec{i}" for i in range(13)]

# Array assignment by roach index
_ARRAY_FOR_ROACH: dict[int, str] = {
    **{i: "a1100" for i in range(7)},   # toltec0–6 → 1.1mm array
    **{i: "a1400" for i in range(7, 11)},  # toltec7–10 → 1.4mm array
    **{i: "a2000" for i in range(11, 13)},  # toltec11–12 → 2.0mm array
}

# 1D Dynamic Partitions: One partition per quartet
# Format: "ics-{obsnum}-{subobsnum}-{scannum}"
# Example: "ics-18846-0-0"
quartet_partitions = DynamicPartitionsDefinition(name="quartet")

# Daily Partitions: For association generation
# Associations span multiple observations, so use time-based partitions
# Start date: Beginning of TolTEC commissioning/science operations
daily_partitions = DailyPartitionsDefinition(start_date="2024-10-01")

# 2D Multi-Partitions: quartet × interface
# Dimension "quartet": dynamic (one key per observation quartet)
# Dimension "quartet_interface": static (toltec0–toltec12)
quartet_interface_partitions = MultiPartitionsDefinition(
    {
        "quartet": quartet_partitions,
        "quartet_interface": StaticPartitionsDefinition(TOLTEC_INTERFACES),
    }
)


def get_interface_roach_index(interface: str) -> int:
    """Extract roach index from interface name like 'toltec5'.

    Parameters
    ----------
    interface : str
        Interface name, e.g. ``"toltec5"``.

    Returns
    -------
    int
        Roach index (0–12).

    Raises
    ------
    ValueError
        If the interface name is invalid or index out of range.
    """
    m = re.fullmatch(r"toltec(\d+)", interface)
    if not m:
        raise ValueError(f"Invalid interface name: {interface!r}")
    idx = int(m.group(1))
    if idx not in _ARRAY_FOR_ROACH:
        raise ValueError(f"Roach index {idx} out of range 0–12")
    return idx


def get_array_name_for_interface(interface: str) -> str:
    """Return the array name (a1100/a1400/a2000) for a given interface.

    Parameters
    ----------
    interface : str
        Interface name, e.g. ``"toltec0"``.

    Returns
    -------
    str
        Array name.

    Raises
    ------
    ValueError
        If the interface name is invalid.
    """
    roach = get_interface_roach_index(interface)
    return _ARRAY_FOR_ROACH[roach]


def validate_partition_key(key: str) -> bool:
    """Check whether *key* matches the quartet partition key format.

    Format: ``"{master}-{obsnum}-{subobsnum}-{scannum}"``
    where master is a non-empty alphanumeric string and the three
    numbers are non-negative integers.

    Parameters
    ----------
    key : str
        Partition key to validate.

    Returns
    -------
    bool
        ``True`` if valid, ``False`` otherwise.
    """
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9]*-\d+-\d+-\d+", key))


def tags_for_partition_fn(partition_key: str) -> dict[str, str]:
    """Generate run tags from a quartet partition key.

    Parses the key and calls :func:`query_obs_timestamp` to obtain
    an observation timestamp, then returns a dict with string tags
    suitable for Dagster run requests.

    Parameters
    ----------
    partition_key : str
        Quartet partition key, e.g. ``"toltec-123456-0-1"``.

    Returns
    -------
    dict[str, str]
        Tags: master, obsnum, subobsnum, scannum, obs_date,
        obs_timestamp, obs_year, obs_month.
    """
    from .helpers import query_obs_timestamp

    parts = partition_key.split("-")
    master, obsnum, subobsnum, scannum = parts[0], parts[1], parts[2], parts[3]

    ts = query_obs_timestamp(master, int(obsnum), int(subobsnum), int(scannum))

    return {
        "master": master,
        "obsnum": obsnum,
        "subobsnum": subobsnum,
        "scannum": scannum,
        "obs_date": ts.strftime("%Y-%m-%d"),
        "obs_timestamp": ts.isoformat(),
        "obs_year": str(ts.year),
        "obs_month": str(ts.month).zfill(2),
    }
