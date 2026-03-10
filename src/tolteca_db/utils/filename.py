"""Filename parsing utilities for TolTEC data products."""

from __future__ import annotations

import re
from typing import Any


def parse_toltec_filename(filename: str) -> dict[str, Any]:
    """
    Parse TolTEC filename to extract metadata.

    Supports various TolTEC data product filename patterns including:
    - toltec{network}_{obsnum}_{subobsnum}_{scannum}_{interface}_{roachid}.nc
    - ics{network}_{obsnum}_{subobsnum}_{scannum}.txt
    - toltec_timestream_{obsnum}_{subobsnum}_{scannum}.nc

    Parameters
    ----------
    filename : str
        Filename to parse

    Returns
    -------
    dict[str, Any]
        Dictionary with extracted metadata including base_type, obsnum,
        subobsnum, scannum, and additional fields depending on product type

    Raises
    ------
    ValueError
        If filename doesn't match any known TolTEC pattern

    Examples
    --------
    >>> parse_toltec_filename("toltec13_123456_00_0001_toltec_12345.nc")
    {'base_type': 'RAW', 'subtype': 'toltec_raw', 'network': '13', 'obsnum': 123456, ...}

    >>> parse_toltec_filename("ics0_123456_00_0001.txt")
    {'base_type': 'RAW', 'subtype': 'ics_raw', 'network': '0', 'obsnum': 123456, ...}
    """
    # Pattern 1: toltec{network}_{obsnum}_{subobsnum}_{scannum}_{interface}_{roachid}.nc
    pattern1 = re.compile(
        r"toltec(?P<network>\d+)_"
        r"(?P<obsnum>\d+)_"
        r"(?P<subobsnum>\d+)_"
        r"(?P<scannum>\d+)_"
        r"(?P<interface>\w+)_"
        r"(?P<roachid>\d+)"
        r"\.nc$"
    )

    # Pattern 2: ics{network}_{obsnum}_{subobsnum}_{scannum}.txt
    pattern2 = re.compile(
        r"ics(?P<network>\d+)_"
        r"(?P<obsnum>\d+)_"
        r"(?P<subobsnum>\d+)_"
        r"(?P<scannum>\d+)"
        r"\.txt$"
    )

    # Pattern 3: toltec_timestream_{obsnum}_{subobsnum}_{scannum}.nc
    pattern3 = re.compile(
        r"toltec_timestream_"
        r"(?P<obsnum>\d+)_"
        r"(?P<subobsnum>\d+)_"
        r"(?P<scannum>\d+)"
        r"\.nc$"
    )

    # Try matching patterns
    match = pattern1.match(filename)
    if match:
        data = match.groupdict()
        return {
            "base_type": "RAW",
            "subtype": "toltec_raw",
            "network": data["network"],
            "obsnum": int(data["obsnum"]),
            "subobsnum": int(data["subobsnum"]),
            "scannum": int(data["scannum"]),
            "interface": data["interface"],
            "roachid": int(data["roachid"]),
            "status": "RAW",
        }

    match = pattern2.match(filename)
    if match:
        data = match.groupdict()
        return {
            "base_type": "RAW",
            "subtype": "ics_raw",
            "network": data["network"],
            "obsnum": int(data["obsnum"]),
            "subobsnum": int(data["subobsnum"]),
            "scannum": int(data["scannum"]),
            "status": "RAW",
        }

    match = pattern3.match(filename)
    if match:
        data = match.groupdict()
        return {
            "base_type": "TIMESTREAM",
            "subtype": "toltec_timestream",
            "obsnum": int(data["obsnum"]),
            "subobsnum": int(data["subobsnum"]),
            "scannum": int(data["scannum"]),
            "status": "PROCESSED",
        }

    # No match found
    msg = f"Filename '{filename}' doesn't match any known TolTEC pattern"
    raise ValueError(msg)


def build_toltec_filename(metadata: dict[str, Any]) -> str:
    """
    Build TolTEC filename from metadata.

    Parameters
    ----------
    metadata : dict[str, Any]
        Dictionary with metadata fields (base_type, obsnum, subobsnum, etc.)

    Returns
    -------
    str
        Generated filename

    Raises
    ------
    ValueError
        If required metadata fields are missing

    Examples
    --------
    >>> metadata = {
    ...     'subtype': 'toltec_raw',
    ...     'network': '13',
    ...     'obsnum': 123456,
    ...     'subobsnum': 0,
    ...     'scannum': 1,
    ...     'interface': 'toltec',
    ...     'roachid': 12345
    ... }
    >>> build_toltec_filename(metadata)
    'toltec13_123456_00_0001_toltec_12345.nc'
    """
    subtype = metadata.get("subtype")

    if subtype == "toltec_raw":
        return (
            f"toltec{metadata['network']}_"
            f"{metadata['obsnum']:06d}_"
            f"{metadata['subobsnum']:02d}_"
            f"{metadata['scannum']:04d}_"
            f"{metadata['interface']}_"
            f"{metadata['roachid']}.nc"
        )

    if subtype == "ics_raw":
        return (
            f"ics{metadata['network']}_"
            f"{metadata['obsnum']:06d}_"
            f"{metadata['subobsnum']:02d}_"
            f"{metadata['scannum']:04d}.txt"
        )

    if subtype == "toltec_timestream":
        return (
            f"toltec_timestream_"
            f"{metadata['obsnum']:06d}_"
            f"{metadata['subobsnum']:02d}_"
            f"{metadata['scannum']:04d}.nc"
        )

    msg = f"Unknown subtype '{subtype}' for filename generation"
    raise ValueError(msg)
