"""UID generation utilities for TolTEC data products.

This module provides functions to generate and parse unique identifiers for
TolTEC data products according to the conventions established in ADR-009.

References
----------
ADR-009: TolTEC Data Product Naming Conventions
"""

from __future__ import annotations

import re
from typing import TypedDict

__all__ = [
    "RawObsIdentity",
    "make_cal_group_uid",
    "make_group_uid",
    "make_raw_obs_uid",
    "make_reduced_obs_uid",
    "parse_raw_obs_uid",
]


class RawObsIdentity(TypedDict):
    """Identity components parsed from a raw observation UID.

    Attributes
    ----------
    master : str
        Master identifier (e.g., 'toltec', 'tcs')
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number
    """

    master: str
    obsnum: int
    subobsnum: int
    scannum: int


def make_raw_obs_uid(master: str, obsnum: int, subobsnum: int, scannum: int) -> str:
    """
    Generate UID for raw observation data product (dp_raw_obs).

    Parameters
    ----------
    master : str
        Master identifier (e.g., 'toltec', 'tcs')
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number

    Returns
    -------
    str
        Unique identifier in format: "{master}-{obsnum}-{subobsnum}-{scannum}"

    Examples
    --------
    >>> make_raw_obs_uid('toltec', 123456, 0, 1)
    'toltec-123456-0-1'
    """
    return f"{master}-{obsnum}-{subobsnum}-{scannum}"


def make_reduced_obs_uid(
    master: str, obsnum: int, subobsnum: int, scannum: int,
) -> str:
    """
    Generate UID for reduced observation data product (dp_reduced_obs).

    Parameters
    ----------
    master : str
        Master identifier (e.g., 'toltec', 'tcs')
    obsnum : int
        Observation number
    subobsnum : int
        Sub-observation number
    scannum : int
        Scan number

    Returns
    -------
    str
        Unique identifier in format: "{master}-{obsnum}-{subobsnum}-{scannum}-reduced"

    Examples
    --------
    >>> make_reduced_obs_uid('toltec', 123456, 0, 1)
    'toltec-123456-0-1-reduced'

    Notes
    -----
    The "-reduced" suffix distinguishes reduced observations from their raw counterparts
    while maintaining the same identification scheme.
    """
    return f"{master}-{obsnum}-{subobsnum}-{scannum}-reduced"


def make_cal_group_uid(master: str, obsnum: int, n_items: int) -> str:
    """
    Generate UID for calibration group data product (dp_cal_group).

    Parameters
    ----------
    master : str
        Master identifier (e.g., 'toltec', 'tcs')
    obsnum : int
        Primary observation number identifying the calibration set
    n_items : int
        Number of items in the calibration group

    Returns
    -------
    str
        Unique identifier in format: "{master}-{obsnum}-g{n_items}-cal"

    Examples
    --------
    >>> make_cal_group_uid('toltec', 123456, 5)
    'toltec-123456-g5-cal'

    Notes
    -----
    The "g{n}" component indicates group size, while "-cal" indicates calibration purpose.
    """
    return f"{master}-{obsnum}-g{n_items}-cal"


def make_group_uid(master: str, obsnum: int, n_items: int, suffix: str) -> str:
    """
    Generate UID for generic group data products.

    Parameters
    ----------
    master : str
        Master identifier (e.g., 'toltec', 'tcs')
    obsnum : int
        Primary observation number
    n_items : int
        Number of items in the group
    suffix : str
        Type suffix (e.g., 'drivefit', 'focus', 'map')

    Returns
    -------
    str
        Unique identifier in format: "{master}-{obsnum}-g{n_items}-{suffix}"

    Examples
    --------
    >>> make_group_uid('toltec', 123456, 3, 'drivefit')
    'toltec-123456-g3-drivefit'
    >>> make_group_uid('toltec', 123457, 10, 'focus')
    'toltec-123457-g10-focus'

    Notes
    -----
    This is the general form used by make_cal_group_uid and other group-based products.
    """
    return f"{master}-{obsnum}-g{n_items}-{suffix}"


def parse_raw_obs_uid(uid: str) -> RawObsIdentity:
    """
    Parse raw observation UID into its component parts.

    Parameters
    ----------
    uid : str
        Raw observation UID to parse

    Returns
    -------
    RawObsIdentity
        Dictionary with 'master', 'obsnum', 'subobsnum', 'scannum' keys

    Raises
    ------
    ValueError
        If UID format is invalid or components cannot be parsed

    Examples
    --------
    >>> parse_raw_obs_uid('toltec-123456-0-1')
    {'master': 'toltec', 'obsnum': 123456, 'subobsnum': 0, 'scannum': 1}

    Notes
    -----
    This parser validates the UID format and converts numeric components to integers.
    Works for both raw and reduced UIDs (strips "-reduced" suffix if present).
    """
    # Remove "-reduced" suffix if present
    uid_clean = uid.removesuffix("-reduced")

    # Pattern: master-obsnum-subobsnum-scannum
    pattern = r"^([a-z_]+)-(\d+)-(\d+)-(\d+)$"
    match = re.match(pattern, uid_clean)

    if not match:
        raise ValueError(
            f"Invalid raw observation UID format: {uid}. "
            f"Expected format: {{master}}-{{obsnum}}-{{subobsnum}}-{{scannum}}",
        )

    master, obsnum_str, subobsnum_str, scannum_str = match.groups()

    try:
        return RawObsIdentity(
            master=master,
            obsnum=int(obsnum_str),
            subobsnum=int(subobsnum_str),
            scannum=int(scannum_str),
        )
    except ValueError as e:
        raise ValueError(f"Failed to parse UID components: {e}") from e
