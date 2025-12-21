"""LMTMC API client for querying telescope metadata.

This module provides functions to query the LMTMC (Large Millimeter Telescope
Metadata Catalog) API for telescope interface metadata in CSV format.

The API endpoint returns CSV data with columns matching the tel_interface_meta table:
- ObsNum, SubObsNum, ScanNum: Observation identifiers
- FileName: Path to telescope telemetry NetCDF file
- Telescope metadata fields (RA, Dec, Az, El, etc.)

API URL Format
--------------
http://187.248.54.232/cgi-bin/lmtmc/mc_sql.cgi?-s=START&-e=END&-format=csv&-instrument=toltec

Parameters:
- -s: Start date (YYYY-MM-DD)
- -e: End date (YYYY-MM-DD)  
- -format: Output format (always 'csv')
- -instrument: Instrument name (always 'toltec')

Caching Strategy
----------------
CSV responses are cached locally to prevent redundant API calls:
- Cache location: ${LMTMC_CACHE_DIR}/lmtmc_{start}_{end}.csv
- Cache key: (start_date, end_date) tuple
- Cache invalidation: Configurable max age (default: 24 hours)

Examples
--------
Query API for single date:

>>> from tolteca_db.ingest.lmtmc_api import query_lmtmc_csv
>>> csv_path = query_lmtmc_csv('2025-10-31', '2025-10-31')
>>> print(f"CSV cached at: {csv_path}")

Query API for date range:

>>> csv_path = query_lmtmc_csv('2025-10-01', '2025-10-31')

Check cached data:

>>> from tolteca_db.ingest.lmtmc_api import get_cached_csv
>>> cached = get_cached_csv('2025-10-31', '2025-10-31')
>>> if cached:
...     print(f"Using cached CSV: {cached}")
... else:
...     csv_path = query_lmtmc_csv('2025-10-31', '2025-10-31')

Force refresh (ignore cache):

>>> csv_path = query_lmtmc_csv('2025-10-31', '2025-10-31', force_refresh=True)
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

__all__ = ["query_lmtmc_csv", "get_cached_csv", "LMTMCAPIError"]


class LMTMCAPIError(Exception):
    """Raised when LMTMC API query fails.

    Examples
    --------
    >>> try:
    ...     csv_path = query_lmtmc_csv('2025-10-31', '2025-10-31')
    ... except LMTMCAPIError as e:
    ...     print(f"API query failed: {e}")
    """

    pass


def _normalize_date(d: str | date) -> str:
    """Normalize date to YYYY-MM-DD string format.

    Parameters
    ----------
    d : str | date
        Date as string (YYYY-MM-DD) or date object

    Returns
    -------
    str
        Date in YYYY-MM-DD format

    Raises
    ------
    ValueError
        If date string format is invalid

    Examples
    --------
    >>> _normalize_date("2025-10-31")
    '2025-10-31'
    >>> _normalize_date(date(2025, 10, 31))
    '2025-10-31'
    """
    if isinstance(d, date):
        return d.isoformat()
    
    # Validate string format
    try:
        datetime.strptime(d, "%Y-%m-%d")
        return d
    except ValueError:
        raise ValueError(f"Invalid date format: {d}. Expected YYYY-MM-DD")


def _get_cache_dir() -> Path:
    """Get LMTMC cache directory from environment.

    Returns
    -------
    Path
        Cache directory path

    Notes
    -----
    Falls back to ${DAGSTER_HOME}/lmtmc_cache if LMTMC_CACHE_DIR not set.
    Creates directory if it doesn't exist.

    Examples
    --------
    >>> cache_dir = _get_cache_dir()
    >>> print(cache_dir)
    PosixPath('/path/to/dagster/lmtmc_cache')
    """
    cache_dir_str = os.getenv("LMTMC_CACHE_DIR")
    if cache_dir_str:
        cache_dir = Path(cache_dir_str)
    else:
        dagster_home = os.getenv("DAGSTER_HOME", ".dagster")
        cache_dir = Path(dagster_home) / "lmtmc_cache"
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _get_api_base_url() -> str:
    """Get LMTMC API base URL from environment.

    Returns
    -------
    str
        API base URL

    Notes
    -----
    Falls back to default URL if LMTMC_API_BASE_URL not set.

    Examples
    --------
    >>> api_url = _get_api_base_url()
    >>> print(api_url)
    'http://187.248.54.232/cgi-bin/lmtmc/mc_sql.cgi'
    """
    return os.getenv(
        "LMTMC_API_BASE_URL",
        "http://187.248.54.232/cgi-bin/lmtmc/mc_sql.cgi",
    )


def _get_cache_path(
    start_date: str,
    end_date: str,
    cache_dir: Path | None = None,
) -> Path:
    """Get cache file path for given date range.

    Parameters
    ----------
    start_date : str
        Start date (YYYY-MM-DD)
    end_date : str
        End date (YYYY-MM-DD)
    cache_dir : Path | None
        Cache directory. Default: from environment

    Returns
    -------
    Path
        Cache file path

    Examples
    --------
    >>> path = _get_cache_path('2025-10-31', '2025-10-31')
    >>> print(path.name)
    'lmtmc_2025-10-31_2025-10-31.csv'
    """
    if cache_dir is None:
        cache_dir = _get_cache_dir()
    
    cache_filename = f"lmtmc_{start_date}_{end_date}.csv"
    return cache_dir / cache_filename


def get_cached_csv(
    start_date: str | date,
    end_date: str | date,
    cache_dir: Path | None = None,
    max_age_hours: float = 24.0,
) -> Path | None:
    """Get cached CSV file if it exists and is recent.

    Parameters
    ----------
    start_date : str | date
        Start date (YYYY-MM-DD)
    end_date : str | date
        End date (YYYY-MM-DD)
    cache_dir : Path | None
        Cache directory. Default: from environment
    max_age_hours : float
        Maximum age of cached file in hours

    Returns
    -------
    Path | None
        Path to cached CSV file, or None if not found/too old

    Examples
    --------
    >>> cached = get_cached_csv('2025-10-31', '2025-10-31')
    >>> if cached:
    ...     print(f"Using cached CSV: {cached}")
    ... else:
    ...     print("No cached CSV found")
    """
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)
    
    cache_path = _get_cache_path(start_date, end_date, cache_dir)
    
    if not cache_path.exists():
        return None
    
    # Check file age
    file_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    age = datetime.now() - file_mtime
    
    if age.total_seconds() > max_age_hours * 3600:
        return None
    
    return cache_path


def query_lmtmc_csv(
    start_date: str | date,
    end_date: str | date,
    cache_dir: Path | None = None,
    api_base_url: str | None = None,
    force_refresh: bool = False,
    timeout: float = 30.0,
) -> Path:
    """Query LMTMC API for telescope metadata CSV.

    Parameters
    ----------
    start_date : str | date
        Start date (YYYY-MM-DD)
    end_date : str | date
        End date (YYYY-MM-DD)
    cache_dir : Path | None
        Cache directory. Default: from environment
    api_base_url : str | None
        API base URL. Default: from environment
    force_refresh : bool
        Force API query even if cached file exists
    timeout : float
        Request timeout in seconds

    Returns
    -------
    Path
        Path to cached CSV file

    Raises
    ------
    LMTMCAPIError
        If API query fails or returns invalid data

    Examples
    --------
    Query for single date:

    >>> csv_path = query_lmtmc_csv('2025-10-31', '2025-10-31')
    >>> print(f"CSV saved to: {csv_path}")

    Query with custom timeout:

    >>> csv_path = query_lmtmc_csv(
    ...     '2025-10-01',
    ...     '2025-10-31',
    ...     timeout=60.0
    ... )

    Force refresh (ignore cache):

    >>> csv_path = query_lmtmc_csv(
    ...     '2025-10-31',
    ...     '2025-10-31',
    ...     force_refresh=True
    ... )
    """
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)
    
    # Check cache if not forcing refresh
    if not force_refresh:
        cached_path = get_cached_csv(start_date, end_date, cache_dir)
        if cached_path:
            return cached_path
    
    # Build API URL
    if api_base_url is None:
        api_base_url = _get_api_base_url()
    
    params = {
        "-s": start_date,
        "-e": end_date,
        "-format": "csv",
        "-instrument": "toltec",
    }
    
    # Query API
    try:
        response = requests.get(
            api_base_url,
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout:
        raise LMTMCAPIError(
            f"API request timed out after {timeout}s. "
            f"URL: {api_base_url}"
        )
    except requests.exceptions.RequestException as e:
        raise LMTMCAPIError(
            f"API request failed: {e}. "
            f"URL: {api_base_url}"
        )
    
    # Validate response
    if not response.text or len(response.text) < 10:
        raise LMTMCAPIError(
            f"API returned empty or invalid response. "
            f"URL: {response.url}"
        )
    
    # Check for HTML error response (API returns HTML on error)
    if response.text.startswith("<"):
        raise LMTMCAPIError(
            f"API returned HTML error page instead of CSV. "
            f"URL: {response.url}"
        )
    
    # Cache response
    cache_path = _get_cache_path(start_date, end_date, cache_dir)
    cache_path.write_text(response.text, encoding="utf-8")
    
    return cache_path
