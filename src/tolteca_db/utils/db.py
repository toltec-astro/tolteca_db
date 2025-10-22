"""Database-specific utilities and configurations."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

__all__ = ["register_sqlite_adapters"]


def _adapt_datetime_iso(val: datetime) -> str:
    """Adapt datetime to ISO 8601 string for SQLite storage.
    
    This adapter converts Python datetime objects to ISO 8601 strings
    for storage in SQLite, avoiding the deprecated default adapter.
    
    Parameters
    ----------
    val : datetime
        Datetime to adapt
        
    Returns
    -------
    str
        ISO 8601 formatted string
    """
    return val.isoformat()


def _convert_datetime(val: bytes) -> datetime:
    """Convert ISO 8601 string from SQLite to datetime.
    
    This converter handles ISO 8601 strings stored in SQLite,
    properly handling timezone information.
    
    Parameters
    ----------
    val : bytes
        ISO 8601 string as bytes
        
    Returns
    -------
    datetime
        Parsed datetime object
    """
    return datetime.fromisoformat(val.decode())


def register_sqlite_adapters() -> None:
    """Register custom datetime adapters for SQLite.
    
    This function registers custom adapters and converters for datetime
    objects to avoid the Python 3.12+ deprecation warning about SQLite's
    default datetime adapter.
    
    The adapters store datetimes as ISO 8601 strings, which is the
    recommended approach for SQLite datetime storage.
    
    Notes
    -----
    This should be called once at application startup, before any
    SQLAlchemy engine is created.
    
    References
    ----------
    - Python sqlite3 docs: https://docs.python.org/3/library/sqlite3.html
    - SQLite date/time: https://www.sqlite.org/lang_datefunc.html
    """
    # Register adapter for datetime -> string (storage)
    sqlite3.register_adapter(datetime, _adapt_datetime_iso)
    
    # Register converter for string -> datetime (retrieval)
    sqlite3.register_converter("timestamp", _convert_datetime)

