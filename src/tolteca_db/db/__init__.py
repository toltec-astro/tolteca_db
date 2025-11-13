"""Database package for tolteca_db."""

from __future__ import annotations

__all__ = [
    # Primary API - use these
    "Database",
    "DuckDBDatabase",
    "SQLiteDatabase",
    "PostgreSQLDatabase",
    "create_database",
    # Repositories
    "BaseRepository",
    "DataProductRepository",
    "FlagDefinitionRepository",
    "DataProductFlagRepository",
    # Parquet queries
    "ParquetQuery",
    "resolve_source_path",
    # Registry
    "populate_registry_tables",
    # Legacy/backward compatibility (deprecated - use Database API instead)
    "HybridDatabase",
    "create_hybrid_database",
    "create_db_and_tables",
    "get_engine",
    "get_session",
]

from .config import create_db_and_tables, get_engine, get_session
from .database import (
    Database,
    DuckDBDatabase,
    SQLiteDatabase,
    PostgreSQLDatabase,
    create_database,
    HybridDatabase,
    create_hybrid_database,
)
from .parquet import ParquetQuery, resolve_source_path
from .registry import populate_registry_tables
from .repository import (
    BaseRepository,
    DataProductFlagRepository,
    DataProductRepository,
    FlagDefinitionRepository,
)
