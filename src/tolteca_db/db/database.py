"""Database module: SQLAlchemy ORM + DuckDB Parquet queries.

This module provides the standard database pattern for tolteca_db:
1. SQLAlchemy ORM for metadata writes (supports SQLite, PostgreSQL, DuckDB)
2. DuckDB for high-performance Parquet queries
3. Cross-database joins (metadata tables + Parquet files)
4. Dagster-friendly multiprocess execution

Architecture:
- Metadata Engine: SQLAlchemy for ORM operations (concurrent writes)
- Query Engine: DuckDB for Parquet queries (concurrent reads)
- Both engines can be the same database (DuckDB file) or different

Use create_database() factory function to automatically select the right
implementation based on your database URL.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from tolteca_db.db.parquet import ParquetQuery

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.engine import Engine

__all__ = [
    "Database",
    "DuckDBDatabase", 
    "SQLiteDatabase",
    "PostgreSQLDatabase",
    "create_database",
    # Backward compatibility
    "HybridDatabase",
    "create_hybrid_database",
]


class Database(ABC):
    """
    Base class for tolteca_db database (metadata + Parquet analytics).

    Provides unified interface for:
    1. Metadata engine (SQLAlchemy) - for ORM operations
    2. Query engine (DuckDB) - for Parquet queries

    Subclasses implement dialect-specific optimizations for:
    - DuckDB: Single database for both metadata and queries
    - SQLite: SQLite for metadata, DuckDB for Parquet
    - PostgreSQL: PostgreSQL for metadata, DuckDB for Parquet

    Use create_database() factory function instead of instantiating directly.

    Parameters
    ----------
    metadata_url : str
        Metadata database URL (SQLite, PostgreSQL, or DuckDB)
    read_only_queries : bool, optional
        Open query engine in READ_ONLY mode for multiprocess safety, by default True
    echo : bool, optional
        Echo SQL statements, by default False

    Attributes
    ----------
    metadata_engine : Engine
        SQLAlchemy engine for metadata operations
    query_con : duckdb.DuckDBPyConnection
        DuckDB connection for Parquet queries
    metadata_dialect : str
        Metadata database dialect name ("sqlite", "postgresql", "duckdb")
    parquet : ParquetQuery
        Helper for querying Parquet files
    """

    def __init__(
        self,
        metadata_url: str,
        read_only_queries: bool = True,
        echo: bool = False,
    ):
        # Create metadata engine with dialect-specific configuration
        self.metadata_engine = self._create_metadata_engine(metadata_url, echo)
        self.metadata_dialect = self.metadata_engine.dialect.name
        self._echo = echo
        self._read_only_queries = read_only_queries

        # Create DuckDB connection for queries (lazily for some implementations)
        self._query_con = None  # Will be created on first access

        # Create ParquetQuery helper (initialized lazily)
        self._parquet_query = None

    @property
    def query_con(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB query connection (lazy initialization)."""
        if self._query_con is None:
            self._query_con = self._create_query_connection()
        return self._query_con

    @abstractmethod
    def _create_metadata_engine(self, url: str, echo: bool) -> Engine:
        """Create metadata engine with dialect-specific optimizations."""
        pass

    @abstractmethod
    def _create_query_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB connection for Parquet queries."""
        pass

    @abstractmethod
    def _configure_metadata_engine(self, engine: Engine) -> None:
        """Configure engine with dialect-specific settings."""
        pass

    @property
    def parquet(self) -> ParquetQuery:
        """
        ParquetQuery helper for querying Parquet files.

        Returns
        -------
        ParquetQuery
            Helper for Parquet queries
        """
        if self._parquet_query is None:
            session = Session(self.metadata_engine)
            self._parquet_query = ParquetQuery(self.query_con, session)
        return self._parquet_query

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Provide transactional database session for metadata operations.

        Yields
        ------
        Session
            SQLAlchemy session for ORM operations
        """
        session = Session(self.metadata_engine)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def parquet_session(self) -> Generator[tuple[Session, ParquetQuery], None, None]:
        """
        Provide session with ParquetQuery helper.

        Yields
        ------
        tuple[Session, ParquetQuery]
            Session for metadata, ParquetQuery for data
        """
        session = Session(self.metadata_engine)
        pq = ParquetQuery(self.query_con, session)
        try:
            yield session, pq
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_raw(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Execute raw SQL query on DuckDB.

        Parameters
        ----------
        query : str
            SQL query to execute

        Returns
        -------
        duckdb.DuckDBPyRelation
            Query result relation
        """
        return self.query_con.execute(query)

    def create_tables(self) -> None:
        """Create all ORM tables in metadata database."""
        from tolteca_db.models.orm.base import Base

        # Import all models to register them
        from tolteca_db.models.orm import (  # noqa: F401
            DataKind,
            DataProd,
            DataProdAssoc,
            DataProdDataKind,
            DataProdFlag,
            DataProdSource,
            DataProdType,
            EventLog,
            Flag,
            Location,
            ReductionTask,
            TaskInput,
            TaskOutput,
        )

        # Apply dialect-specific DDL modifications
        self._apply_dialect_constraints(Base)

        # Create tables
        Base.metadata.create_all(self.metadata_engine)

    def _apply_dialect_constraints(self, base: type) -> None:
        """Apply dialect-specific constraints to table definitions."""
        dialect = self.metadata_dialect

        if dialect == "sqlite":
            # Remove partial index constraints for SQLite
            for table in base.metadata.tables.values():
                for idx in list(table.indexes):
                    if (
                        hasattr(idx, "dialect_options")
                        and "postgresql_where" in idx.dialect_options
                    ):
                        table.indexes.remove(idx)
                        from sqlalchemy import Index

                        new_idx = Index(
                            idx.name,
                            *idx.columns,
                            unique=idx.unique,
                        )
                        table.append_constraint(new_idx)

    def close(self) -> None:
        """Close all connections."""
        try:
            if self._query_con:
                self._query_con.close()
        except Exception:
            pass
        if self.metadata_engine:
            self.metadata_engine.dispose()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class DuckDBDatabase(Database):
    """
    Database implementation using single DuckDB for both metadata and queries.

    Best for:
    - Local development and testing
    - Single-process workloads
    - Simple setup without external databases
    
    Note: Uses the SQLAlchemy engine's connection for both metadata and queries
    to avoid connection conflicts with DuckDB's single-file limitation.
    """

    def _create_metadata_engine(self, url: str, echo: bool) -> Engine:
        """Create DuckDB metadata engine with StaticPool."""
        from sqlalchemy.pool import StaticPool

        # Use StaticPool for shared connection
        connect_args = {
            "read_only": False,
            "config": {
                "access_mode": "automatic",
                "threads": 4,
                "memory_limit": "4GB",
            },
        }

        engine = create_engine(
            url,
            echo=echo,
            connect_args=connect_args,
            poolclass=StaticPool,  # Share single connection
            pool_pre_ping=True,
        )

        self._configure_metadata_engine(engine)
        return engine

    def _configure_metadata_engine(self, engine: Engine) -> None:
        """Configure DuckDB metadata engine."""

        @event.listens_for(engine, "connect")
        def configure_duckdb(dbapi_conn, connection_record):
            dbapi_conn.execute("SET temp_directory = 'scratch/duckdb_temp'")
            dbapi_conn.execute("SET enable_object_cache = true")
            dbapi_conn.execute("SET checkpoint_threshold = '1GB'")

    def _create_query_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get DuckDB query connection from SQLAlchemy pool.

        For DuckDB, we reuse the connection from SQLAlchemy's pool
        to avoid "different configuration" errors.
        """
        # Get raw connection from SQLAlchemy
        raw_conn = self.metadata_engine.raw_connection()
        # The dbapi_connection is the DuckDB connection wrapped by duckdb_engine
        # We need to unwrap it to get the actual DuckDB connection
        dbapi_conn = raw_conn.dbapi_connection

        # duckdb_engine wraps the connection - unwrap it
        if hasattr(dbapi_conn, "_DuckDBEngineConnection__c"):
            # Access private attribute (ConnectionWrapper)
            return dbapi_conn._DuckDBEngineConnection__c
        elif hasattr(dbapi_conn, "connection"):
            return dbapi_conn.connection
        else:
            # Fallback: assume it's already a DuckDB connection
            return dbapi_conn


class SQLiteDatabase(Database):
    """
    Database implementation using SQLite for metadata + DuckDB for queries.

    Best for:
    - Production ingestion with concurrent writes
    - Dagster multiprocess execution
    - Need for ACID guarantees with concurrent access
    """

    def _create_metadata_engine(self, url: str, echo: bool) -> Engine:
        """Create SQLite metadata engine with WAL mode."""
        from sqlalchemy.pool import NullPool

        connect_args = {"check_same_thread": False}

        engine = create_engine(
            url,
            echo=echo,
            connect_args=connect_args,
            poolclass=NullPool,
            pool_pre_ping=True,
        )

        self._configure_metadata_engine(engine)
        return engine

    def _configure_metadata_engine(self, engine: Engine) -> None:
        """Configure SQLite with WAL mode and optimizations."""

        @event.listens_for(engine, "connect")
        def configure_sqlite(dbapi_conn, connection_record):
            dbapi_conn.execute("PRAGMA journal_mode=WAL")
            dbapi_conn.execute("PRAGMA synchronous=NORMAL")
            dbapi_conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            dbapi_conn.execute("PRAGMA temp_store=MEMORY")
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

    def _create_query_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB query connection with SQLite attachment."""
        config = {
            "threads": 4,
            "memory_limit": "4GB",
            "temp_directory": "scratch/duckdb_temp",
        }

        # Use in-memory DuckDB and attach SQLite database
        con = duckdb.connect(":memory:", read_only=False, config=config)

        # Install and load SQLite extension
        try:
            con.execute("INSTALL sqlite")
            con.execute("LOAD sqlite")
        except Exception:
            pass  # Extension may already be installed

        # Attach SQLite database
        sqlite_path = self.metadata_engine.url.database
        try:
            con.execute(
                f"ATTACH '{sqlite_path}' AS metadata (TYPE sqlite, READ_ONLY {self._read_only_queries})"
            )
        except Exception:
            pass  # May fail if file doesn't exist yet

        return con


class PostgreSQLDatabase(Database):
    """
    Database implementation using PostgreSQL for metadata + DuckDB for queries.

    Best for:
    - Production deployments with high concurrency
    - Distributed systems
    - Need for advanced PostgreSQL features
    """

    def _create_metadata_engine(self, url: str, echo: bool) -> Engine:
        """Create PostgreSQL metadata engine."""
        engine = create_engine(
            url,
            echo=echo,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

        self._configure_metadata_engine(engine)
        return engine

    def _configure_metadata_engine(self, engine: Engine) -> None:
        """Configure PostgreSQL connection settings."""
        # PostgreSQL doesn't need connection-level configuration like SQLite
        pass

    def _create_query_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB query connection with PostgreSQL attachment."""
        config = {
            "threads": 4,
            "memory_limit": "4GB",
            "temp_directory": "scratch/duckdb_temp",
        }

        # Use in-memory DuckDB and attach PostgreSQL
        con = duckdb.connect(":memory:", read_only=False, config=config)

        # Install and load PostgreSQL extension
        try:
            con.execute("INSTALL postgres")
            con.execute("LOAD postgres")
        except Exception:
            pass  # Extension may not be available

        # Attach PostgreSQL database
        postgres_url = str(self.metadata_engine.url)
        try:
            con.execute(
                f"ATTACH '{postgres_url}' AS metadata (TYPE postgres, READ_ONLY {self._read_only_queries})"
            )
        except Exception:
            pass  # May fail if connection unavailable

        return con


def create_database(
    database_url: str,
    read_only: bool = False,
    echo: bool = False,
) -> Database:
    """
    Create database for metadata + Parquet analytics.

    Factory function that returns the appropriate Database subclass
    based on the database URL:
    - DuckDB URL → DuckDBDatabase (single database for metadata + queries)
    - SQLite URL → SQLiteDatabase (SQLite metadata + DuckDB queries)
    - PostgreSQL URL → PostgreSQLDatabase (PostgreSQL metadata + DuckDB queries)

    Use Cases
    ---------
    **Scenario 1: Local Development / Testing (Single Process)**
        Use DuckDB for everything (metadata + queries)
        >>> db = create_database("duckdb:///test.duckdb")

    **Scenario 2: Production Ingestion (Write Mode)**
        Use SQLite/PostgreSQL for metadata writes, DuckDB for Parquet queries
        >>> db = create_database("sqlite:///metadata.db")

    **Scenario 3: Query / Analysis (Read Mode, Multiprocess-Safe)**
        Read-only queries safe for Dagster parallel execution
        >>> db = create_database("sqlite:///metadata.db", read_only=True)

    Parameters
    ----------
    database_url : str
        Database connection URL. Supports:
        - DuckDB: "duckdb:///path/to/file.duckdb" or "duckdb:///:memory:"
        - SQLite: "sqlite:///path/to/file.db"
        - PostgreSQL: "postgresql://user:pass@host/db"
    read_only : bool, optional
        Enable read-only mode for queries (multiprocess-safe), by default False.
        Use True for Dagster query assets that run in parallel.
    echo : bool, optional
        Echo SQL statements for debugging, by default False

    Returns
    -------
    Database
        Configured database instance:
        - DuckDBDatabase for duckdb:// URLs
        - SQLiteDatabase for sqlite:// URLs
        - PostgreSQLDatabase for postgresql:// URLs

    Examples
    --------
    **Local Testing (Single Process)**
    >>> # Quick testing with single DuckDB file
    >>> db = create_database("duckdb:///test.duckdb")
    >>> db.create_tables()
    >>> with db.session() as session:
    ...     # Write metadata
    ...     session.add(DataProd(...))
    >>> # Query Parquet
    >>> df = db.execute_raw("SELECT * FROM 'data/*.parquet'").df()

    **Production Ingestion (Write Mode)**
    >>> # SQLite for concurrent metadata writes
    >>> db = create_database("sqlite:///metadata.db")
    >>> with db.session() as session:
    ...     # Multiple Dagster assets can write metadata concurrently
    ...     session.add(DataProd(...))

    **Query/Analysis (Read Mode)**
    >>> # Read-only queries safe for parallel Dagster assets
    >>> db = create_database("sqlite:///metadata.db", read_only=True)
    >>> # Multiple processes can query simultaneously
    >>> df = db.execute_raw('''
    ...     SELECT dp.label, obs.flux
    ...     FROM data_prod dp
    ...     JOIN 'data/obs_*.parquet' obs ON obs.obs_id = dp.pk
    ... ''').df()

    **Dagster Asset Pattern**
    >>> # Write asset (ingestion)
    >>> @asset
    ... def ingest_data():
    ...     db = create_database("sqlite:///metadata.db")
    ...     with db.session() as session:
    ...         session.add(DataProd(...))
    ...
    >>> # Query asset (analysis) - runs in parallel
    >>> @asset(deps=[ingest_data])
    ... def analyze_data():
    ...     db = create_database("sqlite:///metadata.db", read_only=True)
    ...     df = db.execute_raw("SELECT * FROM 'data/*.parquet'").df()

    Notes
    -----
    - **DuckDB URL**: Returns DuckDBDatabase - single file for both metadata and queries
    - **SQLite URL**: Returns SQLiteDatabase - SQLite for metadata, DuckDB for queries
    - **PostgreSQL URL**: Returns PostgreSQLDatabase - PostgreSQL for metadata, DuckDB for queries
    - **read_only=True**: DuckDB queries in READ_ONLY mode (multiprocess-safe)
    - **Parquet queries**: Always use DuckDB for zero-copy performance
    """
    # Factory pattern: return appropriate subclass based on URL
    if database_url.startswith("duckdb://"):
        return DuckDBDatabase(
            metadata_url=database_url,
            read_only_queries=read_only,
            echo=echo,
        )
    elif database_url.startswith("sqlite://"):
        return SQLiteDatabase(
            metadata_url=database_url,
            read_only_queries=read_only,
            echo=echo,
        )
    elif database_url.startswith("postgresql://"):
        return PostgreSQLDatabase(
            metadata_url=database_url,
            read_only_queries=read_only,
            echo=echo,
        )
    else:
        msg = f"Unsupported database URL: {database_url}"
        raise ValueError(msg)


# Backward compatibility aliases
HybridDatabase = Database
create_hybrid_database = create_database
