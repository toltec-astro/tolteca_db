"""Database configuration and session management.

.. deprecated::
   This module is deprecated. Use :class:`~tolteca_db.db.Database` instead.

   The Database class provides better support for:
   - SQLite/PostgreSQL metadata with concurrent writes
   - DuckDB Parquet queries with multiprocess safety
   - Cross-database joins (metadata + Parquet)
   - Integrated ParquetQuery helper

   Migration example::

       # Old way
       from tolteca_db.db import get_engine, create_db_and_tables, get_session
       engine = get_engine("duckdb:///tolteca.duckdb")
       create_db_and_tables(engine)
       with get_session(engine) as session:
           ...

       # New way
       from tolteca_db.db import create_database
       db = create_database("duckdb:///tolteca.duckdb")
       db.create_tables()
       with db.session() as session:
           ...
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session
from sqlalchemy import create_engine

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.engine import Engine

__all__ = ["create_db_and_tables", "get_engine", "get_session"]


def get_engine(database_url: str, echo: bool = False) -> Engine:
    """
    Create database engine.

    Parameters
    ----------
    database_url : str
        Database connection URL
        Examples:
        - In-memory: "duckdb:///:memory:"
        - File: "duckdb:///./tolteca.duckdb"
        - Absolute path: "duckdb:////absolute/path/tolteca.duckdb"
    echo : bool, optional
        Whether to echo SQL statements, by default False

    Returns
    -------
    Engine
        SQLAlchemy engine instance

    Examples
    --------
    >>> engine = get_engine("duckdb:///./tolteca.duckdb")
    >>> # Configure DuckDB settings
    >>> with engine.connect() as conn:
    ...     conn.execute("SET threads TO 4")
    ...     conn.execute("SET memory_limit = '4GB'")

    Notes
    -----
    DuckDB is optimized for OLAP workloads with columnar-vectorized
    execution (10-100x faster than SQLite for analytical queries).

    Connection pooling is disabled (pool_pre_ping=True, poolclass=NullPool)
    to prevent lock conflicts in DuckDB when used with Dagster resources.
    """
    # Configure connection args for DuckDB
    connect_args = {}
    if database_url.startswith("duckdb://"):
        # DuckDB-specific connection settings to reduce lock contention
        connect_args = {
            "read_only": False,
            "config": {
                "access_mode": "automatic",  # Allow concurrent reads
                "threads": 4,
                "memory_limit": "4GB",
            },
        }

    # Choose pooling strategy based on database type
    from sqlalchemy.pool import NullPool, StaticPool

    # For in-memory DuckDB, use StaticPool to maintain single connection
    # (each new connection to :memory: creates separate database!)
    # For file-based, use NullPool to avoid lock conflicts
    if database_url.startswith("duckdb:///:memory"):
        poolclass = StaticPool
    else:
        poolclass = NullPool

    engine = create_engine(
        database_url,
        echo=echo,
        connect_args=connect_args,
        poolclass=poolclass,
        pool_pre_ping=True,  # Verify connections before use
    )

    # Configure DuckDB for analytical workloads
    if database_url.startswith("duckdb://"):
        from sqlalchemy import event

        @event.listens_for(engine, "connect")
        def configure_duckdb(dbapi_conn, connection_record):
            # Set optimal defaults for analytical queries
            # Note: threads and memory_limit already set in connect_args
            dbapi_conn.execute("SET temp_directory = 'scratch/duckdb_temp'")
            dbapi_conn.execute("SET enable_object_cache = true")
            # Improve write performance
            dbapi_conn.execute("SET checkpoint_threshold = '1GB'")

    return engine


def create_db_and_tables(engine: Engine) -> None:
    """
    Create all database tables.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine instance

    Examples
    --------
    >>> engine = get_engine("duckdb:///:memory:")
    >>> create_db_and_tables(engine)
    """
    # Import Base first
    from tolteca_db.models.orm.base import Base

    # Explicitly import ALL models to register them with Base.metadata
    # This is critical - just importing the orm package isn't enough
    from tolteca_db.models.orm import (  # noqa: F401
        DataKind,
        DataProd,
        DataProdAssoc,
        DataProdDataKind,
        DataProdType,
        DataProdFlag,
        DataProdSource,
        EventLog,
        Flag,
        Location,
        ReductionTask,
        TaskInput,
        TaskOutput,
    )

    # Create all tables directly (DuckDB in-memory doesn't need explicit transaction)
    Base.metadata.create_all(engine)


@contextmanager
def get_session(engine: Engine) -> Generator[Session, None, None]:
    """
    Provide transactional database session.

    Automatically commits on success, rolls back on exception.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine instance

    Yields
    ------
    Session
        SQLModel session

    Examples
    --------
    >>> engine = get_engine("duckdb:///:memory:")
    >>> with get_session(engine) as session:
    ...     # Perform database operations
    ...     pass  # Auto-committed
    """
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
