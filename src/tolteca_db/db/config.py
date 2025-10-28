"""Database configuration and session management."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from sqlmodel import Session, SQLModel, create_engine

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
    """
    engine = create_engine(database_url, echo=echo)
    
    # Configure DuckDB for analytical workloads
    if database_url.startswith("duckdb://"):
        from sqlalchemy import event
        
        @event.listens_for(engine, "connect")
        def configure_duckdb(dbapi_conn, connection_record):
            # Set optimal defaults for analytical queries
            dbapi_conn.execute("SET threads TO 4")  # Adjust based on CPU
            dbapi_conn.execute("SET memory_limit = '4GB'")  # Adjust based on RAM
            dbapi_conn.execute("SET temp_directory = 'scratch/duckdb_temp'")
            dbapi_conn.execute("SET enable_object_cache = true")
    
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
    SQLModel.metadata.create_all(engine)


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
