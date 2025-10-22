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
        Database connection URL (e.g., sqlite:///./tolteca_db.sqlite)
    echo : bool, optional
        Whether to echo SQL statements, by default False

    Returns
    -------
    Engine
        SQLAlchemy engine instance

    Examples
    --------
    >>> engine = get_engine("sqlite:///./test.db")
    >>> engine.url.database
    './test.db'
    """
    return create_engine(database_url, echo=echo)


def create_db_and_tables(engine: Engine) -> None:
    """
    Create all database tables.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine instance

    Examples
    --------
    >>> engine = get_engine("sqlite:///:memory:")
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
    >>> engine = get_engine("sqlite:///:memory:")
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
