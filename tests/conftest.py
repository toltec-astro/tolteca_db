"""pytest configuration for tolteca_db tests."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from tolteca_db.models.orm import Base


@pytest.fixture
def engine():
    """Create in-memory DuckDB engine for testing."""
    engine = create_engine("duckdb:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def session(engine):
    """Create SQLAlchemy session for testing."""
    with Session(engine) as session:
        yield session
        session.rollback()


