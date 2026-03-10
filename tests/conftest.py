"""pytest configuration for tolteca_db tests."""

from __future__ import annotations

import warnings
from pathlib import Path  # noqa: F401

import pytest

# Suppress Dagster ConfigArgumentWarning globally
try:
    from dagster_shared.utils.warnings import ConfigArgumentWarning

    warnings.filterwarnings("ignore", category=ConfigArgumentWarning)
except ImportError:
    pass

# v3.x ORM is available — import Base directly.
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from tolteca_db.models.orm import Base

_ORM_AVAILABLE = True


@pytest.fixture
def engine():
    """Create in-memory SQLite engine with v3.x schema."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(engine):
    """Create a transactional SQLAlchemy session."""
    with Session(engine) as session:
        yield session
        session.rollback()


@pytest.fixture(scope="session")
def sample_toltec_db_engine():
    """In-memory SQLite database with minimal legacy raw_obs / interface_file schema.

    Used by integration tests that query the OLD toltec_db v1 schema via SQL.
    Tests that need real production data should skip if not available.
    """
    engine = create_engine("sqlite:///:memory:", echo=False)

    with Session(engine) as session:
        session.execute(text("""
            CREATE TABLE master (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL
            )
        """))
        session.execute(text("""
            CREATE TABLE raw_obs (
                id INTEGER PRIMARY KEY,
                master_id INTEGER NOT NULL,
                obsnum INTEGER NOT NULL,
                subobsnum INTEGER NOT NULL,
                scannum INTEGER NOT NULL,
                ut TEXT,
                tel_header TEXT,
                FOREIGN KEY (master_id) REFERENCES master(id)
            )
        """))
        session.execute(text("""
            CREATE TABLE interface_file (
                id INTEGER PRIMARY KEY,
                raw_obs_id INTEGER NOT NULL,
                nw INTEGER NOT NULL,
                valid INTEGER NOT NULL,
                filename TEXT,
                FOREIGN KEY (raw_obs_id) REFERENCES raw_obs(id)
            )
        """))

        session.execute(text("INSERT INTO master (id, label) VALUES (0, 'TCS')"))
        session.execute(text("INSERT INTO master (id, label) VALUES (1, 'TOLTEC')"))
        session.execute(text("INSERT INTO master (id, label) VALUES (2, 'ICS')"))
        session.execute(text("""
            INSERT INTO raw_obs (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 12345, 0, 0, '2024-01-01T00:00:00', '{}')
        """))
        for nw in [0, 6, 12]:
            session.execute(
                text("""
                    INSERT INTO interface_file (id, raw_obs_id, nw, valid, filename)
                    VALUES (:id, 1, :nw, 1, :filename)
                """),
                {"id": nw + 1, "nw": nw, "filename": f"toltec{nw}_12345_0_0.nc"},
            )
        session.commit()

    return engine


@pytest.fixture
def sample_toltec_db_session(sample_toltec_db_engine):
    """Session for legacy raw_obs / interface_file schema."""
    with Session(sample_toltec_db_engine) as session:
        yield session
        session.rollback()



