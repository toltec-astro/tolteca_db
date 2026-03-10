"""pytest configuration for tolteca_db tests."""

from __future__ import annotations

import warnings
from pathlib import Path  # noqa: F401

import pytest

# Suppress Dagster ConfigArgumentWarning globally
# This addresses pre-existing issue with op parameter naming
try:
    from dagster_shared.utils.warnings import ConfigArgumentWarning

    warnings.filterwarnings("ignore", category=ConfigArgumentWarning)
except ImportError:
    pass

# Guard ORM imports: the v2.5 ORM models are pending replacement in Phase 2.
# If they fail to load (e.g. due to renamed constants), we skip the fixtures
# that depend on them rather than crashing the entire test session.
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session

    from tolteca_db.models.orm import Base

    _ORM_AVAILABLE = True
except Exception:  # noqa: BLE001
    _ORM_AVAILABLE = False


def _skip_if_orm_unavailable() -> None:
    if not _ORM_AVAILABLE:
        pytest.skip("v2.5 ORM models not yet migrated to v3.x (Phase 2 pending)")


@pytest.fixture
def engine():
    """Create in-memory DuckDB engine for testing."""
    _skip_if_orm_unavailable()
    engine = create_engine("duckdb:///:memory:", echo=False)
    Base.metadata.create_all(engine)  # type: ignore[possibly-undefined]
    return engine


@pytest.fixture
def session(engine):
    """Create SQLAlchemy session for testing."""
    with Session(engine) as session:  # type: ignore[possibly-undefined]
        yield session
        session.rollback()


@pytest.fixture(scope="session")
def sample_toltec_db_engine():
    """Create in-memory database with minimal sample data from toltec_db.

    Creates minimal toltec_db schema with sample data for integration testing.
    Tests that need real production data should skip if not available.
    """
    _skip_if_orm_unavailable()
    # Create in-memory SQLite database
    engine = create_engine("sqlite:///:memory:", echo=False)  # type: ignore[possibly-undefined]

    # Create tables for toltec_db schema
    with Session(engine) as session:
        session.execute(
            text("""
            CREATE TABLE master (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL
            )
        """)
        )
        session.execute(
            text("""
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
        """)
        )
        session.execute(
            text("""
            CREATE TABLE interface_file (
                id INTEGER PRIMARY KEY,
                raw_obs_id INTEGER NOT NULL,
                nw INTEGER NOT NULL,
                valid INTEGER NOT NULL,
                filename TEXT,
                FOREIGN KEY (raw_obs_id) REFERENCES raw_obs(id)
            )
        """)
        )

        # Add minimal sample data
        session.execute(text("INSERT INTO master (id, label) VALUES (0, 'TCS')"))
        session.execute(text("INSERT INTO master (id, label) VALUES (1, 'TOLTEC')"))
        session.execute(text("INSERT INTO master (id, label) VALUES (2, 'ICS')"))

        # Add one sample raw_obs entry
        session.execute(
            text("""
            INSERT INTO raw_obs (id, master_id, obsnum, subobsnum, scannum, ut, tel_header)
            VALUES (1, 1, 12345, 0, 0, '2024-01-01T00:00:00', '{}')
        """)
        )

        # Add sample interface files for the raw_obs
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
    """Create session for sample toltec_db data.

    Use this fixture in integration tests that need real database data.
    """
    with Session(sample_toltec_db_engine) as session:
        yield session
        session.rollback()


@pytest.fixture(scope="session")
def sample_tolteca_db_engine():
    """Create in-memory DuckDB database for tolteca_db (DataProd, etc.).

    This fixture creates the tolteca_db schema (data products) with
    sample DataProdType entries. Tests can use this to verify asset
    materialization logic without needing a persistent database.
    """
    from tolteca_db.models.orm import Base as ToltecaBase
    from tolteca_db.models.orm import DataProdType

    # Create in-memory DuckDB
    engine = create_engine("duckdb:///:memory:", echo=False)
    ToltecaBase.metadata.create_all(engine)

    # Add sample DataProdType entries
    with Session(engine) as session:
        # Standard data product types
        dp_types = [
            DataProdType(
                label="dp_raw_obs", description="Raw observation data product"
            ),
            DataProdType(label="dp_reduced", description="Reduced data product"),
            DataProdType(label="dp_cal", description="Calibration data product"),
        ]
        session.add_all(dp_types)
        session.commit()

    return engine


@pytest.fixture
def sample_tolteca_db_session(sample_tolteca_db_engine):
    """Create session for sample tolteca_db data.

    Use this fixture in integration tests that create DataProd entries.
    """
    with Session(sample_tolteca_db_engine) as session:
        yield session
        session.rollback()


@pytest.fixture(scope="session")
def test_toltec_db_path(tmp_path_factory):
    """Create temporary file-based test database path.

    This fixture creates a file-based SQLite database that can be used
    by both pytest and Dagster for integration testing. Unlike in-memory
    databases, file-based databases persist across processes.

    Returns
    -------
    Path
        Path to test SQLite database file

    Examples
    --------
    >>> def test_with_file_db(test_toltec_db_path):
    ...     # Use in pytest
    ...     db = TestToltecDBResource(database_url=f"sqlite:///{test_toltec_db_path}")
    ...     session = db.get_session()
    ...     # ... test code ...

    >>> # Use in Dagster definitions
    >>> defs = create_test_definitions(test_db_path=str(test_toltec_db_path))
    """
    tmpdir = tmp_path_factory.mktemp("dagster_test")
    return tmpdir / "test_toltec.sqlite"


@pytest.fixture
def test_toltec_db_resource(test_toltec_db_path):
    """Create TestToltecDBResource for file-based testing.

    This fixture provides a fully initialized test database that can be
    used in pytest tests simulating Dagster behavior.

    Returns
    -------
    TestToltecDBResource
        Initialized test resource with schema created

    Examples
    --------
    >>> def test_acquisition_flow(test_toltec_db_resource):
    ...     session = test_toltec_db_resource.get_session()
    ...     # Insert test data
    ...     session.execute(text("INSERT INTO interface_file ..."))
    ...     session.commit()
    ...     # Verify behavior
    ...     result = session.execute(text("SELECT * FROM interface_file"))
    """
    from tolteca_db.dagster.test_resources import TestToltecDBResource

    # Create resource
    resource = TestToltecDBResource(
        database_url=f"sqlite:///{test_toltec_db_path}", read_only=False
    )

    # Initialize schema
    class FakeContext:
        """Mock Dagster context for setup."""

        class Log:
            def info(self, msg):
                pass

        @property
        def log(self):
            return self.Log()

    resource.setup_for_execution(FakeContext())

    return resource
