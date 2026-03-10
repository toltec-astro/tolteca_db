"""Basic integration tests for the Database abstraction."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from tolteca_db.db import create_database


@pytest.fixture
def temp_dir():
    """Provide temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_parquet(temp_dir):
    """Create sample Parquet file."""
    data = pd.DataFrame(
        {
            "obs_id": [1, 1, 1, 2, 2, 2],
            "det_id": [0, 1, 2, 0, 1, 2],
            "flux": [0.5, 0.7, 0.3, 0.6, 0.8, 0.4],
        }
    )
    path = temp_dir / "obs_data.parquet"
    data.to_parquet(path)
    return path


def test_single_duckdb_mode_basic(temp_dir):
    """Test basic single DuckDB functionality."""
    db_path = temp_dir / "test.duckdb"
    db_url = f"duckdb:///{db_path}"

    # Simple API - auto-detects DuckDB mode
    db = create_database(db_url)

    # Verify database created
    assert db.metadata_engine is not None
    assert db.metadata_dialect == "duckdb"

    # Can create tables
    db.create_tables()

    # Can get session
    with db.session() as session:
        assert session is not None

    db.close()


def test_dual_mode_sqlite_basic(temp_dir):
    """Test SQLite metadata engine."""
    metadata_db = temp_dir / "metadata.db"
    db_url = f"sqlite:///{metadata_db}"

    # Simple API - auto-detects SQLite mode
    db = create_database(db_url)

    # Verify engines
    assert db.metadata_engine is not None
    assert db.metadata_dialect == "sqlite"

    # Create tables
    db.create_tables()

    # Can query
    with db.session() as session:
        assert session is not None

    db.close()


def test_parquet_direct_query(temp_dir, sample_parquet):
    """Test direct Parquet querying."""
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    # Simple API - auto-detects DuckDB mode
    db = create_database(db_url)

    # Query Parquet file
    result = db.execute_raw(f"SELECT * FROM '{sample_parquet}'")
    df = result.df()

    assert len(df) == 6
    assert "obs_id" in df.columns
    assert "flux" in df.columns

    db.close()


def test_parquet_with_aggregation(temp_dir, sample_parquet):
    """Test Parquet query with aggregation."""
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    # Simple API - auto-detects DuckDB mode
    db = create_database(db_url)

    # Query with GROUP BY
    result = db.execute_raw(f"""
        SELECT obs_id, COUNT(*) as n_det, AVG(flux) as mean_flux
        FROM '{sample_parquet}'
        GROUP BY obs_id
        ORDER BY obs_id
    """)
    df = result.df()

    assert len(df) == 2
    assert df["obs_id"].tolist() == [1, 2]
    assert all(df["n_det"] == 3)

    db.close()


def test_multiple_parquet_files(temp_dir):
    """Test querying multiple Parquet files with glob."""
    # Create multiple files
    for obs_id in [1, 2, 3]:
        data = pd.DataFrame(
            {
                "obs_id": [obs_id] * 3,
                "det_id": [0, 1, 2],
                "flux": [0.5 + obs_id * 0.1, 0.7, 0.3],
            }
        )
        data.to_parquet(temp_dir / f"obs_{obs_id}.parquet")

    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    # Simple API - auto-detects DuckDB mode
    db = create_database(db_url)

    # Query all files
    result = db.execute_raw(f"""
        SELECT obs_id, COUNT(*) as n_rows
        FROM '{temp_dir / "obs_*.parquet"}'
        GROUP BY obs_id
        ORDER BY obs_id
    """)
    df = result.df()

    assert len(df) == 3
    assert df["obs_id"].tolist() == [1, 2, 3]
    assert all(df["n_rows"] == 3)

    db.close()


def test_context_manager(temp_dir):
    """Test database as context manager."""
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    # Simple API - auto-detects DuckDB mode
    with create_database(db_url) as db:
        db.create_tables()
        with db.session() as session:
            assert session is not None

    # Database closed after context


@pytest.mark.integration
def test_dagster_mode_readonly(temp_dir):
    """Test Dagster mode with read-only queries."""
    metadata_db = temp_dir / "metadata.db"
    db_url = f"sqlite:///{metadata_db}"

    # Create database first (auto-detects SQLite mode)
    db_write = create_database(db_url)
    db_write.create_tables()
    db_write.close()

    # Open in read-only mode for multiprocess querying
    db_read = create_database(db_url, read_only=True)

    # Can query (read-only)
    with db_read.session() as session:
        assert session is not None

    db_read.close()
