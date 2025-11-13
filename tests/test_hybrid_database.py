"""Tests for the Database abstraction functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from tolteca_db.db import Database, create_database
from tolteca_db.models.orm import DataProd, DataProdType, Location


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
            "noise": [0.05, 0.06, 0.04, 0.05, 0.07, 0.04],
        }
    )
    path = temp_dir / "obs_data.parquet"
    data.to_parquet(path)
    return path


def test_database_duckdb_mode(temp_dir):
    """Test single DuckDB mode."""
    db_path = temp_dir / "test.duckdb"
    db_url = f"duckdb:///{db_path}"
    db = create_database(db_url)

    # Verify engines
    assert db.metadata_engine is not None
    assert db.query_con is not None
    assert db.metadata_dialect == "duckdb"

    # Create tables
    db.create_tables()

    # Verify can create session
    with db.session() as session:
        assert session is not None

    db.close()


def test_database_sqlite_mode(temp_dir):
    """Test SQLite metadata + DuckDB queries."""
    metadata_db = temp_dir / "metadata.db"
    db_url = f"sqlite:///{metadata_db}"

    db = create_database(db_url)

    # Verify engines
    assert db.metadata_engine is not None
    assert db.query_con is not None
    assert db.metadata_dialect == "sqlite"

    # Create tables
    db.create_tables()

    # Add metadata
    with db.session() as session:
        location = Location(
            label="test",
            root_uri="file:///test",
            description="Test location",
        )
        session.add(location)
        session.commit()

    # Query metadata via SQLAlchemy
    with db.session() as session:
        locations = session.query(Location).all()
        assert len(locations) == 1
        assert locations[0].label == "test"

    db.close()


def test_database_readonly_mode(temp_dir):
    """Test Dagster multiprocess mode with read-only queries."""
    metadata_db = temp_dir / "metadata.db"
    db_url = f"sqlite:///{metadata_db}"

    # First create and populate database
    db_write = create_database(db_url)
    db_write.create_tables()

    with db_write.session() as session:
        prod_type = DataProdType(label="test_type")
        session.add(prod_type)
        session.flush()

        product = DataProd(
            label="test_product",
            data_prod_type_fk=prod_type.pk,
        )
        session.add(product)
        session.commit()

    db_write.close()

    # Now open in Dagster mode (read-only queries)
    db_read = create_database(db_url, read_only=True)

    # Verify can read metadata
    with db_read.session() as session:
        products = session.query(DataProd).all()
        assert len(products) == 1
        assert products[0].label == "test_product"

    db_read.close()


def test_parquet_query_direct(temp_dir, sample_parquet):
    """Test direct Parquet querying."""
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    db = create_database(db_url)

    # Query Parquet file directly
    result = db.execute_raw(f"SELECT * FROM '{sample_parquet}'")
    df = result.df()

    assert len(df) == 6
    assert list(df.columns) == ["obs_id", "det_id", "flux", "noise"]
    assert df["obs_id"].nunique() == 2
    assert df["det_id"].nunique() == 3

    db.close()


def test_parquet_query_with_filter(temp_dir, sample_parquet):
    """Test Parquet query with WHERE clause."""
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    db = create_database(db_url)

    # Query with filter
    result = db.execute_raw(f"""
        SELECT det_id, AVG(flux) as mean_flux
        FROM '{sample_parquet}'
        WHERE obs_id = 1
        GROUP BY det_id
        ORDER BY det_id
    """)
    df = result.df()

    assert len(df) == 3
    assert df["det_id"].tolist() == [0, 1, 2]
    assert abs(df[df["det_id"] == 0]["mean_flux"].iloc[0] - 0.5) < 0.01
    assert abs(df[df["det_id"] == 1]["mean_flux"].iloc[0] - 0.7) < 0.01

    db.close()


def test_join_metadata_parquet(temp_dir):
    """Test joining metadata tables with Parquet files."""
    # Create Parquet files
    for obs_id in [1, 2]:
        data = pd.DataFrame(
            {
                "obs_id": [obs_id] * 3,
                "det_id": [0, 1, 2],
                "flux": [0.5 + obs_id * 0.1, 0.7, 0.3],
            }
        )
        data.to_parquet(temp_dir / f"obs_{obs_id}.parquet")

    # Create database
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    db = create_database(db_url)
    db.create_tables()

    # Add metadata
    with db.session() as session:
        prod_type = DataProdType(label="observation")
        session.add(prod_type)
        session.flush()

        for obs_id in [1, 2]:
            product = DataProd(
                label=f"obs_{obs_id}",
                data_prod_type_fk=prod_type.pk,
            )
            session.add(product)
            session.flush()

    # Join metadata with Parquet
    result = db.execute_raw(f"""
        SELECT
            dp.label as product_label,
            COUNT(*) as row_count,
            AVG(obs.flux) as mean_flux
        FROM data_prod dp
        JOIN '{temp_dir / "obs_*.parquet"}' obs
          ON obs.obs_id = CAST(SPLIT_PART(dp.label, '_', 2) AS INTEGER)
        GROUP BY dp.label
        ORDER BY dp.label
    """)
    df = result.df()

    assert len(df) == 2
    assert df["product_label"].tolist() == ["obs_1", "obs_2"]
    assert all(df["row_count"] == 3)

    db.close()


def test_parquet_session_helper(temp_dir, sample_parquet):
    """Test parquet_session context manager."""
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"
    db = create_database(db_url)
    db.create_tables()

    # Add metadata
    with db.session() as session:
        prod_type = DataProdType(label="test")
        session.add(prod_type)
        session.flush()

        location = Location(
            label="local",
            root_uri=f"file://{temp_dir}",
        )
        session.add(location)
        session.commit()

    # Use parquet_session
    with db.parquet_session() as (session, pq):
        # Query metadata
        prod_type = session.query(DataProdType).first()
        assert prod_type is not None
        assert prod_type.label == "test"

        # ParquetQuery helper available
        assert pq is not None

    db.close()


def test_sqlite_dialect_constraints(temp_dir):
    """Test that SQLite dialect properly handles constraints."""
    metadata_db = temp_dir / "metadata.db"
    db_url = f"sqlite:///{metadata_db}"

    db = create_database(db_url)

    # This should not raise errors for partial indexes
    db.create_tables()

    # Verify tables created
    with db.session() as session:
        # Check that tables exist by querying
        result = session.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in result]
        assert "data_prod" in tables
        assert "location" in tables

    db.close()


def test_context_manager(temp_dir):
    """Test database as context manager."""
    db_url = f"duckdb:///{temp_dir / 'test.duckdb'}"

    with create_database(db_url) as db:
        db.create_tables()

        with db.session() as session:
            location = Location(
                label="test",
                root_uri="file:///test",
            )
            session.add(location)
            session.commit()

    # Database should be closed after context
    # (Cannot easily test this without internal state inspection)


@pytest.mark.integration
def test_concurrent_reads(temp_dir, sample_parquet):
    """Test concurrent read access in Dagster mode."""
    import concurrent.futures

    metadata_db = temp_dir / "metadata.db"
    db_url = f"sqlite:///{metadata_db}"

    # Create and populate database
    db_setup = create_database(db_url)
    db_setup.create_tables()

    with db_setup.session() as session:
        prod_type = DataProdType(label="observation")
        session.add(prod_type)
        session.flush()

        for i in range(5):
            product = DataProd(
                label=f"obs_{i}",
                data_prod_type_fk=prod_type.pk,
            )
            session.add(product)

    db_setup.close()

    # Simulate concurrent reads
    def read_metadata(worker_id):
        db = create_database(db_url, read_only=True)
        with db.session() as session:
            products = session.query(DataProd).all()
            count = len(products)
        db.close()
        return worker_id, count

    # Run concurrent reads
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(read_metadata, i) for i in range(3)]
        results = [f.result() for f in futures]

    # All workers should see all 5 products
    assert all(count == 5 for _, count in results)
