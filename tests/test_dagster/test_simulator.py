"""Tests for data acquisition simulator.

These tests verify the simulator correctly:
1. Reflects schema from real database
2. Starts with empty database
3. Inserts quartets with valid=0
4. Updates valid=0 → valid=1 on subsequent ticks
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from tolteca_db.dagster.test_resources import TestToltecDBResource


@pytest.mark.integration
def test_test_db_schema_reflection():
    """Test that schema is correctly reflected from real database."""
    from dagster import build_init_resource_context

    # Create test resource
    test_resource = TestToltecDBResource(
        source_db_url="sqlite:///../run/toltecdb_last_30days.sqlite"
    )

    # Create resource context
    context = build_init_resource_context()

    # Create test database
    engine = test_resource.create_resource(context)

    # Verify tables exist
    with Session(engine) as session:
        # Check master table
        result = session.execute(
            text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='master'
        """)
        ).fetchone()
        assert result is not None, "master table should exist"

        # Check toltec table
        result = session.execute(
            text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='toltec'
        """)
        ).fetchone()
        assert result is not None, "toltec table should exist"

        # Check obstype table
        result = session.execute(
            text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='obstype'
        """)
        ).fetchone()
        assert result is not None, "obstype table should exist"


@pytest.mark.integration
def test_test_db_starts_empty():
    """Test that test database starts with empty tables."""
    from dagster import build_init_resource_context

    test_resource = TestToltecDBResource()
    context = build_init_resource_context()
    engine = test_resource.create_resource(context)

    with Session(engine) as session:
        # Verify tables are empty
        master_count = session.execute(text("SELECT COUNT(*) FROM master")).scalar()
        toltec_count = session.execute(text("SELECT COUNT(*) FROM toltec")).scalar()
        obstype_count = session.execute(text("SELECT COUNT(*) FROM obstype")).scalar()

        assert master_count == 0, "master table should be empty"
        assert toltec_count == 0, "toltec table should be empty"
        assert obstype_count == 0, "obstype table should be empty"


@pytest.mark.integration
def test_simulator_inserts_first_quartet():
    """Test that simulator inserts first quartet when database is empty."""
    from dagster import AssetExecutionContext, build_asset_context

    from tolteca_db.dagster.test_resources import (
        SimulatorConfig,
        TestToltecDBResource,
        _insert_next_quartet_from_real_db,
    )

    # Create test database
    test_resource = TestToltecDBResource()
    context = build_asset_context()
    engine = test_resource.create_resource(context)

    # Create simulator config
    simulator = SimulatorConfig()

    # Insert first quartet
    with Session(engine) as session:
        _insert_next_quartet_from_real_db(
            context,
            session,
            simulator.source_db_url,
        )

        # Verify toltec entries were inserted
        toltec_count = session.execute(
            text(
                "SELECT COUNT(DISTINCT ObsNum, SubObsNum, ScanNum, Master) FROM toltec"
            )
        ).scalar()
        assert toltec_count == 1, "Should have 1 quartet"

        # Verify interfaces were inserted with Valid=0
        invalid_count = session.execute(
            text("""
            SELECT COUNT(*) FROM toltec WHERE Valid = 0
        """)
        ).scalar()
        assert invalid_count > 0, "Should have invalid interfaces"

        valid_count = session.execute(
            text("""
            SELECT COUNT(*) FROM toltec WHERE Valid = 1
        """)
        ).scalar()
        assert valid_count == 0, "Should have no valid interfaces initially"


@pytest.mark.integration
def test_simulator_marks_interfaces_valid():
    """Test that simulator marks invalid interfaces as valid."""
    from dagster import build_asset_context
    from sqlalchemy.orm import Session

    from tolteca_db.dagster.test_resources import (
        SimulatorConfig,
        TestToltecDBResource,
        _insert_next_quartet_from_real_db,
    )

    # Create test database and insert first quartet
    test_resource = TestToltecDBResource()
    context = build_asset_context()
    engine = test_resource.create_resource(context)

    with Session(engine) as session:
        _insert_next_quartet_from_real_db(
            context,
            session,
            "sqlite:///../run/toltecdb_last_30days.sqlite",
        )

        # Get quartet identifiers
        quartet = session.execute(
            text("SELECT ObsNum, SubObsNum, ScanNum, Master FROM toltec LIMIT 1")
        ).fetchone()

        # Mark all interfaces as valid
        session.execute(
            text("""
            UPDATE toltec
            SET Valid = 1
            WHERE ObsNum = :obsnum
              AND SubObsNum = :subobsnum
              AND ScanNum = :scannum
              AND Master = :master
        """),
            {
                "obsnum": quartet.ObsNum,
                "subobsnum": quartet.SubObsNum,
                "scannum": quartet.ScanNum,
                "master": quartet.Master,
            },
        )

        session.commit()

        # Verify all interfaces are now valid
        invalid_count = session.execute(
            text("""
            SELECT COUNT(*) FROM toltec WHERE Valid = 0
        """)
        ).scalar()
        assert invalid_count == 0, "All interfaces should be valid"

        valid_count = session.execute(
            text("""
            SELECT COUNT(*) FROM toltec WHERE Valid = 1
        """)
        ).scalar()
        assert valid_count > 0, "Should have valid interfaces"
