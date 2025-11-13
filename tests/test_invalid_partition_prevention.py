"""Test validation check to prevent manual materialization of invalid partitions.

This test verifies that query_toltec_db_interface raises DagsterExecutionInterruptedError
when Valid=0, preventing both manual and automatic materialization of incomplete data.
"""

from __future__ import annotations

import pytest
from dagster import DagsterExecutionInterruptedError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from tolteca_db.dagster.helpers import query_toltec_db_interface


@pytest.fixture
def toltec_db_with_valid_data():
    """Create in-memory toltec_db with Valid=1 interface."""
    engine = create_engine("sqlite:///:memory:")
    
    with Session(engine) as session:
        # Create schema
        session.execute(text("""
            CREATE TABLE master (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL
            )
        """))
        session.execute(text("""
            CREATE TABLE toltec (
                id INTEGER PRIMARY KEY,
                Master INTEGER NOT NULL,
                ObsNum INTEGER NOT NULL,
                SubObsNum INTEGER NOT NULL,
                ScanNum INTEGER NOT NULL,
                RoachIndex INTEGER NOT NULL,
                FileName TEXT,
                Valid INTEGER NOT NULL,
                ObsType TEXT,
                Date TEXT,
                Time TEXT,
                FOREIGN KEY (Master) REFERENCES master(id)
            )
        """))
        
        # Add master
        session.execute(text("INSERT INTO master (id, label) VALUES (0, 'TCS')"))
        
        # Add interface with Valid=1
        session.execute(text("""
            INSERT INTO toltec (id, Master, ObsNum, SubObsNum, ScanNum, RoachIndex, 
                                FileName, Valid, ObsType, Date, Time)
            VALUES (1, 0, 123456, 0, 0, 5, 'toltec5_123456_0_0.nc', 1, 
                    'science', '2024-01-01', '12:00:00')
        """))
        
        session.commit()
    
    yield engine
    engine.dispose()


@pytest.fixture
def toltec_db_with_invalid_data():
    """Create in-memory toltec_db with Valid=0 interface."""
    engine = create_engine("sqlite:///:memory:")
    
    with Session(engine) as session:
        # Create schema
        session.execute(text("""
            CREATE TABLE master (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL
            )
        """))
        session.execute(text("""
            CREATE TABLE toltec (
                id INTEGER PRIMARY KEY,
                Master INTEGER NOT NULL,
                ObsNum INTEGER NOT NULL,
                SubObsNum INTEGER NOT NULL,
                ScanNum INTEGER NOT NULL,
                RoachIndex INTEGER NOT NULL,
                FileName TEXT,
                Valid INTEGER NOT NULL,
                ObsType TEXT,
                Date TEXT,
                Time TEXT,
                FOREIGN KEY (Master) REFERENCES master(id)
            )
        """))
        
        # Add master
        session.execute(text("INSERT INTO master (id, label) VALUES (0, 'TCS')"))
        
        # Add interface with Valid=0 (data acquisition in progress)
        session.execute(text("""
            INSERT INTO toltec (id, Master, ObsNum, SubObsNum, ScanNum, RoachIndex, 
                                FileName, Valid, ObsType, Date, Time)
            VALUES (1, 0, 123456, 0, 0, 5, 'toltec5_123456_0_0.nc', 0, 
                    'science', '2024-01-01', '12:00:00')
        """))
        
        session.commit()
    
    yield engine
    engine.dispose()


def test_query_valid_interface_succeeds(toltec_db_with_valid_data):
    """Test that query_toltec_db_interface returns data for Valid=1."""
    with Session(toltec_db_with_valid_data) as session:
        result = query_toltec_db_interface(
            master="tcs",
            obsnum=123456,
            subobsnum=0,
            scannum=0,
            roach_index=5,
            session=session,
        )
        
        assert result["master"] == "tcs"
        assert result["obsnum"] == 123456
        assert result["roach_index"] == 5
        assert result["interface"] == "toltec5"
        assert result["valid"] is True
        assert result["filename"] == "toltec5_123456_0_0.nc"


def test_query_invalid_interface_raises_interrupted_error(toltec_db_with_invalid_data):
    """Test that query_toltec_db_interface raises DagsterExecutionInterruptedError for Valid=0.
    
    This is the key test that verifies manual materialization prevention.
    When a user tries to manually materialize a partition corresponding to
    incomplete data (Valid=0), the query function should raise
    DagsterExecutionInterruptedError, which causes Dagster to mark the
    execution as "interrupted" rather than failed, and it will
    automatically retry when the sensor creates a new run for Valid=1.
    """
    with Session(toltec_db_with_invalid_data) as session:
        with pytest.raises(DagsterExecutionInterruptedError) as exc_info:
            query_toltec_db_interface(
                master="tcs",
                obsnum=123456,
                subobsnum=0,
                scannum=0,
                roach_index=5,
                session=session,
            )
        
        # Verify error message is informative
        error_msg = str(exc_info.value)
        assert "toltec5" in error_msg
        assert "Valid=0" in error_msg
        assert "Data acquisition in progress" in error_msg
        assert "automatically materialized" in error_msg.lower()


def test_query_missing_interface_raises_value_error(toltec_db_with_valid_data):
    """Test that query_toltec_db_interface raises ValueError for non-existent interface."""
    with Session(toltec_db_with_valid_data) as session:
        with pytest.raises(ValueError) as exc_info:
            query_toltec_db_interface(
                master="tcs",
                obsnum=999999,  # Non-existent obsnum
                subobsnum=0,
                scannum=0,
                roach_index=5,
                session=session,
            )
        
        error_msg = str(exc_info.value)
        assert "Interface not found" in error_msg
        assert "999999" in error_msg


def test_sensor_still_creates_runs_for_valid_only():
    """Conceptual test documenting sensor behavior.
    
    The sensor queries all interfaces (Valid=0 and Valid=1) but only creates
    RunRequests for Valid=1. This means:
    
    1. Valid=0: Partition key created, no run scheduled
    2. Valid=1: Partition key created, run scheduled automatically
    3. Manual materialization of Valid=0: Blocked by query_toltec_db_interface
    4. Manual materialization of Valid=1: Allowed by query_toltec_db_interface
    
    This ensures that data is only processed when complete, regardless of
    whether materialization is triggered by sensor or manual action.
    """
    # This is a documentation test - no actual execution
    # See sensors.py lines 168-175 for sensor logic
    # See helpers.py query_toltec_db_interface for validation gate
    assert True  # Placeholder


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
